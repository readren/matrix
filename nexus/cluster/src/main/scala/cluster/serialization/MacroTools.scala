package readren.nexus
package cluster.serialization

import scala.annotation.tailrec
import scala.quoted.{Expr, Quotes, Type}

/** Compile-time reflection utilities shared across [[SerializerDerivation]], [[DeserializerDerivation]], and [[DiscriminationCriteria]]. */
object MacroTools {

	/** Behaves like the identity function at runtime but prints the code of the received argument at compile time. */
	inline def showCode[T](inline expr: T): T = ${ showCodeImpl[T]('expr) }

	private def showCodeImpl[T: Type](expr: Expr[T])(using quotes: Quotes): Expr[T] = {
		import quotes.reflect.*
		val pos = Position.ofMacroExpansion
		println(s"""\nCode of `${pos.sourceCode.fold(Type.show[T])(identity)}` at ${pos.sourceFile}:${pos.startLine}:${pos.startColumn} <<<<""")
		println(expr.asTerm.show(using Printer.TreeShortCode))
		println(s"\n>>>>")
		expr
	}

	/** Description of a value parameter of a product class's primary constructor.
	 *
	 * @param name the parameter name matching the getter/field.
	 * @param tpe the substituted type of the parameter.
	 * @param sinceVersion the protocol version at which the field was introduced via [[since]], or None if present in the initial version.
	 * @param defaultValue an expression evaluating to the default constructor argument value (`$lessinit$greater$default$n`) if defined on the companion module. */
	case class ProductParam[+Tpe, +Trm](
		name: String,
		tpe: Tpe,
		sinceVersion: Option[Int],
		defaultValue: Option[Trm]
	)

	/** Structural classification of a type inspected via Tasty reflection for serializer and deserializer derivation. */
	enum TypeClassification[+Sym, +Tpe, +Trm] {
		/** A singleton value type (`case object`, module, or parameterless `enum` case) referenced by `termSymbol`. */
		case Singleton(termSymbol: Sym)

		/** A `sealed` sum type (`sealed trait`, `sealed abstract class`, or `enum`) whose direct child variant types, with parent type arguments propagated, are `variantTypes`. */
		case Sum(variantTypes: List[Tpe])

		/** An `AnyVal` value class whose single parameter has substituted type `underlyingTpe` and name `fieldName`, constructed via `typedConstructor`. */
		case ValueClass(underlyingTpe: Tpe, fieldName: String, typedConstructor: Trm)

		/** An opaque type alias whose underlying representation is `underlyingTpe`. */
		case OpaqueAlias(underlyingTpe: Tpe)

		/** A concrete product class whose primary constructor selection (with type arguments applied when generic) is `typedConstructor` and whose value parameter lists (with type arguments substituted) are `paramLists`. */
		case Product(typedConstructor: Trm, paramLists: List[List[ProductParam[Tpe, Trm]]])
	}

	/** Classifies `tpe` as a [[TypeClassification.Singleton]], [[TypeClassification.Sum]], [[TypeClassification.ValueClass]], [[TypeClassification.OpaqueAlias]], or [[TypeClassification.Product]] using Tasty reflection without relying on [[scala.deriving.Mirror]].
	 *
	 * @param tpe the type representation to classify.
	 * @return `Some` classification if `tpe` is a supported singleton, sealed sum, value class, opaque type alias, or concrete product class; `None` otherwise. */
	def classifyType(using quotes: Quotes)(
		tpe: quotes.reflect.TypeRepr
	): Option[TypeClassification[quotes.reflect.Symbol, quotes.reflect.TypeRepr, quotes.reflect.Term]] = {
		import quotes.reflect.*

		val dealiased = tpe.dealias
		val sym = dealiased.typeSymbol

		dealiased match {
			case termRef: TermRef =>
				Some(TypeClassification.Singleton(termRef.termSymbol))

			case _ if sym.isClassDef && sym.flags.is(Flags.Module) =>
				Some(TypeClassification.Singleton(sym.companionModule))

			case _ if sym.flags.is(Flags.Opaque) =>
				val underlying = resolveTranslucentType(dealiased)
				Some(TypeClassification.OpaqueAlias(underlying))

			case _ if sym.isClassDef && sym.flags.is(Flags.Sealed) && (sym.flags.is(Flags.Trait) || sym.flags.is(Flags.Abstract)) && sym.children.nonEmpty =>
				val variantTypes = sym.children.map(childSym => resolveChildVariantType(dealiased, sym, childSym))
				Some(TypeClassification.Sum(variantTypes))

			case _ if sym.isClassDef && !sym.flags.is(Flags.Trait) && !sym.flags.is(Flags.Abstract) =>
				val ctor = sym.primaryConstructor
				if ctor == Symbol.noSymbol || ctor.flags.is(Flags.Private) || ctor.flags.is(Flags.Protected) then None
				else {
					val baseCtor = New(Inferred(dealiased)).select(ctor)
					val typedCtor = dealiased match {
						case AppliedType(_, typeArgs) if ctor.paramSymss.headOption.exists(_.exists(_.isTypeDef)) =>
							baseCtor.appliedToTypes(typeArgs)
						case _ =>
							baseCtor
					}
					val rawParamLists = extractValueParamLists(typedCtor.tpe)
					val isValueClass = dealiased.derivesFrom(defn.AnyValClass) && !defn.ScalaPrimitiveValueClasses.contains(sym)

					if isValueClass && rawParamLists.size == 1 && rawParamLists.head.size == 1 then {
						val (pName, pTpe) = rawParamLists.head.head
						Some(TypeClassification.ValueClass(pTpe, pName, typedCtor))
					} else {
						val valueParamSymss = ctor.paramSymss.filterNot(_.headOption.exists(_.isTypeDef))
						var flatParamIndex = 1

						val companionSym = sym.companionModule

						val paramLists: List[List[ProductParam[TypeRepr, Term]]] = rawParamLists.zipWithIndex.map { (rawList, listIdx) =>
						val symList = valueParamSymss.lift(listIdx).getOrElse(Nil)
						rawList.zipWithIndex.map { case ((pName, pTpe), paramIdx) =>
							val pSymOpt = symList.lift(paramIdx)
							val currentParamIndex = flatParamIndex
							flatParamIndex += 1

							val pSymAnnots = pSymOpt.map(_.annotations).getOrElse(Nil)
							val fieldSym = sym.fieldMember(pName)
							val fieldAnnots = if fieldSym != Symbol.noSymbol then fieldSym.annotations else Nil
							val getterSyms = sym.methodMember(pName)
							val getterAnnots = getterSyms.flatMap(_.annotations)
							val allAnnots = pSymAnnots ++ fieldAnnots ++ getterAnnots

							val sinceAnnotOpt = allAnnots.find { annot =>
								val annotTpe = annot.tpe.dealias
								annotTpe.typeSymbol.name == "since" || annotTpe <:< TypeRepr.of[readren.nexus.cluster.serialization.since]
							}

							val sinceVersionOpt: Option[Int] = sinceAnnotOpt.map { annot =>
								@tailrec
								def unwrap(t: Term): Term = t match {
									case Inlined(_, _, body) => unwrap(body)
									case Typed(body, _) => unwrap(body)
									case Block(_, body) => unwrap(body)
									case _ => t
								}

								val unwrapped = unwrap(annot)
								val args: List[Term] = unwrapped match {
									case Apply(Select(New(_), _), args) => args
									case Apply(TypeApply(Select(New(_), _), _), args) => args
									case Apply(_, args) => args
									case _ => Nil
								}

								@tailrec
								def extractLiteral(term: Term): Option[Int] = term match {
									case Inlined(_, _, expr) => extractLiteral(expr)
									case Typed(expr, _) => extractLiteral(expr)
									case Block(_, expr) => extractLiteral(expr)
									case Literal(IntConstant(i)) => Some(i)
									case Literal(ByteConstant(b)) => Some(b.toInt)
									case Literal(ShortConstant(s)) => Some(s.toInt)
									case Apply(Ident("ProtocolVersion"), List(inner)) => extractLiteral(inner)
									case Apply(Select(Ident("ProtocolVersion"), "apply"), List(inner)) => extractLiteral(inner)
									case Select(Ident("ProtocolVersion"), "OF_THIS_PROJECT") => Some(ProtocolVersion.OF_THIS_PROJECT.identifier)
									case Select(Ident("ProtocolVersion"), "NOT_SPECIFIED") => Some(ProtocolVersion.NOT_SPECIFIED.identifier)
									case other =>
										other.asExpr match {
											case '{ ${Expr(i)}: Int } => Some(i)
											case '{ ${Expr(b)}: Byte } => Some(b.toInt)
											case _ =>
												other.symbol.tree match {
													case ValDef(_, _, Some(rhs)) => extractLiteral(rhs)
													case _ => None
												}
										}
								}

								val verInt: Int = args.headOption.flatMap(extractLiteral).getOrElse {
									report.errorAndAbort(s"Annotation @since on parameter `$pName` in `${dealiased.show}` must specify a constant Int literal; got: ${annot.show}")
								}

								if verInt < 1 then {
									report.errorAndAbort(s"Parameter `$pName` in `${dealiased.show}` has an invalid @since version `$verInt`. Protocol versions must be >= 1.")
								}
								verInt
							}

							val defaultMethodName = s"$$lessinit$$greater$$default$$$currentParamIndex"
							val defaultMethodSymOpt = if companionSym != Symbol.noSymbol then {
								companionSym.methodMember(defaultMethodName).headOption
							} else None

							val defaultValueOpt: Option[Term] = defaultMethodSymOpt.map { defaultMethodSym =>
								val baseDefault = Ref(companionSym).select(defaultMethodSym)
								val typedDefault = if defaultMethodSym.paramSymss.headOption.exists(_.exists(_.isTypeDef)) then {
									dealiased match {
										case AppliedType(_, typeArgs) => baseDefault.appliedToTypes(typeArgs)
										case _ => baseDefault
									}
								} else baseDefault

								val valueParamLists = defaultMethodSym.paramSymss.filterNot(_.exists(_.isTypeDef))
								if valueParamLists.isEmpty then typedDefault
								else if valueParamLists == List(Nil) then typedDefault.appliedToArgs(Nil)
								else typedDefault.appliedToArgs(Nil)
							}

							if sinceVersionOpt.isDefined && defaultValueOpt.isEmpty then {
								report.errorAndAbort(
									s"Parameter `$pName` in `${dealiased.show}` is annotated with `@since(${sinceVersionOpt.get})` but does not define a default constructor argument value. Schema evolution requires all fields introduced via @since to have a default value."
								)
							}

							ProductParam(pName, pTpe, sinceVersionOpt, defaultValueOpt)
						}
					}

						Some(TypeClassification.Product(typedCtor, paramLists))
					}
				}

			case _ =>
				None
		}
	}

	/** Recursively resolves the underlying translucent type representation of an opaque type alias. */
	@tailrec
	private def resolveTranslucentType(using quotes: Quotes)(tpe: quotes.reflect.TypeRepr): quotes.reflect.TypeRepr = {
		import quotes.reflect.*

		val dealiased = tpe.dealias
		dealiased match {
			case ref: TypeRef if ref.isOpaqueAlias || ref.typeSymbol.flags.is(Flags.Opaque) =>
				resolveTranslucentType(ref.translucentSuperType)
			case AppliedType(ref: TypeRef, args) if ref.isOpaqueAlias || ref.typeSymbol.flags.is(Flags.Opaque) =>
				resolveTranslucentType(ref.translucentSuperType.appliedTo(args))
			case other =>
				other
		}
	}

	/** Extracts the value parameter lists (parameter name and substituted parameter type) from a constructor method type. */
	private def extractValueParamLists(using quotes: Quotes)(methodTpe: quotes.reflect.TypeRepr): List[List[(String, quotes.reflect.TypeRepr)]] = {
		import quotes.reflect.*

		methodTpe.widen match {
			case MethodType(paramNames, paramTypes, resType) =>
				paramNames.zip(paramTypes) :: extractValueParamLists(resType)
			case _ =>
				Nil
		}
	}

	/** Resolves the concrete [[quotes.reflect.TypeRepr]] of a direct `childSym` of `sumSym` by propagating type arguments from `sumTpe` onto `childSym`'s type parameters. */
	private def resolveChildVariantType(using quotes: Quotes)(
		sumTpe: quotes.reflect.TypeRepr,
		sumSym: quotes.reflect.Symbol,
		childSym: quotes.reflect.Symbol
	): quotes.reflect.TypeRepr = {
		import quotes.reflect.*

		if childSym.isTerm then Ref(childSym).tpe
		else if childSym.isClassDef && childSym.flags.is(Flags.Module) then Ref(childSym.companionModule).tpe
		else if childSym.isClassDef then {
			val childTycon: TypeRepr = sumTpe match {
				case TypeRef(prefix, _) if childSym.owner == sumSym.owner && prefix != NoPrefix =>
					prefix.select(childSym)
				case AppliedType(TypeRef(prefix, _), _) if childSym.owner == sumSym.owner && prefix != NoPrefix =>
					prefix.select(childSym)
				case _ =>
					childSym.typeRef
			}

			val tpSyms: List[Symbol] = childSym.primaryConstructor.paramSymss.headOption match {
				case Some(params) if params.exists(_.isTypeDef) => params
				case _ => childSym.declaredTypes.filter(_.isTypeParam)
			}

			if tpSyms.isEmpty then childTycon
			else {
				val tpRefs = tpSyms.map(_.typeRef)
				val rawChildApplied = childTycon.appliedTo(tpRefs)
				val childAsSum = rawChildApplied.baseType(sumSym)
				val bindings = scala.collection.mutable.Map.empty[Symbol, TypeRepr]

				def unify(pattern: TypeRepr, target: TypeRepr): Unit = (pattern.dealias, target.dealias) match {
					case (p, t) if tpSyms.contains(p.typeSymbol) =>
						bindings.put(p.typeSymbol, t)
					case (AppliedType(pTycon, pArgs), AppliedType(tTycon, tArgs))
						if pTycon.typeSymbol == tTycon.typeSymbol && pArgs.size == tArgs.size =>
						pArgs.lazyZip(tArgs).foreach(unify)
					case _ =>
						()
				}

				unify(childAsSum, sumTpe)

				val resolvedArgs = tpSyms.map { tpSym =>
					bindings.getOrElse(
						tpSym,
						report.errorAndAbort(
							s"Cannot derive for sum type `${sumTpe.show}`: variant `${childSym.name}` has type parameter `${tpSym.name}` that cannot be determined from `${sumTpe.show}`."
						)
					)
				}
				childTycon.appliedTo(resolvedArgs)
			}
		} else {
			report.errorAndAbort(s"Unexpected child symbol `${childSym.name}` of sum type `${sumTpe.show}`.")
		}
	}

	/** Verifies that `productTpe` exposes a non-private, non-protected field or zero-argument getter method matching `fieldName`, aborting compilation otherwise. */
	def ensureAccessibleGetter(using quotes: Quotes)(productTpe: quotes.reflect.TypeRepr, fieldName: String): Unit = {
		import quotes.reflect.*

		val sym = productTpe.typeSymbol
		val fieldSym = sym.fieldMember(fieldName)
		val hasAccessibleField =
			fieldSym != Symbol.noSymbol &&
				!fieldSym.flags.is(Flags.Private) &&
				!fieldSym.flags.is(Flags.PrivateLocal) &&
				!fieldSym.flags.is(Flags.Protected)

		val hasAccessibleGetter =
			sym.methodMember(fieldName).exists { m =>
				m.paramSymss.forall(_.isEmpty) &&
					!m.flags.is(Flags.Private) &&
					!m.flags.is(Flags.Protected)
			}

		if !hasAccessibleField && !hasAccessibleGetter then {
			report.errorAndAbort(
				s"Cannot derive Serializer for `${productTpe.show}`: primary constructor parameter `$fieldName` is not exposed as an accessible `val` or zero-argument getter."
			)
		}
	}

	/** Returns the simple name of `tpe` used in diagnostic entries (preserving module `$` suffixes for `case object`s and value names for `enum` singleton cases). */
	def simpleNameOf(using quotes: Quotes)(tpe: quotes.reflect.TypeRepr): String = {
		import quotes.reflect.*

		tpe.dealias match {
			case termRef: TermRef if !termRef.termSymbol.flags.is(Flags.Module) =>
				termRef.termSymbol.name
			case other =>
				other.typeSymbol.name
		}
	}

	/** Searches implicit scope for a `given` [[DiscriminationCriteria]] for `Sum`. */
	def findDiscriminationCriteria[Sum: Type](using quotes: Quotes, outerTargetName: String): Option[quotes.reflect.Term] = {
		import quotes.reflect.*

		Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
			case iss: ImplicitSearchSuccess =>
				Some(iss.tree)
			case isf: AmbiguousImplicits =>
				report.errorAndAbort(s"During the derivation of `$outerTargetName`, the search for a `given ${Type.show[DiscriminationCriteria[Sum]]}` failed with: ${isf.explanation}")
			case isf: DivergingImplicit =>
				report.errorAndAbort(s"During the derivation of `$outerTargetName`, the search for a `given ${Type.show[DiscriminationCriteria[Sum]]}` failed with: ${isf.explanation}")
			case _: ImplicitSearchFailure =>
				None
		}
	}

	/** Evaluates `dc.Discriminator[P]` to a constant integer literal at compile time. */
	def discriminatorOfImpl[S: Type, P <: S: Type](dcExpr: Expr[DiscriminationCriteria[S]])(using quotes: Quotes): Expr[Int] = {
		import quotes.reflect.*
		Expr(resolveDiscriminator[S](dcExpr.asTerm, TypeRepr.of[P]))
	}

	/** Resolves the compile-time integer constant for `variantTpe` from the [[DiscriminationCriteria.Discriminator]] match type of `dcTerm`.
	 *
	 * First attempts direct type simplification (`dealias.simplified`). If the variant is an intermediate sum type (e.g., `CarPart` where a child `Wheel <: CarPart` precedes `CarPart` in the match type, preventing disjointness reduction), falls back to walking the `MatchType` cases in declaration order using static subtyping (`variantTpe <:< pattern`). */
	def resolveDiscriminator[Sum: Type](using quotes: Quotes)(dcTerm: quotes.reflect.Term, variantTpe: quotes.reflect.TypeRepr): Int = {
		import quotes.reflect.*

		@tailrec
		def stripWrappers(term: Term): Term = term match {
			case Inlined(_, _, expansion) => stripWrappers(expansion)
			case Block(_, expr) => stripWrappers(expr)
			case Typed(expr, _) => stripWrappers(expr)
			case _ => term
		}

		def resolveConcreteCriteriaType(term: Term): TypeRepr = {
			val stripped = stripWrappers(term)
			val baseTpe = stripped.tpe
			val discSym = baseTpe.widen.typeSymbol.typeMember("Discriminator")
			baseTpe.memberType(discSym) match {
				case _: TypeBounds =>
					val (sym, typeArgs) = stripped match {
						case TypeApply(fn, args) => (fn.symbol, args.map(_.tpe))
						case other => (other.symbol, Nil)
					}
					sym.tree match {
						case DefDef(_, _, _, Some(rhs)) =>
							val rhsTpe = resolveConcreteCriteriaType(rhs)
							val typeParamSyms = sym.paramSymss.headOption.filter(_.exists(_.isTypeDef)).getOrElse(Nil)
							if typeParamSyms.nonEmpty && typeParamSyms.size == typeArgs.size then rhsTpe.substituteTypes(typeParamSyms, typeArgs)
							else rhsTpe
						case ValDef(_, _, Some(rhs)) =>
							resolveConcreteCriteriaType(rhs)
						case _ =>
							baseTpe
					}
				case _ =>
					baseTpe
			}
		}

		val effectiveCriteriaTpe = resolveConcreteCriteriaType(dcTerm)
		val discriminatorSym = effectiveCriteriaTpe.widen.typeSymbol.typeMember("Discriminator")
		val rawMemberTpe = effectiveCriteriaTpe.memberType(discriminatorSym) match {
			case TypeBounds(_, hi) => hi
			case other => other
		}

		val appliedTpe = rawMemberTpe.appliedTo(variantTpe).dealias
		appliedTpe.simplified match {
			case ConstantType(IntConstant(value)) =>
				value
			case MatchType(_, scrutinee, cases) =>
				evaluateMatchTypeCases(scrutinee, cases).getOrElse {
					report.errorAndAbort(
						s"The `Discriminator` match type in `${effectiveCriteriaTpe.widen.show}` did not match variant `${variantTpe.show}` of sum type `${Type.show[Sum]}`."
					)
				}
			case other =>
				// Also inspect `appliedTpe` in case `.simplified` partially reduced or left a MatchType alias
				appliedTpe match {
					case MatchType(_, scrutinee, cases) =>
						evaluateMatchTypeCases(scrutinee, cases).getOrElse {
							report.errorAndAbort(
								s"The `Discriminator` match type in `${effectiveCriteriaTpe.widen.show}` did not reduce to an Int constant for variant `${variantTpe.show}` of sum type `${Type.show[Sum]}`; got `${other.show}`."
							)
						}
					case _ =>
						report.errorAndAbort(
							s"The `Discriminator` match type in `${effectiveCriteriaTpe.widen.show}` did not reduce to an Int constant for variant `${variantTpe.show}` of sum type `${Type.show[Sum]}`; got `${other.show}`."
						)
				}
		}
	}

	/** Evaluates `MatchType` cases sequentially by checking whether `scrutinee` statically conforms to each case's `pattern`. */
	private def evaluateMatchTypeCases(using quotes: Quotes)(
		scrutinee: quotes.reflect.TypeRepr,
		cases: List[quotes.reflect.TypeRepr]
	): Option[Int] = {
		import quotes.reflect.*

		def matchesPattern(scrut: TypeRepr, pattern: TypeRepr): Boolean = {
			if scrut <:< pattern then true
			else (scrut.dealias, pattern.dealias) match {
				case (AppliedType(sTycon, _), AppliedType(pTycon, pArgs))
					if sTycon.typeSymbol == pTycon.typeSymbol && pArgs.forall {
						case TypeBounds(_, _) | _: ParamRef => true
						case arg => arg.typeSymbol.isTypeParam
					} =>
					true
				case _ =>
					false
			}
		}

		cases.iterator.collect {
			case MatchCase(pattern, rhs) => (pattern, rhs)
			case TypeLambda(_, _, MatchCase(pattern, rhs)) => (pattern, rhs)
		}.collectFirst {
			case (pattern, rhs) if matchesPattern(scrutinee, pattern) =>
				rhs.dealias.simplified match {
					case ConstantType(IntConstant(v)) => v
					case other =>
						report.errorAndAbort(s"Match case RHS for `${scrutinee.show}` did not reduce to an Int constant; got `${other.show}`.")
				}
		}
	}
}
