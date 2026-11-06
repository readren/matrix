package readren.nexus
package cluster.serialization

import cluster.serialization.Deserializer.Reader
import cluster.serialization.MacroTools.TypeClassification
import cluster.serialization.NestedSumMatchMode.{FLAT, TREE}

import scala.annotation.tailrec
import scala.quoted.{Expr, Quotes, Type}

/** Compile-time macro derivation for [[Deserializer]] using direct Tasty reflection.
 *
 * Supports monomorphic and parameterized product types (including non-`case` concrete classes with an accessible primary constructor), singleton types (`case object`s and parameterless `enum` cases), and `sealed` sum hierarchies (including nested sum types under [[NestedSumMatchMode.FLAT]] and [[NestedSumMatchMode.TREE]], as well as directly and indirectly recursive types). */
object DeserializerDerivation {

	/** Entry point for [[Deserializer.derive]]. */
	def deriveDeserializerImpl[A: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Deserializer[A]] = {
		import quotes.reflect.*

		MacroTools.classifyType(TypeRepr.of[A]) match {
			case Some(TypeClassification.Singleton(termSymbol)) =>
				'{
					new Deserializer[A] {
						def deserialize(reader: Reader): A = ${ Ref(termSymbol).asExprOf[A] }
					}
				}

			case Some(TypeClassification.ValueClass(underlyingTpe, _, ctor)) =>
				deriveValueClassDeserializer[A](underlyingTpe, ctor)

			case Some(TypeClassification.OpaqueAlias(underlyingTpe)) =>
				deriveOpaqueAliasDeserializer[A](underlyingTpe)

			case Some(TypeClassification.Product(_, _)) =>
				deriveProductDeserializer[A]

			case Some(TypeClassification.Sum(_)) =>
				deriveSumDeserializer[A](modeExpr)

			case None =>
				report.errorAndAbort(s"Cannot derive Deserializer for `${Type.show[A]}`: it is neither a sealed sum type, an opaque type alias, a value class, nor a concrete class with an accessible primary constructor.")
		}
	}

	private def deriveValueClassDeserializer[A: Type](using quotes: Quotes)(underlyingTpe: quotes.reflect.TypeRepr, ctor: quotes.reflect.Term): Expr[Deserializer[A]] = {
		import quotes.reflect.*

		underlyingTpe.asType match {
			case '[underlying] =>
				Implicits.search(TypeRepr.of[Deserializer[underlying]]) match {
					case iss: ImplicitSearchSuccess =>
						val underlyingDeserializer = iss.tree.asExprOf[Deserializer[underlying]]
						'{
							new Deserializer[A] {
								def deserialize(reader: Reader): A = {
									val raw = $underlyingDeserializer.deserialize(reader)
									${ ctor.appliedTo('raw.asTerm).asExprOf[A] }
								}
							}
						}
					case _: NoMatchingImplicits =>
						report.errorAndAbort(s"Cannot derive Deserializer for value class `${Type.show[A]}`: missing given `Deserializer[${Type.show[underlying]}]`.")
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(s"Cannot derive Deserializer for value class `${Type.show[A]}`: ${isf.explanation}")
				}
		}
	}

	private def deriveOpaqueAliasDeserializer[A: Type](using quotes: Quotes)(underlyingTpe: quotes.reflect.TypeRepr): Expr[Deserializer[A]] = {
		import quotes.reflect.*

		underlyingTpe.asType match {
			case '[underlying] =>
				Implicits.search(TypeRepr.of[Deserializer[underlying]]) match {
					case iss: ImplicitSearchSuccess =>
						val underlyingDeserializer = iss.tree.asExprOf[Deserializer[underlying]]
						'{
							new Deserializer[A] {
								def deserialize(reader: Reader): A = {
									$underlyingDeserializer.deserialize(reader).asInstanceOf[A]
								}
							}
						}
					case _: NoMatchingImplicits =>
						report.errorAndAbort(s"Cannot derive Deserializer for opaque type `${Type.show[A]}`: missing given `Deserializer[${Type.show[underlying]}]`.")
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(s"Cannot derive Deserializer for opaque type `${Type.show[A]}`: ${isf.explanation}")
				}
		}
	}

	private def deriveProductDeserializer[P: Type](using quotes: Quotes): Expr[Deserializer[P]] = {
		'{
			new Deserializer[P] {
				def deserialize(reader: Reader): P = {
					given self: Deserializer[P] = this
					${ productDeserializerBodyFor[P, P]('reader, 'self) }
				}
			}
		}
	}

	private def deriveSumDeserializer[S: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Deserializer[S]] = {
		'{
			new Deserializer[S] {
				def deserialize(reader: Reader): S = {
					given self: Deserializer[S] = this
					${ sumDeserializerBodyFor[S]('reader, 'self, modeExpr) }
				}
			}
		}
	}

	/** Generates the deserialization expression that reads all constructor arguments from `readerExpr` and instantiates product type `P`.
	 *
	 * @tparam Root the root type for which [[Deserializer.derive]] was invoked, used to short-circuit direct recursive field calls to `rootSelfExpr`.
	 * @tparam P the product type to instantiate.
	 * @param readerExpr reference to the [[Deserializer.Reader]] parameter of [[Deserializer.deserialize]].
	 * @param rootSelfExpr reference to the enclosing [[Deserializer]] instance for `Root`.
	 * @return an expression that deserializes and constructs an instance of `P`. */
	private def productDeserializerBodyFor[Root: Type, P: Type](
		readerExpr: Expr[Reader],
		rootSelfExpr: Expr[Deserializer[Root]]
	)(using quotes: Quotes): Expr[P] = {
		import quotes.reflect.*

		val productTpe = TypeRepr.of[P].dealias
		val rootTpe = TypeRepr.of[Root].dealias

		MacroTools.classifyType(productTpe) match {
			case Some(TypeClassification.Singleton(termSymbol)) =>
				Ref(termSymbol).asExprOf[P]

			case Some(TypeClassification.ValueClass(underlyingTpe, _, ctor)) =>
				val rawReadTerm: Term = if underlyingTpe.dealias =:= rootTpe then {
					'{ $rootSelfExpr.deserialize($readerExpr) }.asTerm
				} else {
					underlyingTpe.asType match {
						case '[underlying] =>
							Implicits.search(TypeRepr.of[Deserializer[underlying]]) match {
								case iss: ImplicitSearchSuccess =>
									val deserializerExpr = iss.tree.asExprOf[Deserializer[underlying]]
									'{ $deserializerExpr.deserialize($readerExpr) }.asTerm
								case _: NoMatchingImplicits =>
									report.errorAndAbort(s"Missing a given `Deserializer[${Type.show[underlying]}]` for value class `${Type.show[P]}`.")
								case isf: ImplicitSearchFailure =>
									report.errorAndAbort(s"The search of a given `Deserializer[${Type.show[underlying]}]` for value class `${Type.show[P]}` failed with: ${isf.explanation}")
							}
					}
				}
				ctor.appliedTo(rawReadTerm).asExprOf[P]

			case Some(TypeClassification.Product(typedCtorTerm, paramLists)) =>
				def deserializeParam(param: MacroTools.ProductParam[TypeRepr, Term]): Term = {
					val rawReadTerm: Term = if param.tpe.dealias =:= rootTpe then {
						'{ $rootSelfExpr.deserialize($readerExpr) }.asTerm
					} else {
						param.tpe.asType match {
							case '[fieldType] =>
								Implicits.search(TypeRepr.of[Deserializer[fieldType]]) match {
									case iss: ImplicitSearchSuccess =>
										val deserializerExpr = iss.tree.asExprOf[Deserializer[fieldType]]
										'{ $deserializerExpr.deserialize($readerExpr) }.asTerm

									case failure =>
										MacroTools.classifyType(param.tpe) match {
											case Some(TypeClassification.ValueClass(underlyingTpe, _, ctor)) =>
												val rawUnderlyingTerm: Term = if underlyingTpe.dealias =:= rootTpe then {
													'{ $rootSelfExpr.deserialize($readerExpr) }.asTerm
												} else {
													underlyingTpe.asType match {
														case '[underlying] =>
															Implicits.search(TypeRepr.of[Deserializer[underlying]]) match {
																case uIss: ImplicitSearchSuccess =>
																	val uDeserializerExpr = uIss.tree.asExprOf[Deserializer[underlying]]
																	'{ $uDeserializerExpr.deserialize($readerExpr) }.asTerm
																case _: NoMatchingImplicits =>
																	report.errorAndAbort(s"Missing a given `Deserializer[${Type.show[fieldType]}]` or `Deserializer[${Type.show[underlying]}]` for field `${param.name}` of `${Type.show[P]}`.")
																case isf: ImplicitSearchFailure =>
																	report.errorAndAbort(s"The search of a `given Deserializer[${Type.show[underlying]}]` for field `${param.name}` of `${Type.show[P]}` failed with: ${isf.explanation}")
															}
													}
												}
												ctor.appliedTo(rawUnderlyingTerm)

											case _ =>
												failure match {
													case _: NoMatchingImplicits =>
														report.errorAndAbort(s"Missing a given `${Type.show[Deserializer[fieldType]]}` for field `${param.name}` of `${Type.show[P]}`.")
													case isf: ImplicitSearchFailure =>
														report.errorAndAbort(s"The search of a `given ${Type.show[Deserializer[fieldType]]}` for the field `${param.name}` of `${Type.show[P]}` failed with: ${isf.explanation}")
												}
										}
								}
						}
					}

					param.sinceVersion match {
						case Some(v) =>
							val condExpr = '{ $readerExpr.governingVersion.isAtLeast(ProtocolVersion(${ Expr(v) })) }
							val defaultTerm = param.defaultValue.getOrElse {
								report.errorAndAbort(s"Missing default constructor value for field `${param.name}` of `${Type.show[P]}`.")
							}
							If(condExpr.asTerm, rawReadTerm, defaultTerm)
						case None =>
							rawReadTerm
					}
				}

				val argLists: List[List[Term]] = paramLists.map { paramList =>
					paramList.map(param => deserializeParam(param))
				}

				val constructorCall = argLists.foldLeft(typedCtorTerm)((acc, args) => acc.appliedToArgs(args))
				constructorCall.asExprOf[P]

			case _ =>
				report.errorAndAbort(s"Expected product, value class, or singleton type, but got `${Type.show[P]}`.")
		}
	}

	/** Generates the switch expression that reads the VLQ discriminator from `readerExpr` and dispatches to the matching variant of sum type `OuterSum`. */
	private def sumDeserializerBodyFor[OuterSum: Type](
		readerExpr: Expr[Reader],
		rootSelfExpr: Expr[Deserializer[OuterSum]],
		modeExpr: Expr[NestedSumMatchMode]
	)(using quotes: Quotes): Expr[OuterSum] = {
		import quotes.reflect.*

		val mode = modeExpr.valueOrAbort
		given outerTargetName: String = Type.show[Deserializer[OuterSum]]

		val outerVariants = MacroTools.classifyType(TypeRepr.of[OuterSum]) match {
			case Some(TypeClassification.Sum(variants)) =>
				variants
			case _ =>
				report.errorAndAbort(s"Expected sealed sum type, but got `${Type.show[OuterSum]}`.")
		}

		case class GeneratedCase(variantTpe: TypeRepr, discriminator: Int, caseDef: CaseDef)

		def genSumCases[Sum: Type](
			variants: List[TypeRepr],
			alreadyFlattenedCases: List[GeneratedCase]
		): List[GeneratedCase] = {

			val oCriteriaTerm: Option[Term] = MacroTools.findDiscriminationCriteria[Sum]

			@tailrec
			def loop(remainingVariants: List[TypeRepr], alreadyDone: List[GeneratedCase]): List[GeneratedCase] = {
				remainingVariants match {
					case Nil =>
						alreadyDone

					case headTpe :: tailTypes =>
						if mode == FLAT && alreadyDone.exists(_.variantTpe =:= headTpe) then {
							loop(tailTypes, alreadyDone)
						} else {
							headTpe.asType match {
								case '[headType] =>
									def buildGeneratedCase(deserialization: Expr[headType]): GeneratedCase = {
										val discInt: Int = oCriteriaTerm match {
											case Some(dcTerm) =>
												MacroTools.resolveDiscriminator[Sum](dcTerm, headTpe)
											case None =>
												alreadyDone.size
										}
										alreadyDone.find(_.discriminator == discInt).foreach { collision =>
											report.errorAndAbort(
												s"Duplicate discriminator value $discInt in `${Type.show[OuterSum]}`: assigned to both `${MacroTools.simpleNameOf(collision.variantTpe)}` and `${MacroTools.simpleNameOf(headTpe)}`."
											)
										}
										val caseDef = CaseDef(Literal(IntConstant(discInt)), None, deserialization.asTerm)
										GeneratedCase(headTpe, discInt, caseDef)
									}

									Implicits.search(TypeRepr.of[Deserializer[headType]]) match {
										case iss: ImplicitSearchSuccess =>
											val deserializerExpr = iss.tree.asExprOf[Deserializer[headType]]
											val genCase = buildGeneratedCase('{ $deserializerExpr.deserialize($readerExpr) })
											loop(tailTypes, genCase :: alreadyDone)

										case isf: AmbiguousImplicits =>
											report.errorAndAbort(s"During the derivation of a `${Type.show[Deserializer[OuterSum]]}`, the search of a potential `given ${Type.show[Deserializer[headType]]}` failed with: ${isf.explanation}")

										case isf: DivergingImplicit =>
											report.errorAndAbort(s"During the derivation of a `${Type.show[Deserializer[OuterSum]]}`, the search of a potential `given ${Type.show[Deserializer[headType]]}` failed with: ${isf.explanation}")

										case _: ImplicitSearchFailure =>
											MacroTools.classifyType(headTpe) match {
												case Some(TypeClassification.Singleton(_)) | Some(TypeClassification.Product(_, _)) | Some(TypeClassification.ValueClass(_, _, _)) =>
													val deserialization = productDeserializerBodyFor[OuterSum, headType](readerExpr, rootSelfExpr)
													val genCase = buildGeneratedCase(deserialization)
													loop(tailTypes, genCase :: alreadyDone)

												case Some(TypeClassification.Sum(nestedVariantTypes)) =>
													mode match {
														case FLAT =>
															loop(tailTypes, genSumCases[headType](nestedVariantTypes, alreadyDone))
														case TREE =>
															val nestedBindSymbol = Symbol.newBind(
																Symbol.spliceOwner,
																"nestedDiscriminator",
																Flags.EmptyFlags,
																TypeRepr.of[Int]
															)
															val nestedDefaultCase = CaseDef(
																Bind(nestedBindSymbol, Wildcard()),
																None,
																'{ throw new Deserializer.InvalidDiscriminatorException($readerExpr.position, ${ Ref(nestedBindSymbol).asExprOf[Int] }, ${ Expr(Type.show[headType]) }) }.asTerm
															)
															val nestedCases = genSumCases[headType](nestedVariantTypes, Nil).reverse.map(_.caseDef) :+ nestedDefaultCase
															val nestedScrutineeExpr: Expr[Int] = '{ $readerExpr.readUnsignedIntVlq() }
															val deserialization = Match(nestedScrutineeExpr.asTerm, nestedCases).asExprOf[headType]
															val genCase = buildGeneratedCase(deserialization)
															loop(tailTypes, genCase :: alreadyDone)
													}

												case None | Some(TypeClassification.OpaqueAlias(_)) =>
													report.errorAndAbort(s"Cannot derive Deserializer for variant `${Type.show[headType]}` of `${Type.show[OuterSum]}`: it is neither a sealed sum type nor a concrete class with an accessible primary constructor.")
											}
									}
							}
						}
				}
			}

			loop(variants, alreadyFlattenedCases)
		}

		val bindSymbol = Symbol.newBind(
			Symbol.spliceOwner,
			"discriminator",
			Flags.EmptyFlags,
			TypeRepr.of[Int]
		)
		val defaultCase = CaseDef(
			Bind(bindSymbol, Wildcard()),
			None,
			'{ throw new Deserializer.InvalidDiscriminatorException($readerExpr.position, ${ Ref(bindSymbol).asExprOf[Int] }, ${ Expr(Type.show[OuterSum]) }) }.asTerm
		)
		val cases = genSumCases[OuterSum](outerVariants, Nil).reverse.map(_.caseDef) :+ defaultCase
		val scrutineeExpr: Expr[Int] = '{ $readerExpr.readUnsignedIntVlq() }
		Match(scrutineeExpr.asTerm, cases).asExprOf[OuterSum]
	}
}
