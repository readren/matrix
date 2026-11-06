package readren.nexus
package cluster.serialization

import cluster.serialization.MacroTools.TypeClassification
import cluster.serialization.NestedSumMatchMode.{FLAT, TREE}
import cluster.serialization.Serializer.Writer

import scala.annotation.tailrec
import scala.quoted.{Expr, Quotes, Type}

/** Compile-time macro derivation for [[Serializer]] using direct Tasty reflection.
 *
 * Supports monomorphic and parameterized product types (including non-`case` concrete classes whose primary constructor parameters are exposed as accessible getters), singleton types (`case object`s and parameterless `enum` cases), and `sealed` sum hierarchies (including nested sum types under [[NestedSumMatchMode.FLAT]] and [[NestedSumMatchMode.TREE]], as well as directly and indirectly recursive types). */
object SerializerDerivation {

	/** Entry point for [[Serializer.derive]]. */
	def deriveSerializerImpl[A: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Serializer[A]] = {
		import quotes.reflect.*

		MacroTools.classifyType(TypeRepr.of[A]) match {
			case Some(TypeClassification.Singleton(_)) =>
				'{
					new Serializer[A] {
						def serialize(message: A, writer: Writer): Unit = ()
					}
				}

			case Some(TypeClassification.ValueClass(underlyingTpe, fieldName, _)) =>
				deriveValueClassSerializer[A](underlyingTpe, fieldName)

			case Some(TypeClassification.OpaqueAlias(underlyingTpe)) =>
				deriveOpaqueAliasSerializer[A](underlyingTpe)

			case Some(TypeClassification.Product(_, _)) =>
				deriveProductSerializer[A]

			case Some(TypeClassification.Sum(_)) =>
				deriveSumSerializer[A](modeExpr)

			case None =>
				report.errorAndAbort(s"Cannot derive Serializer for `${Type.show[A]}`: it is neither a sealed sum type, an opaque type alias, a value class, nor a concrete class with an accessible primary constructor.")
		}
	}

	private def deriveValueClassSerializer[A: Type](using quotes: Quotes)(underlyingTpe: quotes.reflect.TypeRepr, fieldName: String): Expr[Serializer[A]] = {
		import quotes.reflect.*

		underlyingTpe.asType match {
			case '[underlying] =>
				Implicits.search(TypeRepr.of[Serializer[underlying]]) match {
					case iss: ImplicitSearchSuccess =>
						val underlyingSerializer = iss.tree.asExprOf[Serializer[underlying]]
						'{
							new Serializer[A] {
								def serialize(message: A, writer: Writer): Unit = {
									val unwrapped = ${ Select.unique('message.asTerm, fieldName).asExprOf[underlying] }
									$underlyingSerializer.serialize(unwrapped, writer)
								}
							}
						}
					case _: NoMatchingImplicits =>
						report.errorAndAbort(s"Cannot derive Serializer for value class `${Type.show[A]}`: missing given `Serializer[${Type.show[underlying]}]` for underlying field `$fieldName`.")
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(s"Cannot derive Serializer for value class `${Type.show[A]}`: ${isf.explanation}")
				}
		}
	}

	private def deriveOpaqueAliasSerializer[A: Type](using quotes: Quotes)(underlyingTpe: quotes.reflect.TypeRepr): Expr[Serializer[A]] = {
		import quotes.reflect.*

		underlyingTpe.asType match {
			case '[underlying] =>
				Implicits.search(TypeRepr.of[Serializer[underlying]]) match {
					case iss: ImplicitSearchSuccess =>
						val underlyingSerializer = iss.tree.asExprOf[Serializer[underlying]]
						'{
							new Serializer[A] {
								def serialize(message: A, writer: Writer): Unit = {
									$underlyingSerializer.serialize(message.asInstanceOf[underlying], writer)
								}
							}
						}
					case _: NoMatchingImplicits =>
						report.errorAndAbort(s"Cannot derive Serializer for opaque type `${Type.show[A]}`: missing given `Serializer[${Type.show[underlying]}]`.")
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(s"Cannot derive Serializer for opaque type `${Type.show[A]}`: ${isf.explanation}")
				}
		}
	}

	private def deriveProductSerializer[P: Type](using quotes: Quotes): Expr[Serializer[P]] = {
		'{
			new Serializer[P] {
				def serialize(message: P, writer: Writer): Unit = {
					given self: Serializer[P] = this
					${ productSerializerBodyFor[P, P]('message, 'writer, 'self) }
				}
			}
		}
	}

	private def deriveSumSerializer[S: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Serializer[S]] = {
		'{
			new Serializer[S] {
				def serialize(message: S, writer: Writer): Unit = {
					given self: Serializer[S] = this
					${ sumSerializerBodyFor[S]('message, 'writer, 'self, modeExpr) }
				}
			}
		}
	}

	/** Generates the serialization statements for all constructor parameters of product type `P`.
	 *
	 * @tparam Root the root type for which [[Serializer.derive]] was invoked, used to short-circuit direct recursive field calls to `rootSelfExpr`.
	 * @tparam P the product type whose fields are serialized. */
	private def productSerializerBodyFor[Root: Type, P: Type](
		messageExpr: Expr[P],
		writerExpr: Expr[Writer],
		rootSelfExpr: Expr[Serializer[Root]]
	)(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		val productTpe = TypeRepr.of[P].dealias
		val rootTpe = TypeRepr.of[Root].dealias

		MacroTools.classifyType(productTpe) match {
			case Some(TypeClassification.Singleton(_)) =>
				'{ () }

			case Some(TypeClassification.ValueClass(underlyingTpe, fieldName, _)) =>
				MacroTools.ensureAccessibleGetter(productTpe, fieldName)
				val fieldValueTerm = Select.unique(messageExpr.asTerm, fieldName)
				val rawStmt: Expr[Unit] = if underlyingTpe.dealias =:= rootTpe then {
					val fieldValueExpr = fieldValueTerm.asExprOf[Root]
					'{ $rootSelfExpr.serialize($fieldValueExpr, $writerExpr) }
				} else {
					underlyingTpe.asType match {
						case '[underlying] =>
							val fieldValueExpr = fieldValueTerm.asExprOf[underlying]
							Implicits.search(TypeRepr.of[Serializer[underlying]]) match {
								case iss: ImplicitSearchSuccess =>
									val serializer = iss.tree.asExprOf[Serializer[underlying]]
									'{ $serializer.serialize($fieldValueExpr, $writerExpr) }
								case _: NoMatchingImplicits =>
									report.errorAndAbort(s"Missing a given `${Type.show[Serializer[underlying]]}` for value class `${Type.show[P]}`.")
								case isf: ImplicitSearchFailure =>
									report.errorAndAbort(s"The search of a given `${Type.show[Serializer[underlying]]}` for value class `${Type.show[P]}` failed with: ${isf.explanation}")
							}
					}
				}
				rawStmt

			case Some(TypeClassification.Product(_, paramLists)) =>
				val flatParams = paramLists.flatten

				@tailrec
				def loop(remaining: List[MacroTools.ProductParam[TypeRepr, Term]], alreadyDone: List[Term]): List[Term] = {
					remaining match {
						case Nil =>
							alreadyDone

						case param :: tail =>
							MacroTools.ensureAccessibleGetter(productTpe, param.name)
							val fieldValueTerm = Select.unique(messageExpr.asTerm, param.name)

							val rawStmt: Expr[Unit] = if param.tpe.dealias =:= rootTpe then {
								val fieldValueExpr = fieldValueTerm.asExprOf[Root]
								'{ $rootSelfExpr.serialize($fieldValueExpr, $writerExpr) }
							} else {
								param.tpe.asType match {
									case '[fieldType] =>
										val fieldValueExpr = fieldValueTerm.asExprOf[fieldType]
										Implicits.search(TypeRepr.of[Serializer[fieldType]]) match {
											case iss: ImplicitSearchSuccess =>
												val serializer = iss.tree.asExprOf[Serializer[fieldType]]
												'{ $serializer.serialize($fieldValueExpr, $writerExpr) }
											case failure =>
												MacroTools.classifyType(param.tpe) match {
													case Some(TypeClassification.ValueClass(underlyingTpe, fieldName, _)) =>
														val underlyingTerm = Select.unique(fieldValueTerm, fieldName)
														val rawUnderlyingStmt: Expr[Unit] = if underlyingTpe.dealias =:= rootTpe then {
															val underlyingExpr = underlyingTerm.asExprOf[Root]
															'{ $rootSelfExpr.serialize($underlyingExpr, $writerExpr) }
														} else {
															underlyingTpe.asType match {
																case '[underlying] =>
																	val underlyingExpr = underlyingTerm.asExprOf[underlying]
																	Implicits.search(TypeRepr.of[Serializer[underlying]]) match {
																		case uIss: ImplicitSearchSuccess =>
																			val uSerializer = uIss.tree.asExprOf[Serializer[underlying]]
																			'{ $uSerializer.serialize($underlyingExpr, $writerExpr) }
																		case _: NoMatchingImplicits =>
																			report.errorAndAbort(s"Missing a given `Serializer[${Type.show[fieldType]}]` or `Serializer[${Type.show[underlying]}]` for field `${param.name}` of `${Type.show[P]}`.")
																		case isf: ImplicitSearchFailure =>
																			report.errorAndAbort(s"The search of a given `Serializer[${Type.show[underlying]}]` for field `${param.name}` of `${Type.show[P]}` failed with: ${isf.explanation}")
																	}
															}
														}
														rawUnderlyingStmt
													case _ =>
														failure match {
															case _: NoMatchingImplicits =>
																report.errorAndAbort(s"Missing a given `${Type.show[Serializer[fieldType]]}` for field `${param.name}` of `${Type.show[P]}`.")
															case isf: ImplicitSearchFailure =>
																report.errorAndAbort(s"The search of a given `${Type.show[Serializer[fieldType]]}` for field `${param.name}` of `${Type.show[P]}` failed with: ${isf.explanation}")
														}
												}
										}
								}
							}

							val stmt: Expr[Unit] = param.sinceVersion match {
								case Some(v) =>
									'{
										if $writerExpr.governingVersion.isAtLeast(ProtocolVersion(${ Expr(v) })) then {
											$rawStmt
										}
									}
								case None =>
									rawStmt
							}

							loop(tail, stmt.asTerm :: alreadyDone)
					}
				}

				val body: Term = loop(flatParams, Nil) match {
					case Nil => Literal(UnitConstant())
					case single :: Nil => single
					case last :: othersReversed => Block(othersReversed.reverse, last)
				}
				body.asExprOf[Unit]

			case _ =>
				report.errorAndAbort(s"Expected product, value class, or singleton type, but got `${Type.show[P]}`.")
		}
	}

	/** Generates the pattern-matching body that writes the variant discriminator and serializes the payload for sum type `OuterSum`. */
	private def sumSerializerBodyFor[OuterSum: Type](
		messageExpr: Expr[OuterSum],
		writerExpr: Expr[Writer],
		rootSelfExpr: Expr[Serializer[OuterSum]],
		modeExpr: Expr[NestedSumMatchMode]
	)(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		val mode = modeExpr.valueOrAbort
		var caseIndex: Int = 0
		given outerTargetName: String = Type.show[Serializer[OuterSum]]

		val outerVariants = MacroTools.classifyType(TypeRepr.of[OuterSum]) match {
			case Some(TypeClassification.Sum(variants)) =>
				variants
			case _ =>
				report.errorAndAbort(s"Expected sealed sum type, but got `${Type.show[OuterSum]}`.")
		}

		def genSumCases[Sum: Type, Scrutinee: Type](
			scrutineeExpr: Expr[Scrutinee],
			variants: List[TypeRepr],
			alreadyFlattenedCases: List[(TypeRepr, CaseDef)]
		): List[(TypeRepr, CaseDef)] = {

			val oCriteriaTerm: Option[Term] = MacroTools.findDiscriminationCriteria[Sum]

			@tailrec
			def loop(remainingVariants: List[TypeRepr], alreadyDone: List[(TypeRepr, CaseDef)]): List[(TypeRepr, CaseDef)] = {
				remainingVariants match {
					case Nil =>
						alreadyDone

					case headTpe :: tailTypes =>
						if mode == FLAT && alreadyDone.exists(_._1 =:= headTpe) then {
							loop(tailTypes, alreadyDone)
						} else {
							headTpe.asType match {
								case '[headType] =>
									def buildCaseDef(rhsBuilder: Expr[headType] => Term): CaseDef = {
										val bindSymbol = Symbol.newBind(
											Symbol.spliceOwner,
											s"variant$caseIndex",
											Flags.EmptyFlags,
											TypeRepr.of[headType]
										)
										caseIndex += 1

										val pattern = Bind(
											bindSymbol,
											Typed(
												Wildcard(),
												TypeTree.of[headType @unchecked]
											)
										)
										val bindExpr: Expr[headType] = Ref(bindSymbol).asExprOf[headType]
										CaseDef(pattern, None, rhsBuilder(bindExpr))
									}

									def buildDiscriminatorWriterStatement(): Expr[Unit] = {
										val discInt: Int = oCriteriaTerm match {
											case Some(dcTerm) =>
												MacroTools.resolveDiscriminator[Sum](dcTerm, headTpe)
											case None =>
												alreadyDone.size
										}
										'{ $writerExpr.putUnsignedIntVlq(${ Expr(discInt) }) }
									}

									Implicits.search(TypeRepr.of[Serializer[headType]]) match {
										case iss: ImplicitSearchSuccess =>
											val writeDiscriminatorExpr = buildDiscriminatorWriterStatement()
											val serializerExpr = iss.tree.asExprOf[Serializer[headType]]
											val caseDef = buildCaseDef(bindExpr => '{
												$writeDiscriminatorExpr
												$serializerExpr.serialize($bindExpr, $writerExpr)
											}.asTerm)
											loop(tailTypes, (headTpe, caseDef) :: alreadyDone)

										case isf: AmbiguousImplicits =>
											report.errorAndAbort(s"During the derivation of a `${Type.show[Serializer[OuterSum]]}`, the search of a potential `given ${Type.show[Serializer[headType]]}` failed with: ${isf.explanation}")

										case isf: DivergingImplicit =>
											report.errorAndAbort(s"During the derivation of a `${Type.show[Serializer[OuterSum]]}`, the search of a potential `given ${Type.show[Serializer[headType]]}` failed with: ${isf.explanation}")

										case _: ImplicitSearchFailure =>
											MacroTools.classifyType(headTpe) match {
												case Some(TypeClassification.Singleton(_)) | Some(TypeClassification.Product(_, _)) | Some(TypeClassification.ValueClass(_, _, _)) =>
													val writeDiscriminatorExpr = buildDiscriminatorWriterStatement()
													val caseDef = buildCaseDef(bindExpr => '{
														$writeDiscriminatorExpr
														${ productSerializerBodyFor[OuterSum, headType](bindExpr, writerExpr, rootSelfExpr) }
													}.asTerm)
													loop(tailTypes, (headTpe, caseDef) :: alreadyDone)

												case Some(TypeClassification.Sum(nestedVariantTypes)) =>
													mode match {
														case FLAT =>
															loop(tailTypes, genSumCases[headType, Scrutinee](scrutineeExpr, nestedVariantTypes, alreadyDone))

														case TREE =>
															val caseDef = buildCaseDef { bindExpr =>
																val writeDiscriminatorExpr = buildDiscriminatorWriterStatement()
																val nestedCases = genSumCases[headType, headType](bindExpr, nestedVariantTypes, Nil).reverse.map(_._2)
																'{
																	$writeDiscriminatorExpr
																	${ Match(bindExpr.asTerm, nestedCases).asExprOf[Unit] }
																}.asTerm
															}
															loop(tailTypes, (headTpe, caseDef) :: alreadyDone)
													}

												case None | Some(TypeClassification.OpaqueAlias(_)) =>
													report.errorAndAbort(s"Cannot derive Serializer for variant `${Type.show[headType]}` of `${Type.show[OuterSum]}`: it is neither a sealed sum type nor a concrete class with an accessible primary constructor.")
											}
									}
							}
						}
				}
			}

			loop(variants, alreadyFlattenedCases)
		}

		val cases = genSumCases[OuterSum, OuterSum](messageExpr, outerVariants, Nil).reverse.map(_._2)
		Match(messageExpr.asTerm, cases).asExprOf[Unit]
	}
}
