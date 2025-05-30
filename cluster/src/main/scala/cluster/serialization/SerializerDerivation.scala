package readren.matrix
package cluster.serialization

import cluster.serialization.NestedSumMatchMode.{FLAT, NEST, TREE}
import cluster.serialization.Serializer.Writer

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object SerializerDerivation {

	def deriveSerializerImpl[A: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Serializer[A]] = {
		import quotes.reflect.*

		Expr.summon[Mirror.Of[A]] match {
			case Some('{ $m: Mirror.ProductOf[`A`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} }) =>
				deriveProductSerializer[A, fieldTypes, fieldLabels]
			case Some('{ $m: Mirror.SumOf[`A`] {type MirroredElemTypes = variantTypes} }) =>
				deriveSumSerializer[A, variantTypes](modeExpr)
			case _ =>
				// TODO add support of non-ADT types as I did in "https://github.com/readren/json-facile"
				report.errorAndAbort(s"Cannot derive Serializer for non-ADT type ${Type.show[A]}")
		}
	}

	private def deriveProductSerializer[P: Type, FieldTypes: Type, FieldLabels: Type](using quotes: Quotes): Expr[Serializer[P]] = {
		'{
			new Serializer[P] {
				def serialize(message: P, writer: Writer): Unit =
					${ productSerializerBodyFor[P, FieldTypes, FieldLabels]('message, 'writer) }
			}
		}
	}

	private def deriveSumSerializer[S: Type, VariantTypes: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Serializer[S]] = {
		'{
			new Serializer[S] {
				def serialize(message: S, writer: Writer): Unit = {
					${ sumSerializerBodyFor[S, VariantTypes]('message, 'writer, modeExpr) }
				}
			}
		}
	}

	private def productSerializerBodyFor[P: Type, FieldTypes: Type, FieldLabels: Type](messageExpr: Expr[P], writerExpr: Expr[Writer])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		@tailrec
		def loop[RemainingFieldTypes: Type, RemainingFieldLabels: Type](alreadyDone: List[Term]): List[Term] = {
			(Type.of[RemainingFieldTypes], Type.of[RemainingFieldLabels]) match {
				case ('[headType *: tailTypes], '[headLabel *: tailLabels]) =>
					val fieldName: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
					val fieldValueExpr: Expr[headType] = Select.unique(messageExpr.asTerm, fieldName).asExprOf[headType]

					Implicits.search(TypeRepr.of[Serializer[headType]]) match {
						case iss: ImplicitSearchSuccess =>
							val serializer = iss.tree.asExprOf[Serializer[headType]]
							val body: Expr[Unit] = '{ $serializer.serialize($fieldValueExpr, $writerExpr) }
							loop[tailTypes, tailLabels](body.asTerm :: alreadyDone)
						case nmi: NoMatchingImplicits =>
							report.errorAndAbort(s"Missing a given `${Type.show[Serializer[headType]]}` for field `$fieldName` of `${Type.show[P]}`)")
						case isf: ImplicitSearchFailure =>
							report.errorAndAbort(s"The search of a given `${Type.show[Serializer[headType]]}` for field `$fieldName` of `${Type.show[P]}`, failed: ${isf.explanation}")
					}

				case ('[EmptyTuple], '[EmptyTuple]) =>
					alreadyDone

				case _ => report.errorAndAbort("Unreachable")
			}
		}

		val body: Term = loop[FieldTypes, FieldLabels](Nil) match {
			case Nil => Literal(UnitConstant())
			case single :: Nil => single
			case last :: othersReversed => Block(othersReversed.reverse, last)
		}
		body.asExprOf[Unit]
	}


	private def sumSerializerBodyFor[OuterSum: Type, OuterVariants: Type](messageExpr: Expr[OuterSum], writerExpr: Expr[Writer], modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		val mode = modeExpr.valueOrAbort
		var caseIndex: Int = 0

		var previousOrdinal = -1
		inline def takeOrdinal: Int = {
			previousOrdinal += 1
			previousOrdinal
		}

		def genSumCases[Sum: Type, Scrutinee: Type, Variants: Type](scrutineeExpr: Expr[Scrutinee], alreadyFlattenedCases: List[CaseDef]): List[CaseDef] = {

			val oSumDiscriminationCriteriaSelect: Option[Select] =
				Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
					case iss: ImplicitSearchSuccess =>
						Some(Select.unique(iss.tree, "discriminator"))
					case nmi: NoMatchingImplicits =>
						None
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(isf.explanation)
				}

			@tailrec
			def loop[RemainingVariants: Type](alreadyDone: List[CaseDef]): List[CaseDef] = {
				Type.of[RemainingVariants] match {
					case '[headType *: tailTypes] =>

						def buildCaseDef(rhsBuilder: Expr[headType] => Term): CaseDef = {
							// Create a unique symbol for the matched value
							val bindSymbol = Symbol.newBind(
								Symbol.spliceOwner,
								s"variant$caseIndex",
								Flags.EmptyFlags,
								TypeRepr.of[headType]
							)
							caseIndex += 1

							// Create the pattern: `case bind: headType =>`
							val pattern = Bind(
								bindSymbol,
								Typed(
									Wildcard(),
									TypeTree.of[headType]
								)
							)
							val bindExpr: Expr[headType] = Ref(bindSymbol).asExprOf[headType]
							CaseDef(pattern, None, rhsBuilder(bindExpr))
						}

						def buildWriteDiscriminatorExpr(defaultValue: Int): Expr[Unit] = {
							val discriminatorExpr = oSumDiscriminationCriteriaSelect match {
								case Some(discriminatorCriteriaSelect) =>
									discriminatorCriteriaSelect.appliedToType(TypeRepr.of[headType]).asExprOf[Int]

								case None =>
									// Fall back to the default value
									Expr(defaultValue)
							}
							'{ $writerExpr.putUnsignedIntVlq($discriminatorExpr) }
						}

						Implicits.search(TypeRepr.of[Serializer[headType]]) match {
							case iss: ImplicitSearchSuccess =>
								val writeDiscriminatorExpr = buildWriteDiscriminatorExpr(alreadyDone.size)
								val serializerExpr = iss.tree.asExprOf[Serializer[headType]]
								val caseDef = buildCaseDef(bindExpr => '{
									$writeDiscriminatorExpr
									$serializerExpr.serialize($bindExpr, $writerExpr)
								}.asTerm)
								loop[tailTypes](caseDef :: alreadyDone)

							case _: NoMatchingImplicits =>
								Expr.summon[Mirror.Of[headType]] match {
									case Some('{ $m: Mirror.ProductOf[`headType`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} }) =>
										val writeDiscriminatorExpr: Expr[Unit] = buildWriteDiscriminatorExpr(if mode == NestedSumMatchMode.NEST then takeOrdinal else alreadyDone.size)
										val caseDef = buildCaseDef(bindExpr => '{
											$writeDiscriminatorExpr
											${ productSerializerBodyFor[headType, fieldTypes, fieldLabels](bindExpr, writerExpr) }
										}.asTerm)
										loop[tailTypes](caseDef :: alreadyDone)

									case Some('{ $m: Mirror.SumOf[`headType`] {type MirroredElemTypes = variantTypes} }) =>
										mode match {
											case FLAT =>
												loop[tailTypes](genSumCases[headType, Scrutinee, variantTypes](scrutineeExpr, alreadyDone))

											case TREE =>
												val caseDef = buildCaseDef { bindExpr =>
													val writeDiscriminatorExpr = buildWriteDiscriminatorExpr(alreadyDone.size)
													val cases = genSumCases[headType, headType, variantTypes](bindExpr, Nil).reverse
													'{
														$writeDiscriminatorExpr
														${ Match(bindExpr.asTerm, cases).asExprOf[Unit] }
													}.asTerm
												}
												loop[tailTypes](caseDef :: alreadyDone)

											case NEST =>
												val caseDef = buildCaseDef { bindExpr =>
													val cases = genSumCases[headType, headType, variantTypes](bindExpr, Nil).reverse
													Match(bindExpr.asTerm, cases)
												}
												loop[tailTypes](caseDef :: alreadyDone)
										}

									case _ =>
										// TODO add support of non-ADT types as I did in "https://github.com/readren/json-facile"
										report.errorAndAbort(s"Cannot derive Serializer for non-ADT type ${Type.show[headType]}")
								}

							case isf: ImplicitSearchFailure =>
								report.errorAndAbort(isf.explanation)
						}

					case '[EmptyTuple] =>
						alreadyDone

					case _ => report.errorAndAbort("Unreachable")
				}
			}

			loop[Variants](alreadyFlattenedCases)
		}

		val cases = genSumCases[OuterSum, OuterSum, OuterVariants](messageExpr, Nil).reverse
		Match(messageExpr.asTerm, cases).asExprOf[Unit]
	}
}