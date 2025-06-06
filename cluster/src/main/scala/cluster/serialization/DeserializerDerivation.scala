package readren.matrix
package cluster.serialization

import cluster.serialization.Deserializer.Reader

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object DeserializerDerivation {


	def deriveDeserializerImpl[A: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Deserializer[A]] = {
		import quotes.reflect.*

		Expr.summon[Mirror.Of[A]] match {
			case Some('{ $m: Mirror.ProductOf[`A`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} }) =>
				deriveProductDeserializer[A, fieldTypes, fieldLabels]
			case Some('{ $m: Mirror.SumOf[`A`] {type MirroredElemTypes = variantTypes} }) =>
				deriveSumDeserializer[A, variantTypes](modeExpr)
			case _ =>
				// TODO add support of non-ADT types as I did in "https://github.com/readren/json-facile"
				report.errorAndAbort(s"Cannot derive Deserializer for non-ADT type ${Type.show[A]}")
		}
	}

	private def deriveProductDeserializer[P: Type, FieldTypes: Type, FieldLabels: Type](using quotes: Quotes): Expr[Deserializer[P]] = {
		'{
			new Deserializer[P] {
				def deserialize(reader: Reader): P =
					${ productDeserializerBodyFor[P, FieldTypes, FieldLabels]('reader) }
			}
		}
	}

	private def deriveSumDeserializer[S: Type, VariantTypes: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Deserializer[S]] = {
		'{
			new Deserializer[S] {
				def deserialize(reader: Reader): S = {
					${ sumDeserializerBodyFor[S, VariantTypes]('reader, modeExpr) }
				}
			}
		}
	}

	/**
	 * @tparam P the type of the product type to deserialize
	 * @tparam FieldTypes the types of the fields of `A`
	 * @tparam FieldLabels the names of the fields of `A`
	 * @param readerExpr a reference to the reader parameter of the [[Deserializer.deserialize]] method.
	 * @return a body for the [[Deserializer]] */
	private def productDeserializerBodyFor[P: Type, FieldTypes: Type, FieldLabels: Type](readerExpr: Expr[Reader])(using quotes: Quotes): Expr[P] = {
		import quotes.reflect.*

		@tailrec
		def loop[RemainingFieldTypes: Type, RemainingFieldLabels: Type](alreadyDone: List[Term]): List[Term] = {
			(Type.of[RemainingFieldTypes], Type.of[RemainingFieldLabels]) match {
				case ('[headType *: tailTypes], '[headLabel *: tailLabels]) =>

					Implicits.search(TypeRepr.of[Deserializer[headType]]) match {
						case iss: ImplicitSearchSuccess =>
							val deserializerExpr = iss.tree.asExprOf[Deserializer[headType]]
							val fieldValueExpr = '{ $deserializerExpr.deserialize($readerExpr) }
							loop[tailTypes, tailLabels](fieldValueExpr.asTerm :: alreadyDone)

						case nmi: NoMatchingImplicits =>
							val fieldName: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
							report.errorAndAbort(s"Missing a given `${Type.show[Deserializer[headType]]}` for field `$fieldName` of `${Type.show[P]}`)")

						case isf: ImplicitSearchFailure =>
							val fieldName: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
							report.errorAndAbort(s"The search of a `given ${Type.show[Deserializer[headType]]}` for the field `$fieldName` of `${Type.show[P]}` failed with: ${isf.explanation}")

					}

				case ('[EmptyTuple], '[EmptyTuple]) =>
					alreadyDone

				case _ =>
					report.errorAndAbort("Unreachable")
			}
		}

		val constructorArgs = loop[FieldTypes, FieldLabels](Nil).reverse

		val tpe = TypeRepr.of[P]
		tpe match {
			case termRef: TermRef =>
				// Handle enum cases or singleton cases
				Ref(termRef.termSymbol).asExprOf[P]
			case _ =>
				val constructor = tpe.typeSymbol.primaryConstructor
				val constructorTerm = New(Inferred(tpe)).select(constructor)
				val constructorCall = constructorTerm.appliedToArgs(constructorArgs)
				constructorCall.asExprOf[P]
		}
	}

	private def sumDeserializerBodyFor[OuterSum: Type, OuterVariants: Type](readerExpr: Expr[Reader], modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[OuterSum] = {
		import quotes.reflect.*

		val isFlattenModeOn = modeExpr.valueOrAbort != NestedSumMatchMode.TREE
		var caseIndex = 0

		def genSumCases[Sum: Type, Variants: Type](alreadyFlattenedCases: List[CaseDef]): List[CaseDef] = {

			val oSumDiscriminationCriteriaSelect: Option[Select] =
				Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
					case iss: ImplicitSearchSuccess =>
						Some(Select.unique(iss.tree, "discriminator"))
					case nmi: NoMatchingImplicits =>
						None
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(s"During the derivation of a `${Type.show[Deserializer[OuterSum]]}`, the search of a potential `given ${Type.show[DiscriminationCriteria[Sum]]}` failed with: ${isf.explanation}")
				}

			@tailrec
			def loop[RemainingVariants: Type](alreadyDone: List[CaseDef]): List[CaseDef] = {
				Type.of[RemainingVariants] match {
					case '[headType *: tailTypes] =>

						def buildCaseDef(deserialization: Expr[headType]): CaseDef = {
							val caseDef: CaseDef = oSumDiscriminationCriteriaSelect match {
								case Some(discriminatorCriteriaSelect) =>
									val discriminator = discriminatorCriteriaSelect.appliedToType(TypeRepr.of[headType])
									val bindSymbol = Symbol.newBind(Symbol.spliceOwner, s"d$caseIndex", Flags.EmptyFlags, TypeRepr.of[Int])
									val guard = Select.overloaded(Ref(bindSymbol), "==", Nil, List(discriminator))
									// Try to reduce the resulting discriminator term to a constant to be able to create a Literal pattern and avoid the guard.
									guard.underlying match {
										case Apply(Select(_, _), List(literal@Literal(IntConstant(_)))) =>
											CaseDef(literal, None, deserialization.asTerm)
										case _ =>
											CaseDef(Bind(bindSymbol, Wildcard()), Some(guard), deserialization.asTerm)
									}

								case None =>
									CaseDef(Literal(IntConstant(alreadyDone.size)), None, deserialization.asTerm)
							}
							caseIndex += 1
							caseDef
						}

						Implicits.search(TypeRepr.of[Deserializer[headType]]) match {
							case iss: ImplicitSearchSuccess =>
								val deserializerExpr = iss.tree.asExprOf[Deserializer[headType]]
								val caseDef = buildCaseDef('{ $deserializerExpr.deserialize($readerExpr) })
								loop[tailTypes](caseDef :: alreadyDone)

							case nmi: NoMatchingImplicits =>
								Expr.summon[Mirror.Of[`headType`]] match {
									case Some('{ $m: Mirror.ProductOf[`headType`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} }) =>
										val deserialization = productDeserializerBodyFor[headType, fieldTypes, fieldLabels](readerExpr)
										val caseDef = buildCaseDef(deserialization)
										loop[tailTypes](caseDef :: alreadyDone)

									case Some('{ $m: Mirror.SumOf[`headType`] {type MirroredElemTypes = variantTypes} }) =>
										if isFlattenModeOn then {
											loop[tailTypes](genSumCases[headType, variantTypes](alreadyDone))
										} else {
											val nestedCases = genSumCases[headType, variantTypes](Nil).reverse
											val nestedScrutineeExpr: Expr[Int] = '{ $readerExpr.readUnsignedIntVlq() }
											val deserialization = Match(nestedScrutineeExpr.asTerm, nestedCases).asExprOf[headType]
											val caseDef = buildCaseDef(deserialization)
											loop[tailTypes](caseDef :: alreadyDone)
										}


									case _ =>
										// TODO add support of non-ADT types as I did in "https://github.com/readren/json-facile"
										report.errorAndAbort(s"Cannot derive Deserializer for non-ADT type ${Type.show[headType]}")
								}
							case isf: ImplicitSearchFailure =>
								report.errorAndAbort(s"During the derivation of a `${Type.show[Deserializer[OuterSum]]}`, the search of a potential `given ${Type.show[Deserializer[headType]]}` failed with: ${isf.explanation}")
						}

					case '[EmptyTuple] =>
						alreadyDone

					case _ => report.errorAndAbort("Unreachable")
				}

			}
			
			loop[Variants](alreadyFlattenedCases)
		}

		val cases = genSumCases[OuterSum, OuterVariants](Nil).reverse
		val scrutineeExpr: Expr[Int] = '{ $readerExpr.readUnsignedIntVlq() }
		Match(scrutineeExpr.asTerm, cases).asExprOf[OuterSum]
	}
}
