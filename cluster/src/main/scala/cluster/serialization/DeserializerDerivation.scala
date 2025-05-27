package readren.matrix
package cluster.serialization

import cluster.serialization.Deserializer.Reader

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object DeserializerDerivation {


	def deriveDeserializerImpl[A: Type](mirrorExpr: Expr[Mirror.Of[A]], isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Deserializer[A]] = {
		import quotes.reflect.*

		mirrorExpr match {
			case '{ $m: Mirror.ProductOf[`A`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} } =>
				deriveProductDeserializer[A, fieldTypes, fieldLabels]
			case '{ $m: Mirror.SumOf[`A`] {type MirroredElemTypes = variantTypes} } =>
				deriveSumDeserializer[A, variantTypes](isFlattenModeOnExpr)
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

	private def deriveSumDeserializer[S: Type, VariantTypes: Type](isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Deserializer[S]] = {
		'{
			new Deserializer[S] {
				def deserialize(reader: Reader): S = {
					${ sumDeserializerBodyFor[S, VariantTypes]('reader, isFlattenModeOnExpr) }
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

		def loop[RemainingFieldTypes: Type, RemainingFieldLabels: Type]: List[Term] = {
			(Type.of[RemainingFieldTypes], Type.of[RemainingFieldLabels]) match {
				case ('[headType *: tailTypes], '[headLabel *: tailLabels]) =>
					Expr.summon[Deserializer[headType]] match {
						case Some(deserializerExpr) =>
							'{ $deserializerExpr.deserialize($readerExpr) }.asTerm :: loop[tailTypes, tailLabels]

						case None =>
							val fieldName: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
							report.errorAndAbort(s"Missing Deserializer for `${Type.show[headType]}` (field `$fieldName` in `${Type.show[P]}`)")
					}
				case ('[EmptyTuple], '[EmptyTuple]) =>
					Nil

				case _ => report.errorAndAbort("Unreachable")
			}
		}

		val constructorArgs = loop[FieldTypes, FieldLabels]

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

	private def sumDeserializerBodyFor[OuterSum: Type, OuterVariants: Type](readerExpr: Expr[Reader], isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[OuterSum] = {
		import quotes.reflect.*

		val isFlattenModeOn = isFlattenModeOnExpr.valueOrAbort
		var caseIndex = 0

		def genSumCases[Sum: Type, Variants: Type](alreadyFlattenedCases: List[CaseDef]): List[CaseDef] = {

			val oDiscriminatorCriteriaSelect: Option[Select] =
				Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
					case nmi: NoMatchingImplicits =>
						None
					case isf: ImplicitSearchFailure =>
						report.errorAndAbort(isf.explanation)
						None
					case iss: ImplicitSearchSuccess =>
						Some(Select.unique(iss.tree, "discriminator"))
				}

			@tailrec
			def loop[RemainingVariants: Type](alreadyDone: List[CaseDef]): List[CaseDef] = {
				Type.of[RemainingVariants] match {
					case '[headType *: tailTypes] =>

						def buildCaseDef(deserialization: Expr[headType]): CaseDef = {
							val caseDef: CaseDef = oDiscriminatorCriteriaSelect match {
								case Some(discriminatorCriteriaSelect) =>
									val discriminator = discriminatorCriteriaSelect.appliedToType(TypeRepr.of[headType])
									// TODO reduce the resulting discriminator term to a constant in order to be able to create a Literal pattern and avoid the guard. 
									val bindSymbol = Symbol.newBind(Symbol.spliceOwner, s"d$caseIndex", Flags.EmptyFlags, TypeRepr.of[Int])
									val pattern = Bind(bindSymbol, Wildcard())
									val guard = Select.overloaded(Ref(bindSymbol), "==", Nil, List(discriminator))
									CaseDef(pattern, Some(guard), deserialization.asTerm)

								case None =>
									CaseDef(Literal(IntConstant(alreadyDone.size)), None, deserialization.asTerm)
							}
							caseIndex += 1
							caseDef
						}

						Expr.summon[Deserializer[headType]] match { // TODO replace with Implicits.search to be given-resolution-error friendly
							case Some(deserializer) =>
								if isFlattenModeOn && oDiscriminatorCriteriaSelect.isEmpty && TypeRepr.of[headType].typeSymbol.isAbstractType then SerializerDerivation.reportPotentialDiscriminationCriteriasOverlap[OuterSum, headType]
								val deserialization = '{ $deserializer.deserialize($readerExpr) }
								val caseDef = buildCaseDef(deserialization)
								loop[tailTypes](caseDef :: alreadyDone)

							case None =>
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
