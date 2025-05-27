package readren.matrix
package cluster.serialization

import cluster.serialization.Serializer.Writer

import scala.annotation.tailrec
import scala.compiletime.{erasedValue, summonFrom, summonInline}
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object SerializerDerivation {

	def deriveSerializerImpl[A: Type](mirrorExpr: Expr[Mirror.Of[A]], isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Serializer[A]] = {
		import quotes.reflect.*

		mirrorExpr match {
			case '{ $m: Mirror.ProductOf[`A`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} } =>
				deriveProductSerializer[A, fieldTypes, fieldLabels]
			case '{ $m: Mirror.SumOf[`A`] {type MirroredElemTypes = variantTypes} } =>
				deriveSumSerializer[A, variantTypes](isFlattenModeOnExpr)
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

	private def deriveSumSerializer[S: Type, VariantTypes: Type](isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Serializer[S]] = {
		'{
			new Serializer[S] {
				def serialize(message: S, writer: Writer): Unit = {
					${ sumSerializerBodyFor[S, VariantTypes]('message, 'writer, isFlattenModeOnExpr) }
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
					val fieldLabel: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
					val fieldValueExpr: Expr[headType] = Select.unique(messageExpr.asTerm, fieldLabel).asExprOf[headType]

					Expr.summon[Serializer[headType]] match {
						case Some(serializer) =>
							val body: Expr[Unit] = '{ $serializer.serialize($fieldValueExpr, $writerExpr) }
							loop[tailTypes, tailLabels](body.asTerm :: alreadyDone)


						case None =>
							report.errorAndAbort(s"Missing Serializer for `${Type.show[headType]}` (field `$fieldLabel` in `${Type.show[P]}`)")
					}

				case ('[EmptyTuple], '[EmptyTuple]) =>
					alreadyDone

				case _ => report.errorAndAbort("Unreachable")
			}
		}

		val body: Term = loop[FieldTypes, FieldLabels](Nil) match {
			case Nil => Literal(UnitConstant())
			case single::Nil => single
			case last::othersReversed => Block(othersReversed.reverse, last)
		}
		body.asExprOf[Unit]
	}


	private def sumSerializerBodyFor[OuterSum: Type, OuterVariants: Type](messageExpr: Expr[OuterSum], writerExpr: Expr[Writer], isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		val isFlattenModeOn = isFlattenModeOnExpr.valueOrAbort
		var caseIndex: Int = 0
		
		def genSumCases[Sum: Type, Scrutinee: Type, Variants: Type](scrutineeExpr: Expr[Scrutinee], alreadyFlattenedCases: List[CaseDef]): List[CaseDef] = {

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

						def buildCaseDef(serializationBuilder: Expr[headType] => Expr[Unit]): CaseDef = {
							// Create a unique symbol for the matched value
							val bindSymbol = Symbol.newBind(
								Symbol.spliceOwner,
								s"variant${caseIndex}",
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

							val discriminatorExpr: Expr[Int] = oDiscriminatorCriteriaSelect match {
								case Some(discriminatorCriteriaSelect) =>
									discriminatorCriteriaSelect.appliedToType(TypeRepr.of[headType]).asExprOf[Int]

								case None =>
									// Fall back to the ordinal value
									Expr(alreadyDone.size)
							}

							val rhs = '{
								$writerExpr.putUnsignedIntVlq($discriminatorExpr)
								${ serializationBuilder(bindExpr) }
							}

							CaseDef(pattern, None, rhs.asTerm)
						}

						Expr.summon[Serializer[headType]] match { // TODO replace with Implicits.search to be given-resolution-error friendly
							case Some(serializer) =>
								if isFlattenModeOn && oDiscriminatorCriteriaSelect.isEmpty && TypeRepr.of[headType].typeSymbol.isAbstractType then reportPotentialDiscriminationCriteriasOverlap[OuterSum, headType]
								val caseDef = buildCaseDef(bindExpr => '{ $serializer.serialize($bindExpr, $writerExpr) })
								loop[tailTypes](caseDef :: alreadyDone)
								
							case None =>
								Expr.summon[Mirror.Of[headType]] match {
									case Some('{ $m: Mirror.ProductOf[`headType`] {type MirroredElemTypes = fieldTypes; type MirroredElemLabels = fieldLabels} }) =>
										val caseDef = buildCaseDef { bindExpr =>
											productSerializerBodyFor[headType, fieldTypes, fieldLabels](bindExpr, writerExpr)
										}
										loop[tailTypes](caseDef :: alreadyDone)
										
									case Some('{ $m: Mirror.SumOf[`headType`] {type MirroredElemTypes = variantTypes} }) =>
										if isFlattenModeOn then {
											loop[tailTypes](genSumCases[headType, Scrutinee, variantTypes](scrutineeExpr, alreadyDone))
										} else {
											val caseDef = buildCaseDef { bindExpr =>
												val cases = genSumCases[headType, headType, variantTypes](bindExpr, Nil).reverse
												Match(bindExpr.asTerm, cases).asExprOf[Unit]										
											}
											loop[tailTypes](caseDef :: alreadyDone)
										}

									case _ =>
										// TODO add support of non-ADT types as I did in "https://github.com/readren/json-facile"
										report.errorAndAbort(s"Cannot derive Serializer for non-ADT type ${Type.show[headType]}")
								}
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

	def reportPotentialDiscriminationCriteriasOverlap[ParentSumType: Type, ChildSumType: Type](using quotes: Quotes): Unit =
		quotes.reflect.report.errorAndAbort(s"The flatten mode requires that a given discrimination criteria for the nesting sum-type exist (${Type.show[DiscriminationCriteria[ParentSumType]]} in this case), when a given serializer/deserializer of a nested sum-type (${Type.show[Serializer[ChildSumType]]} in this case) exists.")

}