package readren.matrix
package cluster.serialization

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

trait DiscriminationCriteria[-S] {
	transparent inline def discriminator[P <: S]: Int
}

object DiscriminationCriteria {
	transparent inline def discriminatorOf[S : DiscriminationCriteria as dc, P <: S]: Int = dc.discriminator[P]

	/**
	 * Enumerates the discriminator values associated with each concrete variant of sum-type `S`, preserving the order in which the cases appear in the match-case construct of both, the derived [[Serializer]]s and [[Deserializer]]s.
	 *
	 * @tparam S The sum-type (sealed trait or enum) to analyze
	 * @return A tuple whose elements are the discrimination values.
	 *
	 * @note Requires implicit instances of `DiscriminationCriteria` and `Mirror.SumOf` for type `S`.
	 */
	transparent inline def enumDiscriminatorsOf[S: Mirror.SumOf as mirror](inline isFlattenModeOn: Boolean): Tuple =
		${ enumDiscriminatorsOfImpl[S, mirror.MirroredElemTypes]('isFlattenModeOn) }

	private def enumDiscriminatorsOfImpl[OuterSum: Type, OuterVariants: Type](isFlattenModeOnExpr: Expr[Boolean])(using quotes: Quotes): Expr[Tuple] = {
		import quotes.reflect.*

		val isFlattenModeOn = isFlattenModeOnExpr.valueOrAbort

		def enumNest[Sum: Type, Variants: Type](alreadyFlattenCases: List[Expr[Int | Tuple]]): List[Expr[Int | Tuple]] = {

			val oDiscriminatorCriteriaSelect = Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
				case nmi: NoMatchingImplicits =>
					None
				case isf: ImplicitSearchFailure =>
					report.errorAndAbort(isf.explanation)
				case iss: ImplicitSearchSuccess =>
					Some(Select.unique(iss.tree, "discriminator"))
			}
			
			@tailrec
			def loop[RemainingVariants: Type](alreadyDone: List[Expr[Int | Tuple]]): List[Expr[Int | Tuple]] = {
				Type.of[RemainingVariants] match {
					case '[EmptyTuple.type] =>
						alreadyDone

					case '[headVariant *: tailVariants] =>
						
						def discriminatorExpr: Expr[Int] = {
							oDiscriminatorCriteriaSelect match {
								case None => Expr(alreadyDone.size) // Note that the type is the integer singleton type corresponding to the `index` value.
								case Some(criteria) => criteria.appliedToType(TypeRepr.of[headVariant]).asExprOf[Int] // Note that the type is Int (not a singleton type). TODO narrow the type to the integer singleton corresponding to the discrimination value.
							}
						}
						
						Implicits.search(TypeRepr.of[Serializer[headVariant]]) match { // TODO consider the Deserializer too.
							case iss: ImplicitSearchSuccess =>
								loop[tailVariants](discriminatorExpr :: alreadyDone)
								
							case nmi: NoMatchingImplicits =>
								Expr.summon[Mirror.Of[headVariant]] match {
									case None =>
										report.errorAndAbort(s"The variant ${Type.show[headVariant]} of the sum type ${Type.show[OuterSum]} is not algebraic.")

									case Some('{ $m: Mirror.ProductOf[`headVariant`] }) =>
										loop[tailVariants](discriminatorExpr :: alreadyDone)

									case Some('{ $m: Mirror.SumOf[`headVariant`] {type MirroredElemTypes = nestedVariants} }) =>
										if isFlattenModeOn then {
											loop[tailVariants](enumNest[headVariant, nestedVariants](alreadyDone))
										} else {
											val nestedVariants = enumNest[headVariant, nestedVariants](Nil).reverse
											val nestingVariant = '{ ($discriminatorExpr, ${Expr.ofTupleFromSeq(nestedVariants)}) }
											loop[tailVariants](nestingVariant :: alreadyDone)
										}

									case _ => report.errorAndAbort("unreachable")
								}
								
							case isf: ImplicitSearchFailure =>
								report.errorAndAbort(isf.explanation)
						}
				}
			}
			loop[Variants](alreadyFlattenCases)
		}
		Expr.ofTupleFromSeq(enumNest[OuterSum, OuterVariants](Nil).reverse)
	}
}
