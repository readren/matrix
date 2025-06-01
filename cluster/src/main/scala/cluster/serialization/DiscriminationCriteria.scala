package readren.matrix
package cluster.serialization

import cluster.serialization.NestedSumMatchMode.{FLAT, NEST, TREE}

import scala.annotation.tailrec
import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

trait DiscriminationCriteria[-S] {
	transparent inline def discriminator[P <: S]: Int
}

object DiscriminationCriteria {
	transparent inline def discriminatorOf[S : DiscriminationCriteria as dc, P <: S]: Int = dc.discriminator[P]

	/** Represents a case in a match-case construct. */
	trait Entry
	/** Represents a case with a discriminator that does not nest any other case. */
	case class FlatEntry(discriminatorValue: Int, variantName: String) extends Entry {
		override def toString: String = s"$variantName -> $discriminatorValue"
	}
	/** Represents a case with a discriminator that nests child cases. */
	case class TreeEntry(discriminatorValue: Int, variantName: String, nestedVariants: Seq[Entry]) extends Entry {
		override def toString: String = s"$variantName -> $discriminatorValue ${nestedVariants.mkString("{", ", ", "}")}"
	}
	/** Represents a case without discriminator that nests child cases. Apply to [[Serializer]]s only. */
	case class NestEntry(sumTypeName: String, nestedVariants: Seq[Entry]) extends Entry {
		override def toString: String = s"$sumTypeName -> ${nestedVariants.mkString("{", ", ", "}")}"
	}

	/**
	 * Enumerates the associations between discriminator values variant of sum-type `S`, preserving the order and hierarchy in which the match-cases appear in the match-case construct of derived [[Serializer]]s and [[Deserializer]]s.
	 *
	 * @tparam S The sum-type (sealed trait or enum) to analyze
	 * @return A tuple whose elements are the discrimination values.
	 *
	 * @note Requires implicit instances of `DiscriminationCriteria` and `Mirror.SumOf` for type `S`.
	 */
	transparent inline def casesOf[S: Mirror.SumOf as mirror](mode: NestedSumMatchMode): Seq[Entry] =
		${ casesOfImpl[S, mirror.MirroredElemTypes]('mode) }

	private def casesOfImpl[OuterSum: Type, OuterVariants: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Seq[Entry]] = {
		import quotes.reflect.*

		val mode = modeExpr.valueOrAbort

		var currentNatural = 0
		/** First call returns 0, next call returns 1, and so on. */
		inline def takeNatural: Int = {
			val natural = currentNatural
			currentNatural += 1
			natural
		}

		def simpleNameOf[T: Type]: String = {
			TypeRepr.of[T].typeSymbol.name
		}

		def enumNest[Sum: Type, Variants: Type](alreadyFlattenCases: List[Expr[Entry]]): List[Expr[Entry]] = {

			val oSumDiscriminationCriteriaSelect = Implicits.search(TypeRepr.of[DiscriminationCriteria[Sum]]) match {
				case nmi: NoMatchingImplicits =>
					None
				case isf: ImplicitSearchFailure =>
					report.errorAndAbort(isf.explanation)
				case iss: ImplicitSearchSuccess =>
					Some(Select.unique(iss.tree, "discriminator"))
			}

			@tailrec
			def loop[RemainingVariants: Type](alreadyDone: List[Expr[Entry]]): List[Expr[Entry]] = {
				Type.of[RemainingVariants] match {
					case '[EmptyTuple.type] =>
						alreadyDone

					case '[headVariant *: tailVariants] =>

						def discriminatorExpr: Expr[Int] = {
							oSumDiscriminationCriteriaSelect match {
								case None =>
									Expr(if mode == NEST then takeNatural else alreadyDone.size) // Note that the type is the integer singleton type corresponding to the `index` value.
								case Some(criteria) =>
									criteria.appliedToType(TypeRepr.of[headVariant]).asExprOf[Int] // Note that the type is Int (not a singleton type). TODO narrow the type to the integer singleton corresponding to the discrimination value.
							}
						}

						Implicits.search(TypeRepr.of[Serializer[headVariant]]) match { // TODO consider the Deserializer too.
							case iss: ImplicitSearchSuccess =>
								val entryExpr = '{ FlatEntry($discriminatorExpr, ${Expr(simpleNameOf[headVariant])}) }
								loop[tailVariants](entryExpr :: alreadyDone)

							case nmi: NoMatchingImplicits =>
								Expr.summon[Mirror.Of[headVariant]] match {
									case None =>
										report.errorAndAbort(s"The variant ${Type.show[headVariant]} of the sum type ${Type.show[OuterSum]} is not algebraic.")

									case Some('{ $m: Mirror.ProductOf[`headVariant`] }) =>
										val entryExpr = '{ FlatEntry($discriminatorExpr, ${Expr(simpleNameOf[headVariant])}) }
										loop[tailVariants](entryExpr :: alreadyDone)

									case Some('{ $m: Mirror.SumOf[`headVariant`] {type MirroredElemTypes = nestedVariants} }) =>
										mode match {
											case FLAT =>
												loop[tailVariants](enumNest[headVariant, nestedVariants](alreadyDone))
											case TREE =>
												val nestedVariants: List[Expr[Entry]] = enumNest[headVariant, nestedVariants](Nil).reverse
												val fertileEntryExpr = '{ TreeEntry($discriminatorExpr, ${Expr(simpleNameOf[headVariant])}, ${Expr.ofSeq(nestedVariants)}) }
												loop[tailVariants](fertileEntryExpr :: alreadyDone)
											case NEST =>
												val nestedVariants: List[Expr[Entry]] = enumNest[headVariant, nestedVariants](Nil).reverse
												val nestingEntryExpr = '{ NestEntry(${Expr(simpleNameOf[headVariant])}, ${Expr.ofSeq(nestedVariants)}) }
												loop[tailVariants](nestingEntryExpr :: alreadyDone)
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
		Expr.ofSeq(enumNest[OuterSum, OuterVariants](Nil).reverse)
	}

	/**
	 * Enumerates the discriminator values associated with each variant of sum-type `S`, preserving the order in which the cases appear in the match-case construct of derived [[Serializer]]s and [[Deserializer]]s.
	 *
	 * @tparam S The sum-type (sealed trait or enum) to analyze
	 * @return A tuple whose elements are the discrimination values, grouped by cases-match.
	 *
	 * @note Requires implicit instances of `DiscriminationCriteria` and `Mirror.SumOf` for type `S`.
	 */
	inline def enumDiscriminatorsOf[S: Mirror.SumOf](mode: NestedSumMatchMode): Tuple = {
		def loop(remaining: Seq[Entry]): Tuple = {
			if remaining.isEmpty then EmptyTuple
			else remaining.head match {
				case FlatEntry(discriminatorValue, _) =>
					discriminatorValue *: loop(remaining.tail)
				case NestEntry(_, nestedVariants) =>
					loop(nestedVariants) *: loop(remaining.tail)
				case TreeEntry(discriminatorValue, _, nestedVariants) =>
					Tuple2(discriminatorValue, loop(nestedVariants)) *: loop(remaining.tail)
			}
		}
		loop(casesOf[S](mode))
	}
}
