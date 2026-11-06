package readren.nexus
package cluster.serialization

import cluster.serialization.MacroTools.TypeClassification
import cluster.serialization.NestedSumMatchMode.{FLAT, TREE}

import scala.annotation.tailrec
import scala.quoted.{Expr, Quotes, Type}

/** Defines the custom integer discriminator values associated with each variant of a sum type `S`.
 *
 * Implementations define the [[Discriminator]] match type mapping each subtype of `S` to a non-negative integer literal constant. For parameterized sum types `F[T]`, define a single instance for the wildcard type `DiscriminationCriteria[F[?]]` matching variants with wildcard type arguments (e.g., `case VariantA[?] => 10`); contravariance in `S` makes that instance applicable to `F[T]` for any concrete or abstract type argument `T`.
 *
 * @tparam S the sum type (sealed trait, sealed abstract class, or enum) whose variants are discriminated. */
trait DiscriminationCriteria[-S] {
	/** Match type mapping each variant subtype `P` of `S` to its non-negative integer literal discriminator. */
	type Discriminator[P <: S] <: Int

	/** Returns the compile-time integer constant discriminator associated with variant `P`. */
	transparent inline final def discriminator[P <: S]: Int =
		${ MacroTools.discriminatorOfImpl[S, P]('this) }
}

object DiscriminationCriteria {
	/** Returns the compile-time integer constant discriminator for variant `P` of sum type `S` using the `given` [[DiscriminationCriteria]] in scope. */
	transparent inline def discriminatorOf[S: DiscriminationCriteria as dc, P <: S]: Int = dc.discriminator[P]

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

	/** Enumerates the associations between discriminator values and variants of the specified sum type `S` using [[NestedSumMatchMode.FLAT]]. */
	transparent inline def casesOf[S]: Seq[Entry] =
		${ casesOfImpl[S]('{ NestedSumMatchMode.FLAT }) }

	/** Enumerates the associations between discriminator values and variants of the specified sum type `S`, preserving the order and hierarchy in which the match-cases appear in the match-case construct of [[Serializer]]s and [[Deserializer]]s derived with the specified [[NestedSumMatchMode]].
	 *
	 * @tparam S the sum type (sealed trait, sealed abstract class, or enum) to analyze.
	 * @return a sequence of [[Entry]] instances describing the variant names and discriminator values. */
	transparent inline def casesOf[S](inline mode: NestedSumMatchMode): Seq[Entry] =
		${ casesOfImpl[S]('mode) }

	private def casesOfImpl[OuterSum: Type](modeExpr: Expr[NestedSumMatchMode])(using quotes: Quotes): Expr[Seq[Entry]] = {
		import quotes.reflect.*

		val mode = modeExpr.valueOrAbort
		given outerTargetName: String = s"DiscriminationCriteria.casesOf[${Type.show[OuterSum]}]"

		val outerVariants = MacroTools.classifyType(TypeRepr.of[OuterSum]) match {
			case Some(TypeClassification.Sum(variants)) =>
				variants
			case _ =>
				report.errorAndAbort(s"Type `${Type.show[OuterSum]}` is not a sealed sum type.")
		}

		def enumNest[Sum: Type](variants: List[TypeRepr], alreadyFlattenCases: List[(TypeRepr, Expr[Entry])]): List[(TypeRepr, Expr[Entry])] = {
			val oCriteriaTerm = MacroTools.findDiscriminationCriteria[Sum]

			@tailrec
			def loop(remainingVariants: List[TypeRepr], alreadyDone: List[(TypeRepr, Expr[Entry])]): List[(TypeRepr, Expr[Entry])] = {
				remainingVariants match {
					case Nil =>
						alreadyDone

					case headTpe :: tailVariants =>
						if mode == FLAT && alreadyDone.exists(_._1 =:= headTpe) then {
							loop(tailVariants, alreadyDone)
						} else {
							headTpe.asType match {
								case '[headVariant] =>
									def discriminatorExpr: Expr[Int] = {
										val discInt = oCriteriaTerm match {
											case None =>
												alreadyDone.size
											case Some(dcTerm) =>
												MacroTools.resolveDiscriminator[Sum](dcTerm, headTpe)
										}
										Expr(discInt)
									}

									val variantSimpleName = Expr(MacroTools.simpleNameOf(headTpe))

									Implicits.search(TypeRepr.of[Serializer[headVariant]]) match {
										case _: ImplicitSearchSuccess =>
											val entryExpr = '{ FlatEntry($discriminatorExpr, $variantSimpleName) }
											loop(tailVariants, (headTpe, entryExpr) :: alreadyDone)

										case isf: AmbiguousImplicits =>
											report.errorAndAbort(isf.explanation)

										case isf: DivergingImplicit =>
											report.errorAndAbort(isf.explanation)

										case _: ImplicitSearchFailure =>
											MacroTools.classifyType(headTpe) match {
												case Some(TypeClassification.Singleton(_)) | Some(TypeClassification.Product(_, _)) | Some(TypeClassification.ValueClass(_, _, _)) =>
													val entryExpr = '{ FlatEntry($discriminatorExpr, $variantSimpleName) }
													loop(tailVariants, (headTpe, entryExpr) :: alreadyDone)

												case Some(TypeClassification.Sum(nestedVariants)) =>
													mode match {
														case FLAT =>
															loop(tailVariants, enumNest[headVariant](nestedVariants, alreadyDone))
														case TREE =>
															val nestedEntries = enumNest[headVariant](nestedVariants, Nil).reverse.map(_._2)
															val fertileEntryExpr = '{ TreeEntry($discriminatorExpr, $variantSimpleName, ${ Expr.ofSeq(nestedEntries) }) }
															loop(tailVariants, (headTpe, fertileEntryExpr) :: alreadyDone)
													}

												case None | Some(TypeClassification.OpaqueAlias(_)) =>
													report.errorAndAbort(s"The variant `${Type.show[headVariant]}` of the sum type `${Type.show[OuterSum]}` is neither a sealed sum type nor a concrete product class.")
											}
									}
							}
						}
				}
			}

			loop(variants, alreadyFlattenCases)
		}

		Expr.ofSeq(enumNest[OuterSum](outerVariants, Nil).reverse.map(_._2))
	}

	/** Enumerates the discriminator values associated with each variant of sum type `S` using [[NestedSumMatchMode.FLAT]]. */
	inline def enumDiscriminatorsOf[S]: Tuple =
		enumDiscriminatorsOf[S](NestedSumMatchMode.FLAT)

	/** Enumerates the discriminator values associated with each variant of sum type `S`, preserving the order in which the cases appear in the match-case construct of derived [[Serializer]]s and [[Deserializer]]s.
	 *
	 * @tparam S the sum type (sealed trait, sealed abstract class, or enum) to analyze.
	 * @return a tuple whose elements are the discrimination values, grouped by cases-match. */
	inline def enumDiscriminatorsOf[S](inline mode: NestedSumMatchMode): Tuple = {
		def loop(remaining: Seq[Entry]): Tuple = {
			if remaining.isEmpty then EmptyTuple
			else remaining.head match {
				case FlatEntry(discriminatorValue, _) =>
					discriminatorValue *: loop(remaining.tail)
				case TreeEntry(discriminatorValue, _, nestedVariants) =>
					Tuple2(discriminatorValue, loop(nestedVariants)) *: loop(remaining.tail)
			}
		}
		loop(casesOf[S](mode))
	}
}
