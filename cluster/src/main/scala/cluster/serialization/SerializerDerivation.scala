package readren.matrix
package cluster.serialization

import cluster.serialization.Serializer.Writer
import common.CompileTime
import common.CompileTime.getTypeName

import scala.deriving.Mirror
import scala.quoted.{Expr, Quotes, Type}

object SerializerDerivation {

	private class VariantIndexHolder(var nextValue: Int)

	def deriveSerializerImpl[A: Type](mirrorExpr: Expr[Mirror.Of[A]])(using quotes: Quotes): Expr[Serializer[A]] = {
		import quotes.reflect.*

		mirrorExpr match {
			case '{ $m: Mirror.ProductOf[A] {type MirroredElemTypes = elemTypes; type MirroredElemLabels = elemLabels} } =>
				deriveProductSerializer[A, elemTypes, elemLabels]
			case '{ $m: Mirror.SumOf[A] {type MirroredElemTypes = elemTypes} } =>
				deriveSumSerializer[A, elemTypes](m)
			case _ =>
				report.errorAndAbort(s"Cannot derive Serializer for non-ADT type ${Type.show[A]}")
		}
	}

	private def deriveProductSerializer[A: Type, ElemTypes: Type, ElemLabels: Type](using quotes: Quotes): Expr[Serializer[A]] = {
		'{
			new Serializer[A] {
				def serialize(message: A, writer: Writer): Unit =
					${ productSerializerBodyFor[A, ElemTypes, ElemLabels]('message, 'writer) }
			}
		}
	}

	private def deriveSumSerializer[A: Type, ElemTypes: Type](mirrorExpr: Expr[Mirror.SumOf[A]])(using quotes: Quotes): Expr[Serializer[A]] = {
		'{
			new Serializer[A] {
				def serialize(message: A, writer: Writer): Unit = {
					${ sumSerializerBodyFor[A, ElemTypes]('message, 'writer, new VariantIndexHolder(0)) }
				}
			}
		}
	}

	private def productSerializerBodyFor[A: Type, ElemTypes: Type, ElemLabels: Type](messageExpr: Expr[A], writerExpr: Expr[Writer])(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		def loop[RemainingElemTypes: Type, RemainingElemLabels: Type]: Expr[Unit] = {
			(Type.of[RemainingElemTypes], Type.of[RemainingElemLabels]) match {
				case ('[headType *: tailTypes], '[headLabel *: tailLabels]) =>
					val fieldName: String = Type.valueOfConstant[headLabel].get.asInstanceOf[String]
					val fieldValueExpr: Expr[headType] = Select.unique(messageExpr.asTerm, fieldName).asExprOf[headType]

					Expr.summon[Serializer[headType]] match {
						case Some(serializer) => '{
							$serializer.serialize($fieldValueExpr, $writerExpr)
							${ loop[tailTypes, tailLabels] }
						}

						case None =>
							report.errorAndAbort(s"Missing Serializer for ${Type.show[headType]} (field '$fieldName' in ${Type.show[A]})")
					}

				case ('[EmptyTuple], '[EmptyTuple]) =>
					'{}

				case _ => report.errorAndAbort("Unreachable")
			}
		}

		loop[ElemTypes, ElemLabels]
	}

	private def sumSerializerBodyFor[A: Type, ElemTypes: Type](messageExpr: Expr[A], writerExpr: Expr[Writer], variantIndexHolder: VariantIndexHolder)(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		val oDiscriminatorCriteriaExpr: Option[Expr[DiscriminationCriteria[A]]] = Expr.summon[DiscriminationCriteria[A]]

		def loop[RemainingElemTypes: Type]: List[CaseDef] = {
			Type.of[RemainingElemTypes] match {
				case '[headType *: tailTypes] =>

					// Create a unique symbol for the matched value
					val bindSymbol = Symbol.newVal(
						Symbol.spliceOwner,
						s"variant${variantIndexHolder.nextValue}",
						TypeRepr.of[headType],
						Flags.EmptyFlags,
						Symbol.noSymbol
					)

					// Create the pattern: `case bind: headType =>`
					val pattern = Bind(
						bindSymbol,
						Typed(
							Wildcard(),
							TypeTree.of[headType]
						)
					)

					val bindExpr: Expr[headType & A] = Ref(bindSymbol).asExprOf[headType & A]

					val discriminatorExpr: Expr[Int] = oDiscriminatorCriteriaExpr match {
						case Some(discriminatorCriteriaExpr) =>
							'{ $discriminatorCriteriaExpr.getFor[headType & A]($bindExpr) }

						case None =>
							// Fall back to the alphanumerical index
							Expr(variantIndexHolder.nextValue)
					}

					val nextVariantIndex = variantIndexHolder.nextValue + 1
					variantIndexHolder.nextValue = nextVariantIndex
					val serialization: Expr[Unit] = Expr.summon[Serializer[headType]]
						.fold(serializerBodyFor[headType](bindExpr, writerExpr, variantIndexHolder)) { serializer =>
							if oDiscriminatorCriteriaExpr.isEmpty && TypeRepr.of[headType].typeSymbol.isAbstractType then report.errorAndAbort(s"The default ${getTypeName[DiscriminationCriteria[Nothing]]} does not work when a nested sum-type (${Type.show[headType]} in this case) is not derived or derived outside the macro that derives the outer sum-type (${Type.show[A]} in this case). To solve this problem either remove the ${getTypeName[Serializer[headType]]} of the nested sum-type, or ensure a ${getTypeName[DiscriminationCriteria[A]]} exists is in the context and be consistent with the discriminators criteria used by the nested sum-type serializer (an easy way is by sharing the same ${getTypeName[DiscriminationCriteria[A]]} instance).")
							'{ $serializer.serialize($bindExpr, $writerExpr) }
						}
					// Create the RHS: `serializer.serialize(bind, writer)`
					val rhs =
						if variantIndexHolder.nextValue == nextVariantIndex then '{
							$writerExpr.putIntVlq($discriminatorExpr)
							$serialization
						} else serialization

					val caseDef = CaseDef(pattern, None, rhs.asTerm)
					caseDef :: loop[tailTypes]

				case '[EmptyTuple] =>
					Nil

				case _ => report.errorAndAbort("Unreachable")
			}

		}

		val cases = loop[ElemTypes]
		Match(messageExpr.asTerm, cases).asExprOf[Unit]
	}

	private def serializerBodyFor[A: Type](messageExpr: Expr[A], writerExpr: Expr[Writer], variantIndexHolder: VariantIndexHolder)(using quotes: Quotes): Expr[Unit] = {
		import quotes.reflect.*

		Expr.summon[Mirror.Of[A]] match {
			case Some('{ $m: Mirror.ProductOf[A] {type MirroredElemTypes = elemTypes; type MirroredElemLabels = elemLabels} }) =>
				productSerializerBodyFor[A, elemTypes, elemLabels](messageExpr, writerExpr)
			case Some('{ $m: Mirror.SumOf[A] {type MirroredElemTypes = elemTypes} }) =>
				sumSerializerBodyFor[A, elemTypes](messageExpr, writerExpr, variantIndexHolder)
			case _ =>
				report.errorAndAbort(s"Cannot derive Serializer for non-ADT type ${Type.show[A]}")
		}
	}
}