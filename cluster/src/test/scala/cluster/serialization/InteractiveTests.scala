package readren.matrix
package cluster.serialization

import cluster.serialization
import cluster.serialization.CommonSerializers.given
import cluster.serialization.DiscriminationCriteria.{Entry, FlatEntry, TreeEntry}
import cluster.serialization.MacroTools.showCode
import cluster.serialization.InvariantIterableFactory.given
import cluster.serialization.ProtocolVersion

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.compiletime.{erasedValue, summonFrom}


object InteractiveTests {

	sealed trait Animal

	case class Dog(field: String) extends Animal

	object animalDc extends DiscriminationCriteria[Animal] {
		override transparent inline def discriminator[S <: Animal]: Int = 32
	}

	given animalDc.type = animalDc

	val thirtyTwo: 32 = summon[DiscriminationCriteria[Animal]].discriminator[Dog]

	sealed trait Thing

	case class Rare(description: String) extends Thing

	sealed trait CarPart extends Thing

	case class Wheel(diameter: Int) extends CarPart

	case class Motor(power: String) extends CarPart

	case object Glass extends CarPart


	@main def runIt(): Unit = {
		
		scribe.error("Scribe is working fine")
		transparent inline def carPartDiscriminatorByVariant[V <: CarPart] = {
			inline erasedValue[V] match {
				case _: Wheel => 21
				case _: Motor => 22
				case _: Glass.type => 23
			}
		}

		object carPartDiscriminatorCriteria extends DiscriminationCriteria[CarPart] {
			override transparent inline def discriminator[V <: CarPart]: Int = carPartDiscriminatorByVariant[V]
		}

		transparent inline def thingDiscriminatorByVariant[V <: Thing]: Int =
			inline erasedValue[V] match {
				case _: Rare => 10
				case _: Wheel => 111
				case _: Motor => 112
				case _: Glass.type => 113
				case _: CarPart => 11
			}

		object thingDiscriminatorCriteria extends DiscriminationCriteria[Thing] {
			override transparent inline def discriminator[V <: Thing]: Int = thingDiscriminatorByVariant[V]
		}

		val buffer = ByteBuffer.allocate(20)
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = ProtocolVersion(1)

			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer

			override def position: Int = buffer.position()
		}
		val reader = new Deserializer.Reader {
			override def governingVersion: ProtocolVersion = ProtocolVersion(1)

			override def position: Int = buffer.position()

			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = buffer

			override def peekByte: Byte = buffer.get(buffer.position())
		}

		@tailrec
		def peekUnsignedIntVlqAt(depth: Int): Int = {
			val value = reader.readUnsignedIntVlq()
			if depth > 0 then {
				print(s"(skipped=$value) ")
				peekUnsignedIntVlqAt(depth - 1)
			} else {
				buffer.position(0)
				println(s"peekedDiscriminator=$value")
				value
			}
		}

		val parts: List[CarPart] = List(Wheel(37), Motor("987"), Glass)
		val original = List(Rare("zxy"), Wheel(37), Motor("987"), Glass)

		//////////
		//// When mode is TREE
		//////////
		println("When mode IS TREE")
		{
			inline val mode = NestedSumMatchMode.TREE
			given NestedSumMatchMode = mode
			val motorDiscriminatorDepth = if mode == NestedSumMatchMode.TREE then 1 else 0

			//////////
			println("Dealing with the serializer/deserializer of the nested Sum type (CarPart) directly")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (0, 1, 2), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(0, "Wheel"), FlatEntry(1, "Motor"), FlatEntry(2, "Glass$")))

					given Serializer[CarPart] = showCode(Serializer.derive[CarPart](mode))
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
				// When only a criteria for a nested Sum exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (111, 112, 113), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(111, "Wheel"), FlatEntry(112, "Motor"), FlatEntry(113, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
			}

			//////////
			println("Dealing with the serializer/deserializer of the outer Sum type (Thing) when no serializer/deserializer for the nested Sum exists.")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, (1, (0, 1, 2))), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), TreeEntry(1, "CarPart", Seq(FlatEntry(0, "Wheel"), FlatEntry(1, "Motor"), FlatEntry(2, "Glass$")))))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for a nested Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, (1, (21, 22, 23))), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), TreeEntry(1, "CarPart", Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, (11, (111, 112, 113))), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), TreeEntry(11, "CarPart", Seq(FlatEntry(111, "Wheel"), FlatEntry(112, "Motor"), FlatEntry(113, "Glass$")))))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, (11, (21, 22, 23))), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), TreeEntry(11, "CarPart", Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
			}

			//////////
			println("Dealing with the serializer/deserializer of the outer Sum type (Thing) when a serializer/deserializer for the nested Sum exists")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 1), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(1, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for a nested Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 1), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(1, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 11), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(11, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 11), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(11, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
			}
		}

		//////////
		println("When mode is FLAT")
		//////////
		{
			inline val mode = NestedSumMatchMode.FLAT
			given NestedSumMatchMode = mode
			val motorDiscriminatorDepth = if mode == NestedSumMatchMode.TREE then 1 else 0

			//////////
			println("Dealing with the serializer/deserializer of the nested Sum type (CarPart) directly")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (0, 1, 2), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(0, "Wheel"), FlatEntry(1, "Motor"), FlatEntry(2, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)

				}
				// When only a criteria for a nested Sum exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (111, 112, 113), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(111, "Wheel"), FlatEntry(112, "Motor"), FlatEntry(113, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[CarPart](mode)
					assert(enumeration == (21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[CarPart](mode)
					assert(cases == Seq(FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)
					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					test(parts)
				}
			}

			//////////
			println("Dealing with the serializer/deserializer of the outer Sum type (Thing) when no serializer/deserializer for the nested Sum exists.")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 1, 2, 3), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(1, "Wheel"), FlatEntry(2, "Motor"), FlatEntry(3, "Glass$")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for a nested Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 111, 112, 113), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(111, "Wheel"), FlatEntry(112, "Motor"), FlatEntry(113, "Glass$")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 21, 22, 23), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(21, "Wheel"), FlatEntry(22, "Motor"), FlatEntry(23, "Glass$")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
			}

			//////////
			println("Dealing with the serializer/deserializer of the outer Sum type (Thing) when a serializer/deserializer for the nested Sum exists.")
			//////////
			{
				// When no criteria exist.
				{
					buffer.clear()

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 1), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(1, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for a nested Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (0, 1), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(0, "Rare"), FlatEntry(1, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When only a criteria for the outer Sum exists.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 11), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(11, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
				// When a criteria for both, a nesting and a nested sum, exist.
				{
					buffer.clear()

					transparent inline given DiscriminationCriteria[CarPart] = carPartDiscriminatorCriteria

					transparent inline given DiscriminationCriteria[Thing] = thingDiscriminatorCriteria

					given Serializer[CarPart] = Serializer.derive[CarPart](mode)

					given Deserializer[CarPart] = Deserializer.derive[CarPart](mode)

					val enumeration = DiscriminationCriteria.enumDiscriminatorsOf[Thing](mode)
					assert(enumeration == (10, 11), s"found: $enumeration")
					val cases = DiscriminationCriteria.casesOf[Thing](mode)
					assert(cases == Seq(FlatEntry(10, "Rare"), FlatEntry(11, "CarPart")))

					given Serializer[Thing] = Serializer.derive[Thing](mode)
					given Deserializer[Thing] = Deserializer.derive[Thing](mode)
					test(original)
				}
			}
		}

		inline def test[X: {Serializer, Deserializer, ValueOrReferenceTest}](original: List[X])(using mode: NestedSumMatchMode): Unit = {
			buffer.clear()
			Serializer[List[X]].serialize(original, writer)
			buffer.flip()
			println(s"buffer: ${buffer.array().map(e => f"$e%4d").mkString(",")}")
			val cloned = Deserializer[List[X]].deserialize(reader)
			assert(original == cloned)
		}
	}
}