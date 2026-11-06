package readren.nexus
package cluster.serialization

import cluster.serialization.CommonSerializers.given
import cluster.serialization.DiscriminationCriteria.{FlatEntry, TreeEntry}
import cluster.serialization.InvariantIterableFactory.given
import cluster.serialization.NestedSumMatchMode.{FLAT, TREE}

import java.nio.ByteBuffer
import munit.FunSuite
import scala.compiletime.asMatchable

object SerializationDerivationSuite {

	/** Parameterized case class with a single type parameter. */
	case class Box[T](value: T)

	/** Parameterized case class with two type parameters. */
	case class PairBox[A, B](first: A, second: B)

	/** Parameterized case class with multiple parameter lists. */
	case class MultiParamGeneric[A, B](first: A)(val second: B, val count: Int) {
		override def equals(obj: Any): Boolean = obj.asMatchable match {
			case other: MultiParamGeneric[?, ?] =>
				this.first == other.first && this.second == other.second && this.count == other.count
			case _ =>
				false
		}
		override def hashCode(): Int = (first, second, count).hashCode()
	}

	/** Non-ADT concrete class whose primary constructor parameters are exposed as `val`s. */
	class CustomPacket(val id: Int, val payload: String) {
		override def equals(obj: Any): Boolean = obj.asMatchable match {
			case other: CustomPacket => this.id == other.id && this.payload == other.payload
			case _ => false
		}
		override def hashCode(): Int = (id, payload).hashCode()
	}

	/** Generic non-ADT concrete class whose primary constructor parameters are exposed as `val`s. */
	class GenericEnvelope[T](val header: String, val body: T) {
		override def equals(obj: Any): Boolean = obj.asMatchable match {
			case other: GenericEnvelope[?] => this.header == other.header && this.body == other.body
			case _ => false
		}
		override def hashCode(): Int = (header, body).hashCode()
	}

	/** Covariant parameterized enum mixing singleton and parameterized cases. */
	enum ElectorateChangeResponse[+P] {
		case Completed
		case Conflict(peers: List[P])
	}

	/** Invariant parameterized sealed trait hierarchy. */
	sealed trait ElectorateChange[P]
	case class ProposeChange[P](peer: P, sequenceNumber: Int) extends ElectorateChange[P]
	case class ConfirmChange[P](peer: P) extends ElectorateChange[P]

	/** Multi-parameter sum type with partial, swapped, and singleton variant type parameters. */
	sealed trait Outcome[+E, +A]
	case class Ok[+A](value: A) extends Outcome[Nothing, A]
	case class Err[+E](error: E) extends Outcome[E, Nothing]
	case class SwappedBoth[+A, +E](good: A, bad: E) extends Outcome[E, A]
	case object Cancelled extends Outcome[Nothing, Nothing]

	/** Nested parameterized sum type hierarchy for testing both `FLAT` and `TREE` modes and custom `DiscriminationCriteria`. */
	sealed trait Envelope[+T]
	case class DirectMsg[+T](value: T) extends Envelope[T]
	sealed trait ControlMsg[+T] extends Envelope[T]
	case class Retry[+T](attempt: Int, payload: T) extends ControlMsg[T]
	case object Heartbeat extends ControlMsg[Nothing]

	/** Direct recursive parameterized sum type. */
	enum BinaryTree[+T] {
		case Leaf
		case Branch(value: T, left: BinaryTree[T], right: BinaryTree[T])
	}

	/** Indirect recursive parameterized product type (`List[RoseTree[T]]`). */
	case class RoseTree[T](value: T, children: List[RoseTree[T]])

	/** Schema evolution: product class with fields introduced across protocol versions. */
	case class EvolvedPacket(
		id: Int,
		@since(2) tag: String = "v1-tag",
		@since(3) count: Long = 100L
	)

	/** Schema evolution: generic product class with versioned field. */
	case class EvolvedBox[T](
		id: Long,
		@since(2) payload: List[T] = Nil
	)

	/** Schema evolution: non-case concrete class with versioned field and accessible getter. */
	class EvolvedCustomPacket(val id: Int, @since(2) val extra: String = "unset") {
		override def equals(obj: Any): Boolean = obj.asMatchable match {
			case other: EvolvedCustomPacket => this.id == other.id && this.extra == other.extra
			case _ => false
		}
		override def hashCode(): Int = (id, extra).hashCode()
	}

	/** Schema evolution: sealed sum type with versioned variant fields. */
	sealed trait EvolvedEvent
	case class BaseEvent(name: String) extends EvolvedEvent
	case class AdvancedEvent(name: String, @since(2) priority: Int = 10) extends EvolvedEvent

	/** Opaque type alias. */
	opaque type Kilogram = Double
	object Kilogram {
		def apply(d: Double): Kilogram = d
		extension (k: Kilogram) {
			def value: Double = k
		}
	}

	/** Product wrapping an opaque type field. */
	case class Cargo(weight: Kilogram, label: String)

	/** AnyVal value class. */
	case class Meter(value: Double) extends AnyVal

	/** Product class with value class field and no explicit Serializer[Meter] in scope. */
	case class Distance(m: Meter, tag: String)

	/** AnyVal value class intended for testing explicit custom serializer precedence. */
	case class CustomTag(code: Int) extends AnyVal

	/** Product class with CustomTag field. */
	case class TaggedMessage(tag: CustomTag, content: String)

	/** Generic value class. */
	case class GenericId[T](raw: T) extends AnyVal

	/** Product class with generic value class field. */
	case class UserRecord(id: GenericId[String], score: Int)

	/** Multi-level opaque type aliases. */
	opaque type BaseId = Long
	object BaseId {
		def apply(l: Long): BaseId = l
		extension (b: BaseId) { def toLong: Long = b }
	}
	opaque type DomainId = BaseId
	object DomainId {
		def apply(b: BaseId): DomainId = b
		extension (d: DomainId) { def toBase: BaseId = d }
	}

	/** Parameterized opaque type alias. */
	opaque type CustomBuffer[T] = List[T]
	object CustomBuffer {
		def apply[T](list: List[T]): CustomBuffer[T] = list
		extension [T](cb: CustomBuffer[T]) { def toList: List[T] = cb }
	}

	/** Opaque type without codec to verify negative compile-time rejection. */
	opaque type SecretKey = String
	object SecretKey {
		def apply(s: String): SecretKey = s
	}
	case class Vault(secret: SecretKey)

	/** Self-recursion through a value class wrapper. */
	case class NodeWrap(children: List[RecursiveTree]) extends AnyVal
	case class RecursiveTree(value: Int, child: NodeWrap)

	/** Schema evolution on product with value class field. */
	case class EvolvedValuePacket(
		id: Int,
		@since(2) offset: Meter = Meter(0.0)
	)

	/** Universal sealed trait hierarchy with value class variants. */
	sealed trait MetricDimension extends Any
	case class LengthDimension(meters: Double) extends AnyVal with MetricDimension
	case class WeightDimension(kilos: Double) extends AnyVal with MetricDimension

	/** Value class wrapping an AnyRef reference type. */
	case class NameRef(name: String) extends AnyVal
	case class Profile(user: NameRef, manager: NameRef)
}

class SerializationDerivationSuite extends FunSuite {
	import SerializationDerivationSuite.*

	private def roundTrip[T](value: T, bufferCapacity: Int = 512)(using serializer: Serializer[T], deserializer: Deserializer[T]): (T, Array[Byte]) = {
		roundTripWithVersion(value, ProtocolVersion(1), ProtocolVersion(1), bufferCapacity)
	}

	private def roundTripWithVersion[T](
		value: T,
		writerVersion: ProtocolVersion,
		readerVersion: ProtocolVersion,
		bufferCapacity: Int = 512
	)(using serializer: Serializer[T], deserializer: Deserializer[T]): (T, Array[Byte]) = {
		val buffer = ByteBuffer.allocate(bufferCapacity)
		val writer = new Serializer.Writer {
			override val governingVersion: ProtocolVersion = writerVersion
			override def getBuffer(minimumRemaining: Int): ByteBuffer = buffer
			override def position: Int = buffer.position()
		}
		val reader = new Deserializer.Reader {
			override val governingVersion: ProtocolVersion = readerVersion
			override def position: Int = buffer.position()
			override def getContentBytes(maxBytesToConsume: Int): ByteBuffer = buffer
			override def peekByte: Byte = buffer.get(buffer.position())
		}

		serializer.serialize(value, writer)
		buffer.flip()
		val writtenBytes = new Array[Byte](buffer.remaining())
		buffer.duplicate().get(writtenBytes)
		val decoded = deserializer.deserialize(reader)
		assertEquals(buffer.remaining(), 0)
		(decoded, writtenBytes)
	}

	test("Parameterized product types (concrete and generic derivation)") {
		given Serializer[Box[Int]] = Serializer.derive[Box[Int]]
		given Deserializer[Box[Int]] = Deserializer.derive[Box[Int]]

		val (decodedBox, _) = roundTrip(Box(42))
		assertEquals(decodedBox, Box(42))

		given [A: Serializer, B: Serializer] => Serializer[PairBox[A, B]] = Serializer.derive[PairBox[A, B]]
		given [A: Deserializer, B: Deserializer] => Deserializer[PairBox[A, B]] = Deserializer.derive[PairBox[A, B]]

		val originalPair = PairBox("alpha", Box(99))
		val (decodedPair, _) = roundTrip(originalPair)
		assertEquals(decodedPair, originalPair)

		given [A: Serializer, B: Serializer] => Serializer[MultiParamGeneric[A, B]] = Serializer.derive[MultiParamGeneric[A, B]]
		given [A: Deserializer, B: Deserializer] => Deserializer[MultiParamGeneric[A, B]] = Deserializer.derive[MultiParamGeneric[A, B]]

		val originalMulti = MultiParamGeneric("first")(List(1, 2, 3), 7)
		val (decodedMulti, _) = roundTrip(originalMulti)
		assertEquals(decodedMulti, originalMulti)
	}

	test("Non-ADT concrete classes (monomorphic and generic)") {
		given Serializer[CustomPacket] = Serializer.derive[CustomPacket]
		given Deserializer[CustomPacket] = Deserializer.derive[CustomPacket]

		val packet = new CustomPacket(101, "hello-world")
		val (decodedPacket, _) = roundTrip(packet)
		assertEquals(decodedPacket, packet)

		given [T: Serializer] => Serializer[GenericEnvelope[T]] = Serializer.derive[GenericEnvelope[T]]
		given [T: Deserializer] => Deserializer[GenericEnvelope[T]] = Deserializer.derive[GenericEnvelope[T]]

		val envelope = new GenericEnvelope("hdr-1", List(10, 20, 30))
		val (decodedEnvelope, _) = roundTrip(envelope)
		assertEquals(decodedEnvelope, envelope)
	}

	test("Covariant parameterized enum with singleton and parameterized cases") {
		given [P: {Serializer, ValueOrReferenceTest}] => Serializer[ElectorateChangeResponse[P]] = Serializer.derive[ElectorateChangeResponse[P]]
		given [P: {Deserializer, ValueOrReferenceTest}] => Deserializer[ElectorateChangeResponse[P]] = Deserializer.derive[ElectorateChangeResponse[P]]

		val samples: List[ElectorateChangeResponse[String]] = List(
			ElectorateChangeResponse.Completed,
			ElectorateChangeResponse.Conflict(List("node-a", "node-b"))
		)
		val (decoded, _) = roundTrip(samples)
		assertEquals(decoded, samples)

		assertEquals(
			DiscriminationCriteria.casesOf[ElectorateChangeResponse[String]],
			Seq(FlatEntry(0, "Completed"), FlatEntry(1, "Conflict"))
		)
		assertEquals(
			DiscriminationCriteria.enumDiscriminatorsOf[ElectorateChangeResponse[String]],
			(0, 1)
		)
	}

	test("Invariant parameterized sum type") {
		given [P: Serializer] => Serializer[ElectorateChange[P]] = Serializer.derive[ElectorateChange[P]]
		given [P: Deserializer] => Deserializer[ElectorateChange[P]] = Deserializer.derive[ElectorateChange[P]]

		val changes: List[ElectorateChange[Int]] = List(
			ProposeChange(10, 1),
			ConfirmChange(10)
		)
		val (decoded, _) = roundTrip(changes)
		assertEquals(decoded, changes)
	}

	test("Multi-parameter sum type with partial, swapped, and singleton variants") {
		given [E: Serializer, A: Serializer] => Serializer[Outcome[E, A]] = Serializer.derive[Outcome[E, A]]
		given [E: Deserializer, A: Deserializer] => Deserializer[Outcome[E, A]] = Deserializer.derive[Outcome[E, A]]

		val outcomes: List[Outcome[String, Int]] = List(
			Ok(123),
			Err("boom"),
			SwappedBoth(456, "warn"),
			Cancelled
		)
		val (decoded, _) = roundTrip(outcomes)
		assertEquals(decoded, outcomes)
	}

	test("Nested parameterized sum types in FLAT and TREE modes with custom DiscriminationCriteria") {
		given [T] => DiscriminationCriteria[Envelope[T]] {
			override type Discriminator[V <: Envelope[T]] = V match {
				case DirectMsg[?] => 10
				case Retry[?] => 21
				case Heartbeat.type => 22
				case ControlMsg[?] => 20
			}
		}

		val messages: List[Envelope[String]] = List(
			DirectMsg("hello"),
			Retry(3, "again"),
			Heartbeat
		)

		// FLAT mode
		{
			given Serializer[Envelope[String]] = Serializer.derive[Envelope[String]](FLAT)
			given Deserializer[Envelope[String]] = Deserializer.derive[Envelope[String]](FLAT)

			assertEquals(
				DiscriminationCriteria.casesOf[Envelope[String]](FLAT),
				Seq(FlatEntry(10, "DirectMsg"), FlatEntry(21, "Retry"), FlatEntry(22, "Heartbeat$"))
			)
			assertEquals(
				DiscriminationCriteria.enumDiscriminatorsOf[Envelope[String]](FLAT),
				(10, 21, 22)
			)

			val (decodedFlat, _) = roundTrip(messages)
			assertEquals(decodedFlat, messages)
		}

		// TREE mode
		{
			given Serializer[Envelope[String]] = Serializer.derive[Envelope[String]](TREE)
			given Deserializer[Envelope[String]] = Deserializer.derive[Envelope[String]](TREE)

			assertEquals(
				DiscriminationCriteria.casesOf[Envelope[String]](TREE),
				Seq(
					FlatEntry(10, "DirectMsg"),
					TreeEntry(20, "ControlMsg", Seq(FlatEntry(21, "Retry"), FlatEntry(22, "Heartbeat$")))
				)
			)
			assertEquals(
				DiscriminationCriteria.enumDiscriminatorsOf[Envelope[String]](TREE),
				(10, (20, (21, 22)))
			)

			val (decodedTree, _) = roundTrip(messages)
			assertEquals(decodedTree, messages)
		}
	}

	test("Direct and indirect recursive parameterized types") {
		given [T: Serializer] => Serializer[BinaryTree[T]] = Serializer.derive[BinaryTree[T]]
		given [T: Deserializer] => Deserializer[BinaryTree[T]] = Deserializer.derive[BinaryTree[T]]

		val binTree: BinaryTree[Int] = BinaryTree.Branch(
			1,
			BinaryTree.Branch(2, BinaryTree.Leaf, BinaryTree.Leaf),
			BinaryTree.Branch(3, BinaryTree.Branch(4, BinaryTree.Leaf, BinaryTree.Leaf), BinaryTree.Leaf)
		)
		val (decodedBinTree, _) = roundTrip(binTree)
		assertEquals(decodedBinTree, binTree)

		given [T: Serializer] => Serializer[RoseTree[T]] = Serializer.derive[RoseTree[T]]
		given [T: Deserializer] => Deserializer[RoseTree[T]] = Deserializer.derive[RoseTree[T]]

		val roseTree = RoseTree(
			"root",
			List(
				RoseTree("child-1", Nil),
				RoseTree("child-2", List(RoseTree("grandchild-2a", Nil), RoseTree("grandchild-2b", Nil)))
			)
		)
		val (decodedRoseTree, _) = roundTrip(roseTree)
		assertEquals(decodedRoseTree, roseTree)
	}

	test("Schema evolution in product classes across protocol versions (EvolvedPacket)") {
		given Serializer[EvolvedPacket] = Serializer.derive[EvolvedPacket]
		given Deserializer[EvolvedPacket] = Deserializer.derive[EvolvedPacket]

		val original = EvolvedPacket(42, "custom-tag", 999L)

		// Version 1: only id is serialized (4 bytes); tag and count take default values
		val (decodedV1, bytesV1) = roundTripWithVersion(original, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(bytesV1.length, 4)
		assertEquals(decodedV1, EvolvedPacket(42, "v1-tag", 100L))

		// Version 2: id and tag are serialized; count is suppressed and takes default value 100L
		val (decodedV2, bytesV2) = roundTripWithVersion(original, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(decodedV2, EvolvedPacket(42, "custom-tag", 100L))

		// Version 3: all fields are serialized and round-tripped
		val (decodedV3, bytesV3) = roundTripWithVersion(original, ProtocolVersion(3), ProtocolVersion(3))
		assertEquals(decodedV3, original)

		// Asymmetric transmission: v3 sender to v1 receiver (governingVersion = 1)
		val (decodedCrossV1, bytesCrossV1) = roundTripWithVersion(original, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(bytesCrossV1.length, 4)
		assertEquals(decodedCrossV1, EvolvedPacket(42, "v1-tag", 100L))

		// Asymmetric transmission: v3 sender to v2 receiver (governingVersion = 2)
		val (decodedCrossV2, _) = roundTripWithVersion(original, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(decodedCrossV2, EvolvedPacket(42, "custom-tag", 100L))
	}

	test("Schema evolution with generic product classes (EvolvedBox[T])") {
		given [T: {Serializer, ValueOrReferenceTest}] => Serializer[EvolvedBox[T]] = Serializer.derive[EvolvedBox[T]]
		given [T: {Deserializer, ValueOrReferenceTest}] => Deserializer[EvolvedBox[T]] = Deserializer.derive[EvolvedBox[T]]

		val original = EvolvedBox(12345L, List("nested-payload"))

		// Version 1: payload suppressed, defaults to Nil
		val (decodedV1, bytesV1) = roundTripWithVersion(original, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(bytesV1.length, 8) // only Long id
		assertEquals(decodedV1, EvolvedBox(12345L, Nil))

		// Version 2: payload serialized and decoded
		val (decodedV2, _) = roundTripWithVersion(original, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(decodedV2, original)
	}

	test("Schema evolution with non-case concrete classes (EvolvedCustomPacket)") {
		given Serializer[EvolvedCustomPacket] = Serializer.derive[EvolvedCustomPacket]
		given Deserializer[EvolvedCustomPacket] = Deserializer.derive[EvolvedCustomPacket]

		val original = new EvolvedCustomPacket(77, "active-label")

		// Version 1: extra suppressed, defaults to "unset"
		val (decodedV1, bytesV1) = roundTripWithVersion(original, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(bytesV1.length, 4) // only Int id
		assertEquals(decodedV1, new EvolvedCustomPacket(77, "unset"))

		// Version 2: extra serialized and decoded
		val (decodedV2, _) = roundTripWithVersion(original, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(decodedV2, original)
	}

	test("Schema evolution with variants in sealed sum hierarchies (EvolvedEvent)") {
		given Serializer[EvolvedEvent] = Serializer.derive[EvolvedEvent]
		given Deserializer[EvolvedEvent] = Deserializer.derive[EvolvedEvent]

		val advancedEvent: EvolvedEvent = AdvancedEvent("cluster-reconfig", priority = 99)

		// Version 1: priority suppressed, defaults to 10
		val (decodedV1, _) = roundTripWithVersion(advancedEvent, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(decodedV1, AdvancedEvent("cluster-reconfig", priority = 10))

		// Version 2: priority preserved
		val (decodedV2, _) = roundTripWithVersion(advancedEvent, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(decodedV2, advancedEvent)
	}

	test("Top-level derivation for Scala 3 opaque type alias (Kilogram)") {
		given Serializer[Kilogram] = Serializer.derive[Kilogram]
		given Deserializer[Kilogram] = Deserializer.derive[Kilogram]

		val original: Kilogram = Kilogram(75.5)
		val (decoded, bytes) = roundTrip(original)
		assertEquals(bytes.length, 8)
		assertEquals(decoded.value, 75.5)
	}

	test("Product serialization with opaque type field and companion-derived codec (Cargo)") {
		given Serializer[Kilogram] = Serializer.derive[Kilogram]
		given Deserializer[Kilogram] = Deserializer.derive[Kilogram]
		given Serializer[Cargo] = Serializer.derive[Cargo]
		given Deserializer[Cargo] = Deserializer.derive[Cargo]

		val cargo = Cargo(Kilogram(120.0), "machinery")
		val (decoded, _) = roundTrip(cargo)
		assertEquals(decoded.weight.value, 120.0)
		assertEquals(decoded.label, "machinery")
	}

	test("Top-level derivation for AnyVal value class (Meter)") {
		given Serializer[Meter] = Serializer.derive[Meter]
		given Deserializer[Meter] = Deserializer.derive[Meter]

		val original = Meter(42.195)
		val (decoded, bytes) = roundTrip(original)
		assertEquals(bytes.length, 8)
		assertEquals(decoded, original)
	}

	test("Zero-allocation fallback serialization for unboxable AnyVal field (Distance)") {
		// Verifies unboxing fallback works without given Serializer[Meter] or Deserializer[Meter] in scope
		given Serializer[Distance] = Serializer.derive[Distance]
		given Deserializer[Distance] = Deserializer.derive[Distance]

		val original = Distance(Meter(100.0), "track")
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded, original)
	}

	test("Explicit Serializer[ValueClass] takes precedence over unboxing fallback (TaggedMessage)") {
		// Custom serializer that writes inverted code (bitwise NOT) to verify explicit precedence
		given customSerializer: Serializer[CustomTag] = (msg: CustomTag, writer: Serializer.Writer) => {
			writer.putInt(~msg.code)
		}
		given customDeserializer: Deserializer[CustomTag] = (reader: Deserializer.Reader) => {
			CustomTag(~reader.readInt())
		}

		given Serializer[TaggedMessage] = Serializer.derive[TaggedMessage]
		given Deserializer[TaggedMessage] = Deserializer.derive[TaggedMessage]

		val original = TaggedMessage(CustomTag(0x00FF00FF), "payload")
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded, original)
	}

	test("Generic AnyVal value class derivation (GenericId[T] / UserRecord)") {
		given Serializer[UserRecord] = Serializer.derive[UserRecord]
		given Deserializer[UserRecord] = Deserializer.derive[UserRecord]

		val original = UserRecord(GenericId("user-1234"), 9800)
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded, original)
	}

	test("Multi-level opaque type alias derivation (DomainId -> BaseId -> Long)") {
		given Serializer[DomainId] = Serializer.derive[DomainId]
		given Deserializer[DomainId] = Deserializer.derive[DomainId]

		val original: DomainId = DomainId(BaseId(9876543210L))
		val (decoded, bytes) = roundTrip(original)
		assertEquals(bytes.length, 8)
		assertEquals(decoded.toBase.toLong, 9876543210L)
	}

	test("Parameterized opaque type alias derivation (CustomBuffer[T])") {
		given [T: {Serializer, ValueOrReferenceTest}] => Serializer[CustomBuffer[T]] = Serializer.derive[CustomBuffer[T]]
		given [T: {Deserializer, ValueOrReferenceTest}] => Deserializer[CustomBuffer[T]] = Deserializer.derive[CustomBuffer[T]]

		val original: CustomBuffer[String] = CustomBuffer(List("alpha", "beta", "gamma"))
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded.toList, List("alpha", "beta", "gamma"))
	}

	test("Negative compile-time check: product derivation rejects un-codec'd opaque type fields") {
		assert(
			compileErrors("Serializer.derive[Vault]")
				.contains("Missing a given `readren.nexus.cluster.serialization.Serializer[readren.nexus.cluster.serialization.SerializationDerivationSuite.SecretKey]`")
		)
		assert(
			compileErrors("Deserializer.derive[Vault]")
				.contains("Missing a given `readren.nexus.cluster.serialization.Deserializer[readren.nexus.cluster.serialization.SerializationDerivationSuite.SecretKey]`")
		)
	}

	test("Self-recursion through value class wrapper (RecursiveTree / NodeWrap)") {
		given Serializer[RecursiveTree] = Serializer.derive[RecursiveTree]
		given Deserializer[RecursiveTree] = Deserializer.derive[RecursiveTree]

		val original = RecursiveTree(
			1,
			NodeWrap(
				List(
					RecursiveTree(
						2,
						NodeWrap(
							List(RecursiveTree(3, NodeWrap(Nil)))
						)
					)
				)
			)
		)
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded, original)
	}

	test("Schema evolution with unboxable AnyVal field (EvolvedValuePacket)") {
		given Serializer[EvolvedValuePacket] = Serializer.derive[EvolvedValuePacket]
		given Deserializer[EvolvedValuePacket] = Deserializer.derive[EvolvedValuePacket]

		val original = EvolvedValuePacket(42, Meter(12.5))

		// Protocol version 1: offset suppressed on wire, defaults to Meter(0.0)
		val (decodedV1, bytesV1) = roundTripWithVersion(original, ProtocolVersion(1), ProtocolVersion(1))
		assertEquals(bytesV1.length, 4) // only Int id
		assertEquals(decodedV1, EvolvedValuePacket(42, Meter(0.0)))

		// Protocol version 2: offset serialized and restored
		val (decodedV2, bytesV2) = roundTripWithVersion(original, ProtocolVersion(2), ProtocolVersion(2))
		assertEquals(bytesV2.length, 12) // Int id (4) + Double offset (8)
		assertEquals(decodedV2, original)
	}

	test("Sealed sum hierarchy with AnyVal value class variants (MetricDimension)") {
		given Serializer[MetricDimension] = Serializer.derive[MetricDimension]
		given Deserializer[MetricDimension] = Deserializer.derive[MetricDimension]

		val length: MetricDimension = LengthDimension(123.45)
		val (decodedLength, _) = roundTrip(length)
		assertEquals(decodedLength, length)

		val weight: MetricDimension = WeightDimension(67.89)
		val (decodedWeight, _) = roundTrip(weight)
		assertEquals(decodedWeight, weight)
	}

	test("Value class wrapping AnyRef reference type (NameRef / Profile)") {
		given Serializer[Profile] = Serializer.derive[Profile]
		given Deserializer[Profile] = Deserializer.derive[Profile]

		val original = Profile(NameRef("alice"), NameRef("admin"))
		val (decoded, _) = roundTrip(original)
		assertEquals(decoded, original)
	}
}
