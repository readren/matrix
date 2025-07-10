package readren.matrix
package cluster.serialization

import readren.taskflow.Maybe

import java.nio.charset.StandardCharsets
import scala.collection.SortedMap
import scala.reflect.ClassTag

object CommonSerializers {

	//// Short
	private val booleanSerializer: Serializer[Boolean] = (message: Boolean, writer: Serializer.Writer) => writer.putBoolean(message)

	given Serializer[Boolean] = booleanSerializer

	private val booleanDeserializer: Deserializer[Boolean] = (reader: Deserializer.Reader) => reader.readBoolean()

	given Deserializer[Boolean] = booleanDeserializer


	//// Byte
	private val byteSerializer: Serializer[Byte] = (message: Byte, writer: Serializer.Writer) => writer.putByte(message)

	given Serializer[Byte] = byteSerializer

	private val byteDeserializer: Deserializer[Byte] = (reader: Deserializer.Reader) => reader.readByte()

	given Deserializer[Byte] = byteDeserializer

	//// Short
	private val shortSerializer: Serializer[Short] = (message: Short, writer: Serializer.Writer) => writer.putShort(message)

	given Serializer[Short] = shortSerializer

	private val shortDeserializer: Deserializer[Short] = (reader: Deserializer.Reader) => reader.readShort()

	given Deserializer[Short] = shortDeserializer

	//// Int
	private val intSerializer: Serializer[Int] = (message: Int, writer: Serializer.Writer) => writer.putInt(message)

	given Serializer[Int] = intSerializer

	private val intDeserializer: Deserializer[Int] = (reader: Deserializer.Reader) => reader.readInt()

	given Deserializer[Int] = intDeserializer

	//// Long
	private val longSerializer: Serializer[Long] = (message: Long, writer: Serializer.Writer) => writer.putLong(message)

	given Serializer[Long] = longSerializer

	private val longDeserializer: Deserializer[Long] = (reader: Deserializer.Reader) => reader.readLong()

	given Deserializer[Long] = longDeserializer

	//// Array
	given [T: Serializer as sT] => Serializer[Array[T]] = (message: Array[T], writer: Serializer.Writer) => {
		val length = message.length
		writer.putUnsignedIntVlq(length)
		var index = 0
		while index < length do {
			sT.serialize(message(index), writer)
			index += 1
		}
	}

	given [T: {ClassTag, Deserializer as dT}] => Deserializer[Array[T]] = (reader: Deserializer.Reader) => {
		val length = reader.readUnsignedIntVlq()
		val array = new Array[T](length)
		var index = 0
		while index < length do {
			array(index) = dT.deserialize(reader)
			index += 1
		}
		array
	}

	//// String
	private val stringSerializer: Serializer[String] =
		(message: String, writer: Serializer.Writer) => {
			val bytes = message.getBytes(StandardCharsets.UTF_8)
			writer.putUnsignedIntVlq(bytes.length)
			writer.putBytes(bytes)
		}

	given Serializer[String] = stringSerializer

	private val stringDeserializer: Deserializer[String] =
		(reader: Deserializer.Reader) => {
			val length = reader.readUnsignedIntVlq()
			val bytes = reader.readBytes(length)
			new String(bytes, StandardCharsets.UTF_8)
		}

	given Deserializer[String] = stringDeserializer

	//// Maybe
	given [E] =>(sE: Serializer[E]) => Serializer[Maybe[E]] = (message, writer) =>
		message.fold(writer.putBoolean(false)) { e =>
			writer.putBoolean(true)
			sE.serialize(e, writer)
		}

	given [E] =>(dE: Deserializer[E]) => Deserializer[Maybe[E]] = reader =>
		if reader.readBoolean() then Maybe.some(dE.deserialize(reader))
		else Maybe.empty

	/// Iterable
	given iterableSerializer: [E, Ic[e] <: Iterable[e]] =>(sE: Serializer[E]) =>Serializer[Ic[E]] {
		override def serialize(iterable: Ic[E], writer: Serializer.Writer): Unit = {
			writer.putUnsignedIntVlq(iterable.size)
			iterable.foreach(e => sE.serialize(e, writer))
		}
	}

	given iterableDeserializer: [E, Ic[e] <: Iterable[e]] =>(dE: Deserializer[E], vorE: ValueOrReferenceTest[E], iif: InvariantIterableFactory[Ic]) =>Deserializer[Ic[E]] {
		override def deserialize(reader: Deserializer.Reader): Ic[E] = {
			var count = reader.readUnsignedIntVlq()
			val builder = iif.factory.newBuilder[E]
			while count > 0 do {
				builder.addOne(reader.read[E])
				count -= 1
			}
			builder.result()
		}
	}

	//// Unsorted Map
	given mapSerializer: [K, V, Mc[k, v] <: scala.collection.Map[k, v]] =>(sK: Serializer[K], sV: Serializer[V]) =>Serializer[Mc[K, V]] {
		override def serialize(map: Mc[K, V], writer: Serializer.Writer): Unit = {
			writer.putIntVlq(map.size)
			map.foreach { (k, v) =>
				sK.serialize(k, writer)
				sV.serialize(v, writer)
			}
		}
	}

	given mapDeserializer: [K, V, Mc[k, v] <: scala.collection.Map[k, v]] =>(dK: Deserializer[K], dV: Deserializer[V], vorK: ValueOrReferenceTest[K], vorV: ValueOrReferenceTest[V], imf: InvariantMapFactory[Mc]) =>Deserializer[Mc[K, V]] {
		override def deserialize(reader: Deserializer.Reader): Mc[K, V] = {
			var count = reader.readIntVlq()
			val builder = imf.factory.newBuilder[K, V]
			while count > 0 do {
				builder.addOne(dK.deserialize(reader), dV.deserialize(reader))
				count -= 1
			}
			builder.result()
		}
	}

	//// Sorted Map
	given sortedMapSerializer: [K, V, Mc[k, v] <: scala.collection.SortedMap[k, v]] =>(sK: Serializer[K], sV: Serializer[V]) =>Serializer[Mc[K, V]] {
		override def serialize(map: Mc[K, V], writer: Serializer.Writer): Unit = {
			writer.putIntVlq(map.size)
			map.foreach { (k, v) =>
				sK.serialize(k, writer)
				sV.serialize(v, writer)
			}
		}
	}

	given sortedMapDeserializer: [K, V, Mc[k, v] <: scala.collection.SortedMap[k, v]] =>(dK: Deserializer[K], dV: Deserializer[V], vorK: ValueOrReferenceTest[K], vorV: ValueOrReferenceTest[V], imf: InvariantSortedMapFactory[Mc], oK: Ordering[K]) =>Deserializer[Mc[K, V]] {
		override def deserialize(reader: Deserializer.Reader): Mc[K, V] = {
			var count = reader.readIntVlq()
			val builder = imf.factory.newBuilder[K, V]
			while count > 0 do {
				builder.addOne(dK.deserialize(reader), dV.deserialize(reader))
				count -= 1
			}
			builder.result()
		}
	}
}