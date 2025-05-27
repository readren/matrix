package readren.matrix
package cluster.serialization

import java.nio.charset.StandardCharsets
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

	//// Set
	given setSerializer: [E] =>(sE: Serializer[E]) => Serializer[Set[E]] =
	(set: Set[E], writer: Serializer.Writer) => {
		writer.putIntVlq(set.size)
		set.foreach(e => sE.serialize(e, writer))
	}

	given setDeserializer: [E] =>(dE: Deserializer[E], vorE: ValueOrReferenceTest[E]) => Deserializer[Set[E]] =
		(reader: Deserializer.Reader) => {
			var count = reader.readIntVlq()
			val builder = Set.newBuilder[E]
			while count > 0 do {
				builder.addOne(reader.read[E])
				count -= 1
			}
			builder.result()
		}

	//// Map
	given mapSerializer: [K, V] =>(sK: Serializer[K], sV: Serializer[V]) => Serializer[Map[K, V]] =
	(map: Map[K, V], writer: Serializer.Writer) => {
		writer.putIntVlq(map.size)
		map.foreach { (k, v) =>
			sK.serialize(k, writer)
			sV.serialize(v, writer)
		}
	}

	given mapDeserializer: [K, V] =>(dK: Deserializer[K], dV: Deserializer[V], vorK: ValueOrReferenceTest[K], vorV: ValueOrReferenceTest[V]) => Deserializer[Map[K, V]] =
		(reader: Deserializer.Reader) => {
			var count = reader.readIntVlq()
			val builder = Map.newBuilder[K, V]
			while count > 0 do {
				builder.addOne(dK.deserialize(reader), dV.deserialize(reader))
				count -= 1
			}
			builder.result()
		}

}