package readren.matrix
package cluster.channel

import cluster.channel.{Deserializer, Serializer}

import readren.matrix.cluster.channel.Deserializer.{Problem, ValueOrReferenceTest}

import java.nio.charset.StandardCharsets
import scala.util.NotGiven

object CommonSerializers {

	//// Byte
	private val byteSerializer: Serializer[Byte] = (message: Byte, writer: Serializer.Writer) => {
		writer.putByte(message)
		Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
	}
	given Serializer[Byte] = byteSerializer

	private val byteDeserializer: Deserializer[Byte] = (reader: Deserializer.Reader) =>
		reader.readByte()
	given Deserializer[Byte] = byteDeserializer

	//// Short
	private val shortSerializer: Serializer[Short] = (message: Short, writer: Serializer.Writer) => {
		writer.putShort(message)
		Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
	}
	given Serializer[Short] = shortSerializer

	private val shortDeserializer: Deserializer[Short] = (reader: Deserializer.Reader) =>
		reader.readShort()
	given Deserializer[Short] = shortDeserializer

	//// Int
	private val intSerializer: Serializer[Int] = (message: Int, writer: Serializer.Writer) => {
		writer.putInt(message)
		Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
	}
	given Serializer[Int] = intSerializer

	private val intDeserializer: Deserializer[Int] = (reader: Deserializer.Reader) =>
		reader.readInt()
	given Deserializer[Int] = intDeserializer

	//// Long
	private val longSerializer: Serializer[Long] = (message: Long, writer: Serializer.Writer) => {
		writer.putLong(message)
		Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
	}
	given Serializer[Long] = longSerializer

	private val longDeserializer: Deserializer[Long] = (reader: Deserializer.Reader) => reader.readLong()
	given Deserializer[Long] = longDeserializer

	//// String
	private val stringSerializer: Serializer[String] = (message: String, writer: Serializer.Writer) => {
		val bytes = message.getBytes(StandardCharsets.UTF_8)
		writer.putInt(bytes.length) // TODO use VLQ
		writer.putBytes(bytes)
		Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
	}
	given Serializer[String] = stringSerializer

	private val stringDeserializer: Deserializer[String] = (reader: Deserializer.Reader) => {
		val length = reader.readInt() // TODO use VLQ
		val bytes = reader.readBytes(length)
		new String(bytes, StandardCharsets.UTF_8)
	}
	given Deserializer[String] = stringDeserializer

	//// Set
	given setSerializer[E](using sE: Serializer[E]): Serializer[Set[E]] = (message: Set[E], writer: Serializer.Writer) => {
		writer.putIntVlq(message.size)

		val iterator = message.iterator
		var outcome: Serializer.Outcome = Serializer.Success
		while iterator.hasNext && (outcome eq Serializer.Success) do {
			val element = iterator.next()
			outcome = outcome.andThen(element, writer)
		}
		outcome
	}

	given setDeserializer[E](using dE: Deserializer[E], vorE: ValueOrReferenceTest[E]): Deserializer[Set[E]] = new Deserializer[Set[E]] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Set[E] = {
			var count = reader.readIntVlq()
			val builder = Set.newBuilder[E]
			while count > 0 do {
				reader.read[E] match {
					case problem: Problem =>
						return problem
					case k: E @unchecked =>
						builder.addOne(k)
				}
				count -= 1
			}
			builder.result()
		}
	}

	//// Map
	given mapSerializer[K, V](using sK: Serializer[K], sV: Serializer[V]): Serializer[Map[K, V]] = (message: Map[K, V], writer: Serializer.Writer) => {
		writer.putIntVlq(message.size)

		val iterator = message.iterator
		var outcome: Serializer.Outcome = Serializer.Success
		while iterator.hasNext && (outcome eq Serializer.Success) do {
			val entry = iterator.next()
			outcome = outcome.andThen(entry._1, writer).andThen(entry._2, writer)
		}
		outcome
	}

	given mapDeserializer[K, V](using dK: Deserializer[K], dV: Deserializer[V], vorK: ValueOrReferenceTest[K], vorV: ValueOrReferenceTest[V]): Deserializer[Map[K, V]] = new Deserializer[Map[K, V]] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Map[K, V] = {
			var count = reader.readIntVlq()
			val builder = Map.newBuilder[K, V]
			while count > 0 do {
				reader.read[K] match {
					case problem: Problem =>
						return problem
					case k: K @unchecked =>
						reader.read[V] match {
							case problem: Problem => problem
							case v: V @unchecked => builder.addOne(k, v)
						}
				}
				count -= 1
			}
			builder.result()
		}
	}

}