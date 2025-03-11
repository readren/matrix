package readren.matrix
package cluster.channel

import cluster.channel.{Deserializer, Serializer}

import java.nio.charset.StandardCharsets

object CommonSerializers {

	//// Byte
	given Serializer[Byte] = new Serializer[Byte] {
		override def serialize(message: Byte, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putByte(message)
			Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
		}
	}

	given Deserializer[Byte] = new Deserializer[Byte] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Byte =
			reader.readByte()
	}

	//// Short
	given Serializer[Short] = new Serializer[Short] {
		override def serialize(message: Short, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putShort(message)
			Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
		}
	}

	given Deserializer[Short] = new Deserializer[Short] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Short = reader.readShort()
	}

	//// Int
	given Serializer[Int] = new Serializer[Int] {
		override def serialize(message: Int, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putInt(message)
			Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
		}
	}

	given Deserializer[Int] = new Deserializer[Int] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Int = reader.readInt()
	}

	//// Long
	given Serializer[Long] = new Serializer[Long] {
		override def serialize(message: Long, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putLong(message)
			Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
		}
	}

	given Deserializer[Long] = new Deserializer[Long] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | Long = reader.readLong()
	}

	//// String
	given Serializer[String] = new Serializer[String] {
		override def serialize(message: String, writer: Serializer.Writer): Serializer.Outcome = {
			val bytes = message.getBytes(StandardCharsets.UTF_8) 
			writer.putInt(bytes.length) // TODO use VLQ
			writer.putBytes(bytes)
			Serializer.Success // CAUTION: some serializers assume this serializer always returns `Success`. If that changes don't forget to update them.
		}
	}

	given Deserializer[String] = new Deserializer[String] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | String = {
			val length = reader.readInt() // TODO use VLQ
			val bytes = reader.readBytes(length)
			new String(bytes, StandardCharsets.UTF_8)
		}
	}
}
