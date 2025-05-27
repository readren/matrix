import readren.matrix.cluster.serialization.{Deserializer, ProtocolVersion, Serializer}

import java.nio.ByteBuffer


object model {
	sealed trait Thing

	case class Rare(description: String) extends Thing

	sealed trait CarPart(val ordinal: Int) extends Thing

	case class Wheel(diameter: Int) extends CarPart(11)

	case class Motor(power: String) extends CarPart(12)

	case class Fails(crash: Int) extends CarPart(13)
}
import model.*
import readren.matrix.cluster.serialization.CommonSerializers.given

val serializer = Serializer.derive[Thing] 
val deserializer = Deserializer.derive[Thing]

val buffer = ByteBuffer.allocate(20)
val writer = new Serializer.Writer {
	override val governingVersion: ProtocolVersion = ProtocolVersion(1)
	override def getBuffer(minimumRemaining: Int) = buffer
	override def position = buffer.position()
}
val reader = new Deserializer.Reader {
	override def governingVersion = ProtocolVersion(1)
	override def position = buffer.position()
	override def getContentBytes(maxBytesToConsume: Int) = buffer
	override def peekByte = buffer.get(buffer.position())
}

serializer.serialize(Motor("polenta"), writer)
val motor = deserializer.deserialize(reader)

