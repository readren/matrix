package readren.nexus
package cluster.service

import cluster.serialization.{Deserializer, Serializer}

opaque type RingSerial = Short

object RingSerial {

	def create(value: Short = 0): RingSerial = value

	extension (a: RingSerial) {
		inline def nextSerial: RingSerial = (a + 1).toShort
		/** @return true if this [[RingSerial]] is newer (greater) than the specified one. */
		inline def isAheadOf(b: RingSerial): Boolean = (a << 16) - (b << 16) > 0
		inline def isBehindOf(b: RingSerial): Boolean = (a << 16) - (b << 16) < 0
	}

	private val serializer: Serializer[RingSerial] =
		(message: RingSerial, writer: Serializer.Writer) => {
			writer.putShort(message)
		}
	given Serializer[RingSerial] = serializer

	private val deserializer: Deserializer[RingSerial] =
		(reader: Deserializer.Reader) => reader.readShort()
	given Deserializer[RingSerial] = deserializer

}

