package readren.matrix
package cluster.service

import cluster.channel.{Deserializer, Serializer}

/** Represents a version of the [[ClusterService]]'s [[Protocol]] in a compact form (one byte), without losing the ability to determine which of a group of versions is the newest, as long as such versions are sufficiently close. */
opaque type ProtocolVersion = Byte

object ProtocolVersion {

	/** The identifier of the version of the [[ClusterService]]'s [[Protocol]] corresponding to this project version.
	 * Every time a new version of the cluster subproject that changes the [[Protocol]] will be released, this identifier must be circularly incremented by one.
	 * */
	val OF_THIS_PROJECT: ProtocolVersion = 0

	inline def apply(identifier: Byte): ProtocolVersion = identifier

	extension (thisVersion: ProtocolVersion) {
		def identifier: Byte = thisVersion
		def isNewerThan(otherVersion: ProtocolVersion): Boolean = {
			(thisVersion << 24) - (otherVersion << 24) > 0
		}
	}

	given Serializer[ProtocolVersion] = new Serializer[ProtocolVersion] {
		override def serialize(message: ProtocolVersion, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putByte(message)
			Serializer.Success
		}
	}
	
	given Deserializer[ProtocolVersion] = new Deserializer[ProtocolVersion] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | ProtocolVersion =
			reader.readByte()
	}
}