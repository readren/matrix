package readren.nexus
package cluster.serialization

/** Represents a version of message-protocol as a monotonic integer, encoded compactly on the wire via VLQ. */
opaque type ProtocolVersion <: AnyVal = Int

object ProtocolVersion {

	/** The identifier of the version corresponding to this project version.
	 * Every time a new version that changes the protocol is released, this identifier must be incremented by one. */
	val OF_THIS_PROJECT: ProtocolVersion = 1
	/** A singular value that indicates no version was specified. */
	val NOT_SPECIFIED: ProtocolVersion = 0

	inline def apply(identifier: Int): ProtocolVersion = identifier

	extension (thisVersion: ProtocolVersion) {
		def identifier: Int = thisVersion
		inline def isNewerThan(otherVersion: ProtocolVersion): Boolean = thisVersion > otherVersion
		inline def isAtLeast(otherVersion: ProtocolVersion): Boolean = thisVersion >= otherVersion
	}

	val serializer: Serializer[ProtocolVersion] = (message: ProtocolVersion, writer: Serializer.Writer) => writer.putUnsignedIntVlq(message)

	given Serializer[ProtocolVersion] = serializer

	val deserializer: Deserializer[ProtocolVersion] = (reader: Deserializer.Reader) => reader.readUnsignedIntVlq()

	given Deserializer[ProtocolVersion] = deserializer

	/**
	 * Defines an ordering where newer versions come first.
	 */
	val newerFirstOrdering: Ordering[ProtocolVersion] =
		(x: ProtocolVersion, y: ProtocolVersion) => java.lang.Integer.compare(y, x)

	given Ordering[ProtocolVersion] = newerFirstOrdering
}
