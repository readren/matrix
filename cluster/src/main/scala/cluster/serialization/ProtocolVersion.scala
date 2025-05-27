package readren.matrix
package cluster.serialization

/** Represents a version of message-protocol in a compact form (one byte), without losing the ability to determine which of a group of versions is the newest, as long as such versions are sufficiently close. */
opaque type ProtocolVersion <: AnyVal = Byte

object ProtocolVersion {

	/** The identifier of the version of the [[ClusterService]]'s [[Protocol]] corresponding to this project version.
	 * Every time a new version of the cluster subproject that changes the [[Protocol]] will be released, this identifier must be circularly incremented by one.
	 * */
	val OF_THIS_PROJECT: ProtocolVersion = 1
	/** A singular value that indicates no version was specified. */
	val NOT_SPECIFIED: ProtocolVersion = 0

	inline def apply(identifier: Byte): ProtocolVersion = identifier

	extension (thisVersion: ProtocolVersion) {
		def identifier: Byte = thisVersion
		def isNewerThan(otherVersion: ProtocolVersion): Boolean = {
			(thisVersion << 24) - (otherVersion << 24) > 0
		}
	}

	val serializer: Serializer[ProtocolVersion] = (message: ProtocolVersion, writer: Serializer.Writer) => writer.putByte(message)

	given Serializer[ProtocolVersion] = serializer

	val deserializer: Deserializer[ProtocolVersion] = (reader: Deserializer.Reader) => reader.readByte()

	given Deserializer[ProtocolVersion] = deserializer

	/**
	 * Defines an ordering where newer versions come first.
	 */
	val newerFirstOrdering: Ordering[ProtocolVersion] =
		(x: ProtocolVersion, y: ProtocolVersion) => if x == y then 0 else if x.isNewerThan(y) then -1 else 1

	given Ordering[ProtocolVersion] = newerFirstOrdering
}