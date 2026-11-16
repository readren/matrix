package readren.consensus
package protocol

/** A repeatable supplier of streaming byte channels representing a serialized snapshot.\
 * In Lazy Raft consensus, this is an alias for [[readren.common.ByteSource]], supporting zero-copy network replication, out-of-line byte source queueing, and constant-memory disk-to-socket streaming. */
type SnapshotByteSource = readren.common.ByteSource
val SnapshotByteSource = readren.common.ByteSource
