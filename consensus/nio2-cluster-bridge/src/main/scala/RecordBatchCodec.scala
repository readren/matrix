package readren.consensus
package nio2

import protocol.*
import readren.common.{ByteSource, ChunkedBufferByteSource}
import readren.nexus.cluster.serialization.*

import java.nio.ByteBuffer
import scala.util.control.NonFatal

/** Immutable pre-encoded binary representation of an `IArray[Record]` produced in an isolated reference-deduplication scope.
 * @param chunks the immutable sequence of read-only or flipped heap [[ByteBuffer]] chunks holding the serialized batch bytes.
 * @param byteLength the exact total number of serialized bytes across all [[chunks]].
 * @param governingVersion the [[ProtocolVersion]] used to encode this batch.*/
final class EncodedRecordBatch(
	val chunks: IArray[ByteBuffer],
	val byteLength: Long,
	val governingVersion: ProtocolVersion
) {

	/** Projects this pre-encoded batch as a repeatable, multi-session [[ByteSource]] for out-of-line trailing stream transmission. */
	def toByteSource: ByteSource = new ChunkedBufferByteSource(chunks, byteLength)

	/** Copies the pre-encoded bytes of this batch inline into `writer`.
	 * @param writer the target [[Serializer.Writer]] receiving the inline batch bytes.*/
	def writeInlineTo(writer: Serializer.Writer): Unit = {
		var index = 0
		while index < chunks.length do {
			writer.putBuffer(chunks(index).duplicate())
			index += 1
		}
	}
}

/** Wire-level representation of the record batch carried by `AppendRecords` and `InstallSnapshot`. */
enum RecordBatch {
	/** Unstaged records encoded and decoded directly within the enclosing control package's deduplication scope (wire tag `0`), or the decoded result of an inline isolated-scope batch (wire tag `1`). */
	case Records(records: IArray[Record])
	/** Outbound-only pre-encoded batch placed inline inside the control package with an isolated deduplication scope (wire tag `1`). */
	case EncodedInline(encoded: EncodedRecordBatch)
	/** Pre-encoded batch transmitted out-of-line as a trailing [[ByteSource]] stream with an isolated deduplication scope (wire tag `2`). */
	case EncodedOutOfLine(source: ByteSource)

	/** Returns the underlying `IArray[Record]` when this batch is [[RecordBatch.Records]].
	 * @throws UnsupportedOperationException if this batch has not been decoded into [[RecordBatch.Records]].*/
	def toIArray: IArray[Record] = this match {
		case Records(records) => records
		case other => throw new UnsupportedOperationException(s"RecordBatch is not in decoded Records form: $other")
	}

	/** Returns the underlying records as a `Seq[Record]` when this batch is [[RecordBatch.Records]]. */
	def toSeq: Seq[Record] = toIArray.toSeq

	/** Returns the number of records when this batch is [[RecordBatch.Records]]. */
	def length: Int = toIArray.length

	/** Returns the record at `index` when this batch is [[RecordBatch.Records]]. */
	def apply(index: Int): Record = toIArray(index)
}

/** Helper methods for pre-encoding, placing, and decoding [[RecordBatch]] instances in an isolated reference-deduplication scope. */
object RecordBatchCodec {

	/** Encodes `records` into an [[EncodedRecordBatch]] using a fresh [[ChunkedHeapWriter]] and isolated [[RecyclableIdentityIntMap]], enforcing `allocationLimits`.
	 * @param records the record array to serialize.
	 * @param governingVersion the [[ProtocolVersion]] governing serialization.
	 * @param allocationLimits the allocation bounds enforced on collection length and maximum encoded byte size.
	 * @param batchSerializer the serializer for `IArray[Record]`.
	 * @return the resulting immutable [[EncodedRecordBatch]].
	 * @throws Serializer.SerializationException if `records.length` exceeds `allocationLimits.maxCollectionElements` or the encoded byte count exceeds `allocationLimits.maxByteSourceBytes`.*/
	def encode(
		records: IArray[Record],
		governingVersion: ProtocolVersion,
		allocationLimits: AllocationLimits
	)(using batchSerializer: Serializer[IArray[Record]]): EncodedRecordBatch = {
		if records.length > allocationLimits.maxCollectionElements then {
			throw new Serializer.SerializationException(0, s"Record batch length (${records.length}) exceeds maximum collection elements limit (${allocationLimits.maxCollectionElements})")
		}
		val writer = new ChunkedHeapWriter(
			governingVersion = governingVersion,
			maxEncodedBytes = allocationLimits.maxByteSourceBytes
		)
		batchSerializer.serialize(records, writer)
		val (chunks, totalBytes) = writer.finish()
		new EncodedRecordBatch(chunks, totalBytes, governingVersion)
	}

	/** Selects inline (`EncodedInline`, tag `1`) or out-of-line (`EncodedOutOfLine`, tag `2`) placement for `encoded` based on `maxInlineBytes`.
	 * @param encoded the pre-encoded batch to place.
	 * @param maxInlineBytes the maximum byte size permitted for inline placement on the target connection.
	 * @return [[RecordBatch.EncodedInline]] if `encoded.byteLength <= maxInlineBytes`, or [[RecordBatch.EncodedOutOfLine]] otherwise.*/
	def place(encoded: EncodedRecordBatch, maxInlineBytes: Int): RecordBatch = {
		if encoded.byteLength <= maxInlineBytes.toLong then RecordBatch.EncodedInline(encoded)
		else RecordBatch.EncodedOutOfLine(encoded.toByteSource)
	}

	/** Decodes an out-of-line batch [[ByteSource]] into an `IArray[Record]` using a [[ReadableChannelReader]] with a fresh [[RecyclableObjectBuffer]] and `allocationLimits`.
	 * Does not close `source`; the caller is responsible for closing `source` in a `finally` block.
	 * @param source the sealed [[ByteSource]] containing the serialized `IArray[Record]` bytes.
	 * @param governingVersion the [[ProtocolVersion]] governing deserialization.
	 * @param allocationLimits the security bounds enforced during deserialization.
	 * @param batchDeserializer the deserializer for `IArray[Record]`.
	 * @return the decoded `IArray[Record]`.*/
	def decodeOutOfLine(
		source: ByteSource,
		governingVersion: ProtocolVersion,
		allocationLimits: AllocationLimits
	)(using batchDeserializer: Deserializer[IArray[Record]]): IArray[Record] = {
		val declaredSize = source.size
		if declaredSize <= 0L then {
			throw new Deserializer.NegativeLengthException(0, declaredSize, "RecordBatch.EncodedOutOfLine")
		}
		if declaredSize > allocationLimits.maxByteSourceBytes then {
			throw new Deserializer.AllocationThresholdExceededException(0, declaredSize, allocationLimits.maxByteSourceBytes, "RecordBatch.EncodedOutOfLine")
		}
		val channel = source.openChannel()
		try {
			val reader = new ReadableChannelReader(
				channel = channel,
				byteLength = declaredSize,
				governingVersion = governingVersion,
				allocationLimits = allocationLimits
			)
			val records = batchDeserializer.deserialize(reader)
			reader.requireFullyConsumed()
			records
		} finally {
			try channel.close() catch { case NonFatal(_) => () }
		}
	}
}
