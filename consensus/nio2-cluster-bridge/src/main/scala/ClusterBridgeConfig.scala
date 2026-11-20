package readren.consensus
package nio2

import readren.common.ByteSink
import readren.consensus.protocol.Record
import readren.nexus.cluster.channel.{PipelinedTransmitter, Receiver}
import readren.nexus.cluster.misc.{DualEndedCircularStorage, RingLength}
import readren.nexus.cluster.serialization.{AllocationLimits, ProtocolVersion}
import readren.sequencer.MilliDuration

/** Strategy that determines on which thread an outbound record batch is encoded; byte placement (inline vs out-of-line) is determined after encoding from the exact encoded byte size and the connection's negotiated control-package capacity. */
enum RecordBatchEncodingStrategy {
	/** Encoded directly into the control package by the transmitter's serialization owner, sharing the message's deduplication scope; fails fast if the control package capacity is exceeded. */
	case WithinControlPackage
	/** Pre-encoded on the participant's sequencer into an isolated deduplication scope, then placed inline or out-of-line by encoded byte size. */
	case StagedOnSequencer
	/** Pre-encoded on the bridge's codec executor into an isolated deduplication scope, then placed inline or out-of-line by encoded byte size. */
	case StagedOnCodecExecutor
}

/** Operational configuration for the NIO2 cluster transport bridge.
 * @param sizeOfBuffersInRing byte capacity of each direct buffer allocated by each [[DualEndedCircularStorage]] used for both transmission and reception. The [[PipelinedTransmitter]] uses two [[DualEndedCircularStorage]] and the [[Receiver]] uses one. [[DualEndedCircularStorage]] instances are used only for the control package (the inline part of the wired message).
 * @param maxRingLengthForTransmission maximum number of direct buffers allowed in each of the two [[DualEndedCircularStorage]] of the [[PipelinedTransmitter]].
 * @param maxRingLengthForReception maximum number of direct buffers allowed in the [[DualEndedCircularStorage]] of the [[Receiver]].
 * @param minRingLengthOnIdle minimum number of direct buffers preserved in the [[DualEndedCircularStorage]] instances when the channel is idle. Applies for both, transmission and reception.
 * @param streamTransferBufferCapacity byte capacity of the direct buffer used for the trailing stream (out-of-line part of the message). Applies to both, transmission and reception.
 * @param maxGatheringBuffers maximum number of [[java.nio.ByteBuffer]] entries submitted per gathering socket write call in [[PipelinedTransmitter]].
 * @param allocationLimits limits enforced on dynamic memory allocations during serialization.
 * @param supportedVersions set of protocol versions supported by this participant, exchanged during handshakes to negotiate the highest mutually supported version.
 * @param connectTimeout maximum duration to await asynchronous peer TCP channel establishment.
 * @param fastRpcTimeout maximum duration to await responses for low-latency control and replication RPCs \
 * (`HowAreYou`, `RequestBallot`, `AppendRecords`, `PermitQuiescence`).
 * @param snapshotRpcTimeout maximum duration to await completion of streaming state machine snapshot installations \
 * (`InstallSnapshot`).
 * @param clientRpcTimeout maximum duration to await completion of client command and electorate mutations.
 * @param receiverTimeout maximum duration to await inbound network frames before timing out.
 * @param quiescenceAuthorizationRetryDelay backoff delay before retrying quiescence authorization upon transport rejection.
 * @param unreachableFollowersRetryDelay backoff delay before retrying replication to unreachable cluster peers.
 * @param retirementDriveRetryDelay backoff delay between subsequent retirement drive attempts.
 * @param replicationLoopRetryDelay delay before retrying failed replication loops on active leaders.
 * @param recordBatchEncodingSelector function selecting the [[RecordBatchEncodingStrategy]] for outbound [[ClusterWireProtocol.AppendRecords]] and [[ClusterWireProtocol.InstallSnapshot]] batches; empty batches are always normalized to [[RecordBatchEncodingStrategy.WithinControlPackage]].
 * @param stagedBatchInlineThreshold threshold determining whether the batch field of a staged [[ClusterWireProtocol.AppendRecords]] or [[ClusterWireProtocol.InstallSnapshot]] message is wired inline (within the control package) or out-of-line (in the trailing stream) when encoded using [[RecordBatchEncodingStrategy.StagedOnSequencer]] or [[RecordBatchEncodingStrategy.StagedOnCodecExecutor]] (batches using [[RecordBatchEncodingStrategy.WithinControlPackage]] are always wired inline). Measured in circular buffers needed in the transmitter ring to hold the entire control package (the whole message except any streamed snapshot byte source of `InstallSnapshot`). A staged batch is wired inline if that control package fits within this many transmitter ring buffers and also within both the local transmitter ring and the remote receiver ring. Values exceeding [[maxRingLengthForTransmission]] are clamped to [[maxRingLengthForTransmission]]. Default is twice the default [[minRingLengthOnIdle]].
 * @param batchCodecParallelism number of worker threads in the bridge's off-sequencer record-batch codec executor.
 * @param inboundStagingAllocator [[ByteSink.Allocator]] provisioned to [[Receiver]] instances for staging inbound out-of-line byte streams.*/
final case class ClusterBridgeConfig(
	sizeOfBuffersInRing: Int = 8192,
	maxRingLengthForTransmission: RingLength = PipelinedTransmitter.DEFAULT_RING_MAX_LENGTH,
	maxRingLengthForReception: RingLength = Receiver.DEFAULT_RING_MAX_LENGTH,
	minRingLengthOnIdle: RingLength = ClusterBridgeConfig.DefaultRingMinLengthOnIdle,
	streamTransferBufferCapacity: Int = PipelinedTransmitter.DEFAULT_STREAM_TRANSFER_BUFFER_CAPACITY,
	maxGatheringBuffers: Int = PipelinedTransmitter.DEFAULT_MAX_GATHERING_BUFFERS,
	allocationLimits: AllocationLimits = AllocationLimits.Default,
	supportedVersions: Set[ProtocolVersion] = Set(ProtocolVersion.OF_THIS_PROJECT),
	connectTimeout: MilliDuration = 5000L,
	fastRpcTimeout: MilliDuration = 2000L,
	snapshotRpcTimeout: MilliDuration = 30000L,
	clientRpcTimeout: MilliDuration = 5000L,
	receiverTimeout: MilliDuration = 60000L,
	quiescenceAuthorizationRetryDelay: MilliDuration = 200L,
	unreachableFollowersRetryDelay: MilliDuration = 500L,
	retirementDriveRetryDelay: MilliDuration = 500L,
	replicationLoopRetryDelay: MilliDuration = 100L,
	recordBatchEncodingSelector: IArray[Record] => RecordBatchEncodingStrategy = ClusterBridgeConfig.DefaultRecordBatchEncodingSelector,
	stagedBatchInlineThreshold: RingLength = ClusterBridgeConfig.DefaultStagedBatchInlineThreshold,
	batchCodecParallelism: Int = ClusterBridgeConfig.DefaultBatchCodecParallelism,
	inboundStagingAllocator: ByteSink.Allocator = ClusterBridgeConfig.DefaultInboundStagingAllocator
) {
	require(sizeOfBuffersInRing > Receiver.FRAME_HEADER_MAX_SIZE, s"sizeOfBuffersInRing ($sizeOfBuffersInRing) must exceed FRAME_HEADER_MAX_SIZE (${Receiver.FRAME_HEADER_MAX_SIZE})")
	require(maxRingLengthForTransmission.toLong * sizeOfBuffersInRing.toLong <= Int.MaxValue, s"Aggregate transmitter circular buffer capacity (${maxRingLengthForTransmission.toLong * sizeOfBuffersInRing.toLong} bytes) exceeds Int.MaxValue")
	require(maxRingLengthForReception.toLong * sizeOfBuffersInRing.toLong <= Int.MaxValue, s"Aggregate receiver circular buffer capacity (${maxRingLengthForReception.toLong * sizeOfBuffersInRing.toLong} bytes) exceeds Int.MaxValue")
	require(streamTransferBufferCapacity > 0, s"streamTransferBufferCapacity must be positive, got $streamTransferBufferCapacity")
	require(maxGatheringBuffers >= 2, s"maxGatheringBuffers must be at least 2, got $maxGatheringBuffers")
	require(batchCodecParallelism >= 1, s"batchCodecParallelism must be at least 1, got $batchCodecParallelism")

	/** Maximum control package payload bytes that the local [[PipelinedTransmitter]] can serialize into its circular buffer ring. */
	val localTransmitterControlCapacity: Int = maxRingLengthForTransmission * sizeOfBuffersInRing

	/** Maximum single-frame control package payload bytes guaranteed to fit in the local [[Receiver]]'s circular buffer ring, delegated to [[Receiver.maxSingleFramePackagePayloadBytes]]. */
	val localReceiverControlCapacity: Int = Receiver.maxSingleFramePackagePayloadBytes(maxRingLengthForReception, sizeOfBuffersInRing)

	/** Effective number of transmitter circular buffers permitted for a single inline control package (`min(stagedBatchInlineThreshold, maxRingLengthForTransmission)`). */
	val effectiveStagedBatchInlineThreshold: RingLength = RingLength.min(stagedBatchInlineThreshold, maxRingLengthForTransmission)

	/** Local transmitter control-package byte ceiling imposed by [[effectiveStagedBatchInlineThreshold]]. */
	val localInlineTransmitterControlCapacity: Int = effectiveStagedBatchInlineThreshold * sizeOfBuffersInRing

	/** Computes the maximum byte size of a pre-encoded batch that may be inlined into a control package sent to a peer advertising `remoteMaxInboundControlPackageBytes`, subtracting `envelopeBytes` for the enclosing message's non-batch fields and framing slack.
	 * @param remoteMaxInboundControlPackageBytes the receiver control package byte capacity advertised by the remote peer during handshake.
	 * @param envelopeBytes the serialized byte footprint (plus buffer-boundary alignment slack) of all non-batch fields in the enclosing control package.
	 * @return the maximum byte length of an [[EncodedRecordBatch]] that can be placed inline on this connection.*/
	def maxInlineBatchBytesForPeer(remoteMaxInboundControlPackageBytes: Int, envelopeBytes: Int): Int = {
		val negotiatedControlCapacity = math.min(localInlineTransmitterControlCapacity, math.max(0, remoteMaxInboundControlPackageBytes))
		math.max(0L, negotiatedControlCapacity.toLong - math.max(0, envelopeBytes).toLong).toInt
	}
}

object ClusterBridgeConfig {
	/** Default minimum number of direct buffers preserved per circular storage ring when idle. */
	val DefaultRingMinLengthOnIdle: RingLength = RingLength(2)

	/** Default transmitter-ring buffer-count threshold for staging pre-encoded batches inline in control packages (`DefaultRingMinLengthOnIdle * 2`). */
	val DefaultStagedBatchInlineThreshold: RingLength = RingLength(DefaultRingMinLengthOnIdle * 2)

	/** Default selector: [[RecordBatchEncodingStrategy.WithinControlPackage]] for empty batches and [[RecordBatchEncodingStrategy.StagedOnCodecExecutor]] for all non-empty batches. */
	val DefaultRecordBatchEncodingSelector: IArray[Record] => RecordBatchEncodingStrategy =
		batch => if batch.isEmpty then RecordBatchEncodingStrategy.WithinControlPackage else RecordBatchEncodingStrategy.StagedOnCodecExecutor

	/** Default number of worker threads in the off-sequencer batch codec executor. */
	val DefaultBatchCodecParallelism: Int = math.max(2, Runtime.getRuntime.availableProcessors() / 2)

	/** Default inbound staging allocator keeping streams up to 4 MiB in heap memory before spilling to temporary disk files. */
	val DefaultInboundStagingAllocator: ByteSink.Allocator =
		new ByteSink.DefaultAllocator(inMemoryThreshold = 4 * 1024 * 1024)

	val Default: ClusterBridgeConfig = ClusterBridgeConfig()
}
