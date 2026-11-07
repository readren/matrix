package readren.consensus
package nio2

import readren.nexus.cluster.misc.DualEndedCircularStorage
import readren.nexus.cluster.serialization.{AllocationLimits, ProtocolVersion}
import readren.sequencer.MilliDuration

/** Operational configuration for the NIO2 cluster transport bridge.
 * @param frameCapacity byte capacity of each direct buffer allocated by [[DualEndedCircularStorage]].
 * @param maxBuffers maximum number of circular direct buffers retained per socket channel.
 * @param minBuffersOnIdle minimum number of direct buffers preserved when the channel is idle.
 * @param allocationLimits limits enforced on dynamic memory allocations during serialization.
 * @param supportedVersions set of protocol versions supported by this participant, exchanged during handshakes to negotiate the highest mutually supported version.
 * @param connectTimeout maximum duration to await asynchronous peer TCP channel establishment.
 * @param fastRpcTimeout maximum duration to await responses for low-latency control and replication RPCs \
 * ([[HowAreYou]], [[RequestBallot]], [[AppendRecords]], [[PermitQuiescence]]).
 * @param snapshotRpcTimeout maximum duration to await completion of streaming state machine snapshot installations \
 * ([[InstallSnapshot]]).
 * @param clientRpcTimeout maximum duration to await completion of client command and electorate mutations.
 * @param receiverTimeout maximum duration to await inbound network frames before timing out.
 * @param quiescenceAuthorizationRetryDelay backoff delay before retrying quiescence authorization upon transport rejection.
 * @param unreachableFollowersRetryDelay backoff delay before retrying replication to unreachable cluster peers.
 * @param retirementDriveRetryDelay backoff delay between subsequent retirement drive attempts.
 * @param replicationLoopRetryDelay delay before retrying failed replication loops on active leaders.*/
final case class ClusterBridgeConfig(
	frameCapacity: Int = 8192,
	maxBuffers: Int = DualEndedCircularStorage.DEFAULT_MAX_BUFFERS,
	minBuffersOnIdle: Int = 2,
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
	replicationLoopRetryDelay: MilliDuration = 100L
)

object ClusterBridgeConfig {
	val Default: ClusterBridgeConfig = ClusterBridgeConfig()
}
