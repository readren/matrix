package readren.consensus
package nio2

import munit.FunSuite
import readren.consensus.protocol.*
import readren.nexus.cluster.serialization.CommonSerializers
import readren.nexus.cluster.serialization.CommonSerializers.given
import readren.sequencer.SchedulingDoer
import readren.sequencer.providers.CooperativeThreadDrivenSchedulerDp

import java.net.{InetSocketAddress, SocketAddress}
import java.util.Comparator
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.reflect.ClassTag

class WakeUpSchedulingTest extends FunSuite {

	val schedulerDp = new CooperativeThreadDrivenSchedulerDp.Impl(unhandledExceptionReporter = (_, e) => e.printStackTrace())
	private val strSer: readren.nexus.cluster.serialization.Serializer[String] = summon
	private val strDeser: readren.nexus.cluster.serialization.Deserializer[String] = summon

	object SdmInstance extends Nio2ClusterBridgeCdm {
		override type ParticipantId = String
		override val participantIdComparator: Comparator[ParticipantId] = Ordering.String
		override type ClientId = String
		override type ClientCommand = String
		override type StateMachineResponse = String

		override val sequencer: SchedulingDoer = schedulerDp.provide("test-sequencer")

		override given participantIdSerializer: readren.nexus.cluster.serialization.Serializer[ParticipantId] = strSer
		override given participantIdDeserializer: readren.nexus.cluster.serialization.Deserializer[ParticipantId] = strDeser
		override given clientCommandSerializer: readren.nexus.cluster.serialization.Serializer[ClientCommand] = strSer
		override given clientCommandDeserializer: readren.nexus.cluster.serialization.Deserializer[ClientCommand] = strDeser
		override given stateMachineResponseSerializer: readren.nexus.cluster.serialization.Serializer[StateMachineResponse] = strSer
		override given stateMachineResponseDeserializer: readren.nexus.cluster.serialization.Deserializer[StateMachineResponse] = strDeser

		override def contactAddressOf(id: ParticipantId): SocketAddress = new InetSocketAddress("127.0.0.1", 0)
		override def localBindAddress: SocketAddress = new InetSocketAddress("127.0.0.1", 0)

		override val bridgeConfig: ClusterBridgeConfig = ClusterBridgeConfig(
			replicationLoopRetryDelay = 50L,
			unreachableFollowersRetryDelay = 200L
		)
	}

	test("requestWakeUp triggers callback within sequencer after configured delay") {
		val bridge = new SdmInstance.Nio2ClusterBridge("node-1")
		val latch = new CountDownLatch(1)
		var executedInSequence = false

		SdmInstance.sequencer.run {
			bridge.requestWakeUp(WakeUpReason.ReplicationLoopRetry, 0, () => {
				executedInSequence = SdmInstance.sequencer.isInSequence
				latch.countDown()
			})
		}

		val triggered = latch.await(2, TimeUnit.SECONDS)
		assert(triggered, "Wake-up callback was not triggered within timeout")
		assert(executedInSequence, "Wake-up callback was not executed within sequencer thread")
	}

	test("requestWakeUp token cancels pending callback") {
		val bridge = new SdmInstance.Nio2ClusterBridge("node-1")
		val latch = new CountDownLatch(1)
		var wasExecuted = false

		SdmInstance.sequencer.run {
			val token = bridge.requestWakeUp(WakeUpReason.UnreachableFollowersRetry, 0, () => {
				wasExecuted = true
				latch.countDown()
			})
			token.cancel()
		}

		val triggered = latch.await(500, TimeUnit.MILLISECONDS)
		assert(!triggered, "Cancelled wake-up callback should not have been triggered")
		assert(!wasExecuted, "wasExecuted should remain false after token cancellation")
	}
}
