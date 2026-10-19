package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable

/** A pending wake-up registered via [[ConsensusParticipantSdm.ClusterParticipant.requestWakeUp]]. */
final case class PendingWakeUp(
	tokenId: WakeUpTokenId,
	token: WakeUpToken,
	nodeId: NodeId,
	reason: WakeUpReason,
	wakeupsDone: Int,
	scheduledTime: VirtualTime,
	callback: () => Unit
)

/** Virtual time progression and wake-up timer registry for [[ConsensusEnvironment]]. */
class VirtualClock(val ticksPerMilli: Int, env: ConsensusEnvironment) {
	private var _virtualTime: VirtualTime = 0
	private var wakeUpTokenSequencer: WakeUpTokenId = 0
	private val pendingWakeUpsMap: mutable.Map[WakeUpTokenId, PendingWakeUp] = mutable.Map.empty

	inline def currentVirtualTime: VirtualTime = _virtualTime

	private[env] def reset(): Unit = {
		_virtualTime = 0
		wakeUpTokenSequencer = 0
		pendingWakeUpsMap.clear()
	}

	def registerWakeUp(node: EnvironmentNode, reason: WakeUpReason, wakeupsDone: Int, callback: () => Unit): WakeUpToken = {
		wakeUpTokenSequencer += 1
		val tokenId = wakeUpTokenSequencer
		val delayTicks = reason match {
			case WakeUpReason.RetirementDriveRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.UnreachableFollowersRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.QuiescenceAuthorizationRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
			case WakeUpReason.ReplicationLoopRetry => 10 * (wakeupsDone + 1) * ticksPerMilli
		}
		val scheduled = _virtualTime + delayTicks
		val token = new WakeUpToken {
			override def cancel(): Unit = pendingWakeUpsMap.remove(tokenId)
		}
		pendingWakeUpsMap(tokenId) = PendingWakeUp(tokenId, token, node.myId, reason, wakeupsDone, scheduled, callback)
		token
	}

	def pendingWakeUps: Seq[PendingWakeUp] = pendingWakeUpsMap.values.toSeq.sortBy(_.scheduledTime)

	def pendingWakeUpsFor(ref: NodeRef): Seq[PendingWakeUp] = {
		val id = ref.asNodeId
		pendingWakeUpsMap.values.filter(_.nodeId == id).toSeq.sortBy(_.scheduledTime)
	}

	def advanceTime(ticks: Int): Seq[PendingWakeUp] = env.journal.recordOrExecute(EnvOperation.AdvanceTime(ticks)) {
		_virtualTime += ticks
		val expired = pendingWakeUpsMap.values.filter(_.scheduledTime <= _virtualTime).toSeq.sortBy(_.scheduledTime)
		for w <- expired do {
			pendingWakeUpsMap.remove(w.tokenId)
			val n = env.node(w.nodeId)
			n.stepDoer.executeSequentially(() => w.callback())
		}
		expired
	}

	def advanceTimeMillis(ms: Int): Seq[PendingWakeUp] = advanceTime(ms * ticksPerMilli)

	def triggerWakeUp(tokenId: WakeUpTokenId): Boolean = env.journal.recordOrExecute(EnvOperation.TriggerWakeUp(tokenId)) {
		pendingWakeUpsMap.remove(tokenId).fold(false) { w =>
			val n = env.node(w.nodeId)
			n.stepDoer.executeSequentially(() => w.callback())
			true
		}
	}
}
