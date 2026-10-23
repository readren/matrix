package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable

/** A pending wake-up timer registered via [[ConsensusParticipantSdm.ClusterParticipant.requestWakeUp]].
 * @param tokenId unique identifier assigned to this wake-up timer by [[VirtualClock]].
 * @param token cancellation handle returned to the registering participant.
 * @param nodeId identifier of the node that requested the wake-up.
 * @param reason the protocol retry reason governing the timer delay.
 * @param wakeupsDone number of prior consecutive wake-ups for this retry cycle, used for linear backoff calculation.
 * @param scheduledTime the target [[VirtualTime]] at which this wake-up expires.
 * @param callback callback enqueued onto the node's [[StepDoer]] when the timer fires. */
final case class PendingWakeUp(
	tokenId: WakeUpTokenId,
	token: WakeUpToken,
	nodeId: NodeId,
	reason: WakeUpReason,
	wakeupsDone: Int,
	scheduledTime: VirtualTime,
	callback: () => Unit
)

/** Virtual time progression and wake-up timer registry subsystem (Tier 1) for [[ConsensusEnvironment]].
 *
 * Replaces wall-clock timers with deterministic integer tick progression. Timers registered by consensus participants are held until virtual time advances to or past their `scheduledTime`, or until explicitly triggered via [[triggerWakeUp]].
 * @param ticksPerMilli conversion factor from simulated milliseconds to discrete virtual ticks.
 * @param env the owning [[ConsensusEnvironment]]. */
class VirtualClock(val ticksPerMilli: Int, env: ConsensusEnvironment) {
	private var _virtualTime: VirtualTime = 0
	private var wakeUpTokenSequencer: WakeUpTokenId = 0
	private val pendingWakeUpsMap: mutable.Map[WakeUpTokenId, PendingWakeUp] = mutable.Map.empty

	/** The current virtual time in integer ticks. */
	inline def currentVirtualTime: VirtualTime = _virtualTime

	/** Resets virtual time to zero and clears all pending wake-up timers. */
	private[env] def reset(): Unit = {
		_virtualTime = 0
		wakeUpTokenSequencer = 0
		pendingWakeUpsMap.clear()
	}

	/** Registers a new wake-up timer for `node` with linear backoff based on `wakeupsDone`.
	 * @param node the node requesting the timer.
	 * @param reason the retry category of the wake-up.
	 * @param wakeupsDone the count of prior wake-up attempts in the current retry sequence.
	 * @param callback the action to enqueue on `node.stepDoer` when the timer expires.
	 * @return a [[WakeUpToken]] that removes the timer from the registry if cancelled before expiration. */
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

	/** Returns all active wake-up timers across the cluster, ordered by `scheduledTime`. */
	def pendingWakeUps: Seq[PendingWakeUp] = pendingWakeUpsMap.values.toSeq.sortBy(_.scheduledTime)

	/** Returns all active wake-up timers registered by `ref`, ordered by `scheduledTime`.
	 * @param ref the node whose pending timers should be queried. */
	def pendingWakeUpsFor(ref: NodeRef): Seq[PendingWakeUp] = {
		val id = ref.asNodeId
		pendingWakeUpsMap.values.filter(_.nodeId == id).toSeq.sortBy(_.scheduledTime)
	}

	/** Advances [[currentVirtualTime]] by `ticks` and enqueues the callbacks of all timers whose `scheduledTime <= currentVirtualTime` onto their respective nodes' [[StepDoer]] queues.
	 * @param ticks number of virtual ticks to advance.
	 * @return the sequence of [[PendingWakeUp]] timers that expired and were triggered during this advancement. */
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

	/** Advances [[currentVirtualTime]] by `ms * ticksPerMilli` ticks and triggers all expired wake-up timers.
	 * @param ms number of simulated milliseconds to advance.
	 * @return the sequence of [[PendingWakeUp]] timers that expired and were triggered. */
	def advanceTimeMillis(ms: Int): Seq[PendingWakeUp] = advanceTime(ms * ticksPerMilli)

	/** Immediately fires a specific pending wake-up timer regardless of [[currentVirtualTime]] and removes it from the registry.
	 * @param tokenId the identifier of the timer to fire.
	 * @return true if the timer was found and triggered; false if it was already expired or cancelled. */
	def triggerWakeUp(tokenId: WakeUpTokenId): Boolean = env.journal.recordOrExecute(EnvOperation.TriggerWakeUp(tokenId)) {
		pendingWakeUpsMap.remove(tokenId).fold(false) { w =>
			val n = env.node(w.nodeId)
			n.stepDoer.executeSequentially(() => w.callback())
			true
		}
	}
}
