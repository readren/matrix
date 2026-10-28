package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable
import scala.util.{Random, Success}

/** Adversarial channel selection strategy biasing packet delivery to maximize distributed consensus race coverage.
 *
 * Implements a multiplicative hybrid model combining continuous log-uniform node affinity drift across shifted exponential epochs with semantic message-type weighting. Deliberately stresses consensus edge cases—such as retirement catch-up races, Raft §5.4.2 commitment barriers, joint consensus quorum asymmetry, and log compaction lapping—while preserving strict per-channel FIFO delivery.
 * @param env the target [[ConsensusEnvironment]].
 * @param random the pseudo-random generator governing epoch transitions and weighted sampling.
 * @param minEpochSteps minimum duration in simulation steps of an affinity drift epoch.
 * @param meanAdditionalEpochSteps mean parameter for the shifted exponential distribution determining epoch duration.
 * @param maxEpochSteps upper bound ceiling on epoch duration to prevent permanent starvation. */
class AdversarialPacketPicker(
	val env: ConsensusEnvironment,
	val random: Random,
	val minEpochSteps: Int = 20,
	val meanAdditionalEpochSteps: Double = 60.0,
	val maxEpochSteps: Int = 200
) {
	private var epochRemainingSteps: Int = 0
	private val nodeAffinities: mutable.Map[NodeId, Double] = mutable.Map.empty
	for n <- env.allNodes do nodeAffinities(n.myId) = 1.0

	/** Returns the remaining simulation steps in the current affinity drift epoch. */
	def currentEpochRemainingSteps: Int = epochRemainingSteps

	/** Returns an immutable snapshot of current node affinity multipliers. */
	def currentNodeAffinities: Map[NodeId, Double] = nodeAffinities.toMap

	/** Refreshes node-level temporal affinities by independently drawing continuous multipliers for all nodes.
	 *
	 * Intention: Simulates temporally correlated physical host degradation (e.g., GC pauses, thread stalls, or hardware throttling) across random epochs, naturally producing dynamic single or multiple stragglers. */
	def refreshEpochAffinities(): Unit = {
		val u = random.nextDouble()
		val additional = (-meanAdditionalEpochSteps * math.log(1.0 - u)).toInt
		epochRemainingSteps = math.min(maxEpochSteps, minEpochSteps + additional)
		for n <- env.allNodes do {
			// X ~ Uniform(-3.0, 1.0), yielding A_n = 2^X in [0.125, 2.0]
			val x = -3.0 + (random.nextDouble() * 4.0)
			nodeAffinities(n.myId) = math.pow(2.0, x)
		}
	}

	/** Advances the epoch countdown step counter, refreshing node affinities when the active epoch elapses. */
	def advanceStep(): Unit = {
		if epochRemainingSteps <= 0 then refreshEpochAffinities()
		epochRemainingSteps -= 1
	}

	/** Computes the semantic adversarial weight multiplier for a candidate channel head packet.
	 *
	 * Intention: Biases packet selection to prolong critical vulnerability windows (e.g. holding back [[ConsensusRpc.PermitQuiescence]] or catch-up [[ConsensusRpc.AppendRecords]]) while accelerating disruptive events (such as [[ConsensusRpc.RequestBallot]] or pre-vote discovery baiting).
	 * @param packet the head packet pending delivery at the front of the candidate directional channel.
	 * @param to the recipient participant node identifier.
	 * @return the relative adversarial weighting factor. */
	def calculateSemanticWeight(packet: Packet, to: NodeId): Double = {
		packet match {
			case req: RequestPacket =>
				req.rpc match {
					case ConsensusRpc.PermitQuiescence(_) =>
						0.05
					case ConsensusRpc.AppendRecords(_, _, _, _, _, _) =>
						val role = env.nodeRole(to)
						if role == "RETIRING" || role == "JOINING" then 0.10 else 1.0
					case ConsensusRpc.InstallSnapshot(_, _, _, _, _) =>
						2.0
					case ConsensusRpc.RequestBallot(_, _) =>
						3.0
					case ConsensusRpc.HowAreYou(_) =>
						1.0
				}
			case resp: ResponsePacket =>
				import scala.compiletime.asMatchable
				resp.response match {
					case Success(res) =>
						res.asMatchable match {
							case stateInfo: StateInfo if stateInfo.rank != ER_LEADING =>
								2.0
							case _: AppendResult_Accepted =>
								val destRole = env.nodeRole(resp.destination)
								if destRole == "LEADER" then 0.50 else 1.0
							case _ =>
								1.0
						}
					case _ =>
						1.0
				}
		}
	}

	/** Selects the next active channel to advance using weighted probabilistic sampling across candidate channels.
	 *
	 * Intention: Evaluates composite weights factoring both sender/receiver node affinities and head-packet semantic urgency, advancing strictly the head packet of the sampled channel to preserve point-to-point FIFO delivery.
	 * @param candidates the collection of currently active non-empty directional channel keys.
	 * @return the sampled directional channel key (from, to). */
	def selectChannel(candidates: Seq[(NodeId, NodeId)]): (NodeId, NodeId) = {
		if candidates.size == 1 then candidates.head
		else {
			val weights = new Array[Double](candidates.size)
			var totalWeight = 0.0
			var i = 0
			while i < candidates.size do {
				val (from, to) = candidates(i)
				val headPacketOpt = env.network.channelHeadPacket(from, to)
				val semanticWeight = headPacketOpt.fold(1.0)(p => calculateSemanticWeight(p, to))
				val affFrom = nodeAffinities.getOrElse(from, 1.0)
				val affTo = nodeAffinities.getOrElse(to, 1.0)
				val compositeWeight = math.max(1.0, 100.0 * semanticWeight * affFrom * affTo)
				weights(i) = compositeWeight
				totalWeight += compositeWeight
				i += 1
			}
			val threshold = random.nextDouble() * totalWeight
			var cumulative = 0.0
			var selectedIndex = 0
			var found = false
			var j = 0
			while j < candidates.size && !found do {
				cumulative += weights(j)
				if cumulative >= threshold then {
					selectedIndex = j
					found = true
				}
				j += 1
			}
			candidates(selectedIndex)
		}
	}
}
