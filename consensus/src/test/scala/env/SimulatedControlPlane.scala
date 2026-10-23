package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.immutable.ListSet
import scala.collection.mutable

/** Simulated administrative control plane (Tier 2) coordinating cluster provisioning,
 * dynamic node additions, and probable participant discovery.
 *
 * @param env the owning [[ConsensusEnvironment]]. */
class SimulatedControlPlane(env: ConsensusEnvironment) {
	/** Set of node identifiers currently known to the administrative control plane as provisioned and operational consensus participants. */
	private val _knownProbableParticipants: mutable.Set[NodeId] = mutable.Set.from(env.defaultInitialParticipants)

	/** Set of participants currently presumed by the administrative control plane to constitute the active cluster electorate. */
	private var _presumedActiveParticipants: Set[NodeId] = env.defaultInitialParticipants.toSet

	/** In-flight target configuration currently being proposed, if any. */
	private var _inFlightTarget: Option[Set[NodeId]] = None

	/** Returns an immutable snapshot of all nodes currently known to the control plane. */
	def knownProbableParticipants: Set[NodeId] = _knownProbableParticipants.toSet

	/** Returns the set of participants currently presumed active by the control plane. */
	def presumedActiveParticipants: Set[NodeId] = _presumedActiveParticipants

	/** Returns the in-flight target configuration if a reconfiguration is in progress. */
	def inFlightTarget: Option[Set[NodeId]] = _inFlightTarget

	/** Returns the declarative intended membership: [[inFlightTarget]] during in-flight reconfigurations, or [[presumedActiveParticipants]] when idle. */
	def intendedTargetParticipants: Set[NodeId] = _inFlightTarget.getOrElse(_presumedActiveParticipants)

	/** Resets the control-plane state to the environment's default initial participants. */
	private[env] def reset(): Unit = {
		_knownProbableParticipants.clear()
		_knownProbableParticipants.addAll(env.defaultInitialParticipants)
		_presumedActiveParticipants = env.defaultInitialParticipants.toSet
		_inFlightTarget = None
	}

	/** Records the submission of an administrative reconfiguration proposal targeting `desiredParticipants`. */
	def onReconfigurationProposed(desiredParticipants: Set[NodeId]): Unit = {
		_inFlightTarget = Some(desiredParticipants)
	}

	/** Records the terminal outcome of an administrative reconfiguration proposal. */
	def onReconfigurationTerminal(desiredParticipants: Set[NodeId], isSuccessful: Boolean): Unit = {
		if isSuccessful then _presumedActiveParticipants = desiredParticipants
		_inFlightTarget = None
	}

	/** Records the authoritative exclusion of a participant from the presumed active electorate. */
	def onParticipantExcluded(participantId: NodeId): Unit = {
		_presumedActiveParticipants -= participantId
	}

	/** Administratively informs `targetNode` of the updated set of probable consensus participants.
	 *
	 * Directly enqueues a sequential task on `targetNode`'s [[StepDoer]] to update its
	 * [[EnvironmentNode.TestClusterParticipant.memorizedOtherProbableParticipants]].
	 *
	 * @param targetNode the node whose cluster bridge is being updated.
	 * @param probableParticipants the updated set of probable consensus participant references. */
	def updateProbableParticipants(
		targetNode: NodeRef,
		probableParticipants: Set[? <: NodeRef]
	): Unit = {
		val targetId = targetNode.asNodeId
		val probableIds: Set[NodeId] = probableParticipants.map(_.asNodeId)
		env.journal.recordOrExecute(EnvOperation.UpdateProbableParticipants(targetId, probableIds)) {
			val n = env.node(targetId)
			n.stepDoer.executeSequentially(() => {
				n.clusterParticipant.updateProbableParticipants(probableIds)
			})
		}
	}

	/** Broadcasts the updated probable participant set across `targetNodes` (or all known probable nodes if [[None]]).
	 *
	 * @param probableParticipants the updated set of probable consensus participant references.
	 * @param targetNodes the subset of nodes to update, or [[None]] to update all known probable nodes. */
	def broadcastProbableParticipantsUpdate(
		probableParticipants: Set[? <: NodeRef],
		targetNodes: Option[Iterable[? <: NodeRef]] = None
	): Unit = {
		val targets = targetNodes match {
			case Some(nodes) => nodes.map(_.asNodeId)
			case None => _knownProbableParticipants.toSet
		}
		for t <- targets do updateProbableParticipants(t, probableParticipants)
	}

	/** Called when a node finishes retirement and releases its service, informing the control plane of its quiescence.
	 *
	 * If the quiesced node is included in the declarative [[intendedTargetParticipants]], the control plane immediately
	 * provisions and starts a fresh instance to avoid orphaning desired cluster membership.
	 * Otherwise, the control plane deregisters the quiesced node from [[knownProbableParticipants]] and [[presumedActiveParticipants]]
	 * and broadcasts the updated probable participant view to surviving nodes. */
	def onNodeQuiesced(quiescedNode: NodeId): Unit = {
		val isIntendedActive = _inFlightTarget.exists(_.contains(quiescedNode))
		if isIntendedActive then {
			startDynamicNode(quiescedNode)
		} else {
			_knownProbableParticipants.remove(quiescedNode)
			_presumedActiveParticipants -= quiescedNode
			broadcastProbableParticipantsUpdate(_knownProbableParticipants.toSet)
		}
	}

	/** Provisions and starts an offline node, informing existing nodes of the new participant.
	 *
	 * @param newNode the node to start and provision dynamically. */
	def startDynamicNode(newNode: NodeRef): Unit = {
		val newId = newNode.asNodeId
		_knownProbableParticipants.add(newId)
		env.startNode(newId)
		updateProbableParticipants(newId, _knownProbableParticipants.toSet)
		val otherNodes = _knownProbableParticipants.toSet - newId
		broadcastProbableParticipantsUpdate(_knownProbableParticipants.toSet, Some(otherNodes))
	}
}
