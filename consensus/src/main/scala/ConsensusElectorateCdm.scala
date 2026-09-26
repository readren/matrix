package readren.consensus

import readren.common.*
import readren.common.Trace.Context
import readren.consensus.protocol.*
import readren.sequencer.Doer

import java.util
import java.util.Comparator
import scala.collection.immutable.{ArraySeq, ListSet}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
 * Component Definition Module (CDM) for consensus electorate management,
 * quorum evaluation, and leader election candidate ranking.
 *
 * This module defines the [[Electorate]] strategy hierarchy representing active
 * cluster membership authorities (both stable and joint consensus).
 */
trait ConsensusElectorateCdm { thisModule =>

	/** The type of participant ids. */
	type ParticipantId <: AnyRef: {Ordering, ClassTag}

	/** The execution sequencer that drives deterministic state transitions. */
	val sequencer: Doer

	val participantIdComparator: Comparator[ParticipantId]

	/** View of a peer's replication progress needed for quorum evaluation. */
	trait PeerProgressView {
		def highestRecordIndexKnownToBeAppended: RecordIndex
		def respondedAsRetiring: Boolean
	}

	/** View of the persistent log needed to determine the highest committable record index. */
	trait LogTermLookup {
		def firstEmptyRecordIndex: RecordIndex
		def currentTerm: Term
		def getRecordTermAt(index: RecordIndex): Term
		def latestSnapshotLastIncludedRecordIndex: RecordIndex
	}

	trait ClusterView {
		val boundParticipantId: ParticipantId
		def getOtherProbableParticipants: ListSet[ParticipantId]
	}

	//// ELECTORATE STRATEGY ////

	/** Identifies which participants are involved in consensus and defines election and replication rules governing participant behavior.
	 *
	 * It has exactly two concrete subclasses:
	 *  - [[JointElectorate]]: Active during joint consensus (`Cold` ∪ `Cnew`). Joint electorate behavior begins immediately once the transitional entry is appended to the participant's log. Replication and election quorums require majorities across both `Cold` and `Cnew`. Joint consensus remains active until the corresponding [[SoleElectorateChange]] entry is replicated to a majority of both `Cold` and `Cnew` (`commitIndex >= indexOfCorrespondingSoleElectorateChange`).
	 *  - [[SoleElectorate]]: Active during non-joint consensus (`Cnew`). Sole electorate behavior begins only once the backing [[SoleElectorateChange]] entry is committed (`commitIndex >= indexOfBackingSoleElectorateChange`). Replication and election quorums require the majority of `Cnew` only. Non-leader `Cold`-only participants having committed the [[SoleElectorateChange]] that excluded them transition to retiring status and await authorization to quiesce from a stable leader of a succeeding term. A leader `Cold`-only participant having committed the [[SoleElectorateChange]] that excluded it remains active as a ghost leader until either it observes that all other participants have committed said [[SoleElectorateChange]], or it receives an append records or authorization to quiesce RPC from a leader of a higher term.
	 *
	 * === Commit Index Dependency ===
	 *  - [[JointElectorate]] behavior starts on append of its [[backingElectorateChange]], but ends when the stable entry is committed.
	 *  - [[SoleElectorate]] behavior starts only when its [[backingElectorateChange]] is committed.
	 *
	 * Although each of these two electorate strategies depends only on log state, the transition between them is commit-sensitive because the transition instant depends on `commitIndex`. */
	sealed trait Electorate {
		val boundParticipantId: ParticipantId
		/** The [[Term]] of the backing [[ElectorateChange]]. */
		val term: Term
		/** The index of the backing [[ElectorateChange]]. */
		val changeIndex: RecordIndex
		/** The participants that conform the electorate for consensus and elections. Contains the same elements as the [[members]] array. */
		val activeParticipants: Set[ParticipantId]
		/** The current set of participants involved in the consensus, sorted. Contains the same elements as the [[activeParticipants]] set. */
		val members: IArray[ParticipantId]
		/** The current set of participants involved in the consensus, excluding the bound participant, sorted. */
		val peers: IArray[ParticipantId]
		/** True when the bound participant is part of the [[members]]. */
		val isBoundIncluded: Boolean

		/** The [[ElectorateChange]] that caused this [[Electorate]] and on which it is based.
		 * An [[ElectorateChange]] is a complete snapshot containing all membership information needed. */
		val backingElectorateChange: ElectorateChange[ParticipantId]

		/** The identifiers of the stable participants. Equivalent to [[backingElectorateChange.newParticipants]]. */
		val stableParticipants: Set[ParticipantId]

		/** The set of participants to include in [[Unable]] responses. */
		def otherProbableParticipants: ListSet[ParticipantId]

		def reachedAMajority(vote: Vote[ParticipantId]): Boolean

		def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): Boolean

		def indexOfTheCommittableRecordWithHighestIndex(log: LogTermLookup, from: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): RecordIndex

		/** Accumulates peer votes until a definitive quorum decision (Won, Lost, or Stale) is reached, unsubscribing all remaining in-flight inquires upon early exit. */
		def accumulateVotingQuorum(
			myVote: Vote[ParticipantId],
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[Vote[ParticipantId]]]
		): sequencer.Capture[VotingQuorumResult[ParticipantId]]

		/** Accumulates peer StateInfos until a discovery quorum decision (MajorityReached, MajorityImpossible, Stale, or ActiveLeaderDetected) is reached, unsubscribing all remaining in-flight inquires upon early exit. */
		def accumulateDiscoveryQuorum(
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[StateInfo]]
		): sequencer.Capture[DiscoveryQuorumResult[ParticipantId]]

		/** Determines the best leader candidate based on the [[StateInfo]] of all participants, including itself.
		 * This method only queries and does not mutate local state.
		 *
		 * @param peersReplies the answers to the status inquiries done to the other participants, stored as a parallel array with index correspondence [[peers]].
		 * @return a [[Maybe]] containing the [[Vote]] with the chosen leader for the current term, or empty if no candidate has an adequately complete log.
		 * @see [[CandidateDecider]] for the candidate ranking hierarchy. */
		def decideMyVote(myStateInfo: StateInfo, peersReplies: IArray[Try[StateInfo]])(using Trace.Context): Maybe[Vote[ParticipantId]]

		/** @return the index of the provided [[ParticipantId]] in the [[peers]]' [[IndexedSeq]] or a negative number if not present.
		 * @param peerId the id of the peer to find. */
		inline def peerIndexOf(peerId: ParticipantId): Int = {
			java.util.Arrays.binarySearch(peers.asInstanceOf[Array[ParticipantId]], peerId, participantIdComparator)
		}

		override def toString: String = backingElectorateChange.toString
	}

	//// StableElectorate ////

	/** Pure new electorate (`Cnew`).
	 *
	 * Activated only once the [[SoleElectorateChange]] entry is committed (`commitIndex >= indexOfSoleElectorateChange`). Replication and quorum reduce to `Cnew` only. `Cold`-only servers, having seen this entry committed, shut down after quiescing.
	 *
	 * @see [[Electorate]] for the dual-electorate lifecycle specification and ghost leader mechanics. */
	final class SoleElectorate(
		override val backingElectorateChange: SoleElectorateChange[ParticipantId],
		override val changeIndex: RecordIndex,
		clusterView: ClusterView
	) extends Electorate {
		override val boundParticipantId: ParticipantId = clusterView.boundParticipantId
		override val term: Term = backingElectorateChange.term
		override val activeParticipants: Set[ParticipantId] = backingElectorateChange.activeParticipants
		override val members: IArray[ParticipantId] = IArray.unsafeFromArray(activeParticipants.toArray.sorted)
		private val halfTheNumberOfParticipants = members.length / 2
		override val peers: IArray[ParticipantId] = members.filter(_ != boundParticipantId)
		override val isBoundIncluded: Boolean = activeParticipants.contains(boundParticipantId)

		override val stableParticipants: Set[ParticipantId] = backingElectorateChange.newParticipants

		def otherProbableParticipants: ListSet[ParticipantId] = {
			ListSet.newBuilder
				.addAll(peers)
				.addAll(clusterView.getOtherProbableParticipants)
				.result()
		}

		override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
			vote.reachableCommonCount > halfTheNumberOfParticipants || members.length == 0
		}

		override def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): Boolean = {
			val othersQuorumThreshold = if isBoundIncluded then halfTheNumberOfParticipants else halfTheNumberOfParticipants + 1
			learnerProgressByIndex.countWithIndex((learnerProgress, _) => learnerProgress.highestRecordIndexKnownToBeAppended >= index) >= othersQuorumThreshold
		}

		override def indexOfTheCommittableRecordWithHighestIndex(log: LogTermLookup, from: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): RecordIndex = {
			assert(from >= log.latestSnapshotLastIncludedRecordIndex, s"from=$from, snapshotLastIncluded=${log.latestSnapshotLastIncludedRecordIndex}")
			var n = log.firstEmptyRecordIndex
			while {
				n -= 1
				// The second condition of this expression enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
				n > from && (log.getRecordTermAt(n) != log.currentTerm || !hasMajorityAppended(n, learnerProgressByIndex))
			} do ()
			n
		}

		override def decideMyVote(myStateInfo: StateInfo, peersReplies: IArray[Try[StateInfo]])(using Trace.Context): Maybe[Vote[ParticipantId]] = {
			Trace.step(() => s"${this.toString}.decideMyVote") {
				var participantsCount = if myStateInfo.rank != ER_NONE then 1 else 0
				val decider = new CandidateDecider(boundParticipantId, myStateInfo, MEMBERSHIP_NEW)
				var contenderIndex = peers.length
				while contenderIndex > 0 do {
					contenderIndex -= 1
					peersReplies(contenderIndex) match {
						case Success(contenderStateInfo) =>
							if contenderStateInfo.rank != ER_NONE then participantsCount += 1
							decider.contend(peers(contenderIndex), contenderStateInfo, MEMBERSHIP_NEW)
						case _ => // do nothing	
					}
				}
				decider.castVote(participantsCount, 0)
			}
		}

		override def accumulateDiscoveryQuorum(
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[StateInfo]]
		): sequencer.Capture[DiscoveryQuorumResult[ParticipantId]] = {
			val numberOfPeers = peers.length
			assert(inquires.length == numberOfPeers)

			val replies = Array.ofDim[Try[StateInfo]](numberOfPeers)
			val subscriptions = new Array[sequencer.Subscription](numberOfPeers)

			def unsubscribeRemaining(): Unit = {
				var j = 0
				while j < numberOfPeers do {
					val sub = subscriptions(j)
					if sub != null then {
						sub.unsubscribeSync()
						subscriptions(j) = null
					}
					if replies(j) == null then replies(j) = DiscoveryEarlyExitCancellation
					j += 1
				}
			}

			val captor = new sequencer.Captor[DiscoveryQuorumResult[ParticipantId]]()
			var highestTermSeen: Term = myStateInfo.currentTerm
			var successfulCount: Int = if isBoundIncluded then 1 else 0
			var unresolvedCount: Int = numberOfPeers

			inline def checkTermination(): Boolean = {
				if successfulCount > halfTheNumberOfParticipants then {
					unsubscribeRemaining()
					captor.captureSync(DiscoveryQuorumResult(
						DiscoveryQuorumOutcome_MajorityReached,
						highestTermSeen,
						IArray.unsafeFromArray(replies)
					))
					true
				} else if successfulCount + unresolvedCount <= halfTheNumberOfParticipants then {
					unsubscribeRemaining()
					captor.captureSync(DiscoveryQuorumResult(
						DiscoveryQuorumOutcome_MajorityImpossible,
						highestTermSeen,
						IArray.unsafeFromArray(replies)
					))
					true
				} else false
			}

			if !checkTermination() then {
				var peerIndex = 0
				while peerIndex < numberOfPeers && captor.isPending do {
					val inquire = inquires(peerIndex)
					val peerId = peers(peerIndex)
					val ventureIndex = peerIndex

					val subscription = inquire.subscribeSyncCallbacks(
						success = { info =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Success(info)
							unresolvedCount -= 1

							if info.currentTerm > highestTermSeen then highestTermSeen = info.currentTerm

							if info.currentTerm > myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(DiscoveryQuorumResult(
									DiscoveryQuorumOutcome_Stale(highestTermSeen),
									highestTermSeen,
									IArray.unsafeFromArray(replies)
								))
							} else if info.rank == ER_LEADING && info.currentTerm == myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(DiscoveryQuorumResult(
									DiscoveryQuorumOutcome_ActiveLeaderDetected(peerId, info.currentTerm),
									highestTermSeen,
									IArray.unsafeFromArray(replies)
								))
							} else {
								successfulCount += 1
								checkTermination()
							}
						},
						error = { ex =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Failure(ex)
							unresolvedCount -= 1
							checkTermination()
						}
					)
					// If the reply is pending (not synchronously captured), then memorize the subscription to the corresponding inquire.
					if replies(ventureIndex) == null then subscriptions(ventureIndex) = subscription
					peerIndex += 1
				}
			}

			captor
		}

		override def accumulateVotingQuorum(
			myVote: Vote[ParticipantId],
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[Vote[ParticipantId]]]
		): sequencer.Capture[VotingQuorumResult[ParticipantId]] = {
			val numberOfPeers = peers.length
			assert(inquires.length == numberOfPeers)

			val replies = Array.ofDim[Try[Vote[ParticipantId]]](numberOfPeers)
			val subscriptions = new Array[sequencer.Subscription](numberOfPeers)

			def unsubscribeRemaining(): Unit = {
				var j = 0
				while j < numberOfPeers do {
					val sub = subscriptions(j)
					if sub != null then {
						sub.unsubscribeSync()
						subscriptions(j) = null
					}
					if replies(j) == null then replies(j) = VotingEarlyExitCancellation
					j += 1
				}
			}

			val captor = new sequencer.Captor[VotingQuorumResult[ParticipantId]]()
			var highestTermSeen: Term = myStateInfo.currentTerm
			var matchingVotesCount: Int = if isBoundIncluded && myVote.isNonBlank then 1 else 0
			var joiningCount: Int = 0
			var unresolvedCount: Int = numberOfPeers

			inline def checkTermination(): Boolean = {
				if matchingVotesCount + joiningCount > halfTheNumberOfParticipants then {
					unsubscribeRemaining()
					captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Won, highestTermSeen, IArray.unsafeFromArray(replies)))
					true
				} else if matchingVotesCount + joiningCount + unresolvedCount <= halfTheNumberOfParticipants then {
					unsubscribeRemaining()
					captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Lost, highestTermSeen, IArray.unsafeFromArray(replies)))
					true
				} else false
			}

			if !checkTermination() then {
				var peerIndex = 0
				while peerIndex < numberOfPeers && captor.isPending do {
					val inquire = inquires(peerIndex)
					val ventureIndex = peerIndex
					val subscription = inquire.subscribeSyncCallbacks(
						success = { vote =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Success(vote)
							unresolvedCount -= 1
							if vote.term > highestTermSeen then highestTermSeen = vote.term

							if vote.term > myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Stale(highestTermSeen), highestTermSeen, IArray.unsafeFromArray(replies)))
							} else {
								if vote.votedId == myVote.votedId && vote.isNonBlank then matchingVotesCount += 1
								else if vote.votedRank == ER_JOINER then joiningCount += 1
								checkTermination()
							}
						},
						error = { ex =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Failure(ex)
							unresolvedCount -= 1
							checkTermination()
						}
					)
					// If the reply is pending (not synchronously captured), then memorize the subscription to the corresponding inquire.
					if replies(ventureIndex) == null then subscriptions(ventureIndex) = subscription
					peerIndex += 1
				}
			}

			captor
		}
	}

	//// JointElectorate ////

	/** Joint consensus electorate (`Cold` ∪ `Cnew`).
	 *
	 * Activated immediately upon append of the transitional entry. Replication and quorum require majorities across both `Cold` and `Cnew`. Elections must also consider both sets. Ends when a [[SoleElectorate]] entry is committed.
	 *
	 * @see [[Electorate]] for commit-index sensitivity and transition invariants. */
	final class JointElectorate(
		override val backingElectorateChange: JointElectorateChange[ParticipantId],
		override val changeIndex: RecordIndex,
		clusterView: ClusterView
	) extends Electorate {
		override val boundParticipantId: ParticipantId = clusterView.boundParticipantId
		private val oldParticipants: Set[ParticipantId] = backingElectorateChange.oldParticipants
		private val newParticipants: Set[ParticipantId] = backingElectorateChange.newParticipants
		private val halfOfOldParticipants: Int = oldParticipants.size / 2
		private val halfOfNewParticipants: Int = newParticipants.size / 2
		override val term: Term = backingElectorateChange.term
		override val activeParticipants: Set[ParticipantId] = backingElectorateChange.activeParticipants
		override val members: IArray[ParticipantId] = IArray.unsafeFromArray(activeParticipants.toArray.sorted)
		override val peers: IArray[ParticipantId] = members.filter(_ != boundParticipantId)
		override val isBoundIncluded: Boolean = activeParticipants.contains(boundParticipantId)
		private val numberOfPeersInOld: Int = peers.count(oldParticipants.contains)
		private val numberOfPeersInNew: Int = peers.count(newParticipants.contains)

		override val stableParticipants: Set[ParticipantId] = newParticipants

		def otherProbableParticipants: ListSet[ParticipantId] = {
			ListSet.newBuilder
				.addAll(peers)
				.addAll(clusterView.getOtherProbableParticipants)
				.result()
		}

		override def reachedAMajority(vote: Vote[ParticipantId]): Boolean = {
			(vote.reachableCommonCount > halfOfOldParticipants || oldParticipants.isEmpty)
				&& (vote.reachableTargetCount > halfOfNewParticipants || newParticipants.isEmpty)
		}

		override def hasMajorityAppended(index: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): Boolean = {
			var oldParticipantsWithRecordAtN = 0
			var newParticipantsWithRecordAtN = 0

			var participantId = boundParticipantId
			var otherParticipantIndex = peers.length
			while otherParticipantIndex >= 0 do {
				if oldParticipants.contains(participantId) then oldParticipantsWithRecordAtN += 1
				if newParticipants.contains(participantId) then newParticipantsWithRecordAtN += 1

				var goNext = true
				otherParticipantIndex -= 1
				while otherParticipantIndex >= 0 && goNext do {
					participantId = peers(otherParticipantIndex)
					if learnerProgressByIndex(otherParticipantIndex).highestRecordIndexKnownToBeAppended >= index
						|| (learnerProgressByIndex(otherParticipantIndex).respondedAsRetiring && !newParticipants.contains(participantId))
					then goNext = false
					else otherParticipantIndex -= 1
				}
			}
			(oldParticipantsWithRecordAtN > halfOfOldParticipants || oldParticipants.isEmpty) && (newParticipantsWithRecordAtN > halfOfNewParticipants || newParticipants.isEmpty)
		}

		override def indexOfTheCommittableRecordWithHighestIndex(log: LogTermLookup, from: RecordIndex, learnerProgressByIndex: IArray[PeerProgressView]): RecordIndex = {
			assert(from >= log.latestSnapshotLastIncludedRecordIndex, s"from=$from, snapshotLastIncluded=${log.latestSnapshotLastIncludedRecordIndex}")
			var n = log.firstEmptyRecordIndex - 1
			while n > from do {
				// The first condition of this `if` enforces Raft §5.4.2 ("A leader cannot determine commitment using entries from previous terms") and that a leader inheriting previous-term records cannot commit them without first committing a current-term record
				if log.getRecordTermAt(n) == log.currentTerm && hasMajorityAppended(n, learnerProgressByIndex) then return n
				n -= 1
			}
			from
		}

		override def decideMyVote(myStateInfo: StateInfo, peersReplies: IArray[Try[StateInfo]])(using Trace.Context): Maybe[Vote[ParticipantId]] = {
			Trace.step(() => s"${this.toString}.decideMyVote") {
				var oldParticipantsCount = 0
				var newParticipantsCount = 0

				// Constructs the membership bitmask indicating whether the contender belongs to oldParticipants, newParticipants, or both.
				inline def treatParticipant(contenderId: ParticipantId, contenderStateInfo: StateInfo): MembershipMask = {
					val isInOldSet = oldParticipants.contains(contenderId)
					val isInNewSet = newParticipants.contains(contenderId)
					if contenderStateInfo.rank != ER_NONE then {
						if isInOldSet then oldParticipantsCount += 1
						if isInNewSet then newParticipantsCount += 1
					}
					(if isInOldSet then MEMBERSHIP_OLD else 0) | (if isInNewSet then MEMBERSHIP_NEW else 0)
				}

				val decider = new CandidateDecider(boundParticipantId, myStateInfo, treatParticipant(boundParticipantId, myStateInfo))
				var contenderIndex = peers.length
				while contenderIndex > 0 do {
					contenderIndex -= 1
					peersReplies(contenderIndex) match {
						case Success(contenderStateInfo) =>
							val contenderId = peers(contenderIndex)
							decider.contend(contenderId, contenderStateInfo, treatParticipant(contenderId, contenderStateInfo))
						case _ =>
					}
				}
				decider.castVote(oldParticipantsCount, newParticipantsCount)
			}
		}

		override def accumulateDiscoveryQuorum(
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[StateInfo]]
		): sequencer.Capture[DiscoveryQuorumResult[ParticipantId]] = {
			val numberOfPeers = peers.length
			assert(inquires.length == numberOfPeers)

			var unresolvedOld: Int = numberOfPeersInOld
			var unresolvedNew: Int = numberOfPeersInNew

			val replies = Array.ofDim[Try[StateInfo]](numberOfPeers)
			val subscriptions = new Array[sequencer.Subscription](numberOfPeers)

			def unsubscribeRemaining(): Unit = {
				var j = 0
				while j < numberOfPeers do {
					val sub = subscriptions(j)
					if sub != null then {
						sub.unsubscribeSync()
						subscriptions(j) = null
					}
					if replies(j) == null then replies(j) = DiscoveryEarlyExitCancellation
					j += 1
				}
			}

			val captor = new sequencer.Captor[DiscoveryQuorumResult[ParticipantId]]()
			var highestTermSeen: Term = myStateInfo.currentTerm
			var oldSuccessful: Int = if oldParticipants.contains(boundParticipantId) then 1 else 0
			var newSuccessful: Int = if newParticipants.contains(boundParticipantId) then 1 else 0

			inline def checkTermination(): Boolean = {
				val oldWon = oldParticipants.isEmpty || (oldSuccessful > halfOfOldParticipants)
				val newWon = newParticipants.isEmpty || (newSuccessful > halfOfNewParticipants)
				if oldWon && newWon then {
					unsubscribeRemaining()
					captor.captureSync(DiscoveryQuorumResult(
						DiscoveryQuorumOutcome_MajorityReached,
						highestTermSeen,
						IArray.unsafeFromArray(replies)
					))
					true
				} else {
					val oldLost = oldParticipants.nonEmpty && (oldSuccessful + unresolvedOld <= halfOfOldParticipants)
					val newLost = newParticipants.nonEmpty && (newSuccessful + unresolvedNew <= halfOfNewParticipants)
					if oldLost || newLost then {
						unsubscribeRemaining()
						captor.captureSync(DiscoveryQuorumResult(
							DiscoveryQuorumOutcome_MajorityImpossible,
							highestTermSeen,
							IArray.unsafeFromArray(replies)
						))
						true
					} else false
				}
			}

			if !checkTermination() then {
				var peerIndex = 0
				while peerIndex < numberOfPeers && captor.isPending do {
					val inquire = inquires(peerIndex)
					val peerId = peers(peerIndex)
					val ventureIndex = peerIndex

					val subscription = inquire.subscribeSyncCallbacks(
						success = { info =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Success(info)
							if oldParticipants.contains(peerId) then unresolvedOld -= 1
							if newParticipants.contains(peerId) then unresolvedNew -= 1

							if info.currentTerm > highestTermSeen then highestTermSeen = info.currentTerm

							if info.currentTerm > myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(DiscoveryQuorumResult(
									DiscoveryQuorumOutcome_Stale(highestTermSeen),
									highestTermSeen,
									IArray.unsafeFromArray(replies)
								))
							} else if info.rank == ER_LEADING && info.currentTerm == myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(DiscoveryQuorumResult(
									DiscoveryQuorumOutcome_ActiveLeaderDetected(peerId, info.currentTerm),
									highestTermSeen,
									IArray.unsafeFromArray(replies)
								))
							} else {
								if oldParticipants.contains(peerId) then oldSuccessful += 1
								if newParticipants.contains(peerId) then newSuccessful += 1
								checkTermination()
							}
						},
						error = { ex =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Failure(ex)
							if oldParticipants.contains(peerId) then unresolvedOld -= 1
							if newParticipants.contains(peerId) then unresolvedNew -= 1
							checkTermination()
						}
					)
					// If the reply is pending (not synchronously captured), then memorize the subscription to the corresponding inquire.
					if replies(ventureIndex) == null then subscriptions(ventureIndex) = subscription
					peerIndex += 1
				}
			}

			captor
		}

		override def accumulateVotingQuorum(
			myVote: Vote[ParticipantId],
			myStateInfo: StateInfo,
			inquires: IArray[sequencer.Capture[Vote[ParticipantId]]]
		): sequencer.Capture[VotingQuorumResult[ParticipantId]] = {
			val numberOfPeers = peers.length
			assert(inquires.length == numberOfPeers)

			var unresolvedOld: Int = numberOfPeersInOld
			var unresolvedNew: Int = numberOfPeersInNew

			val replies = Array.ofDim[Try[Vote[ParticipantId]]](numberOfPeers)
			val subscriptions = new Array[sequencer.Subscription](numberOfPeers)

			def unsubscribeRemaining(): Unit = {
				var j = 0
				while j < numberOfPeers do {
					val sub = subscriptions(j)
					if sub != null then {
						sub.unsubscribeSync()
						subscriptions(j) = null
					}
					if replies(j) == null then replies(j) = VotingEarlyExitCancellation
					j += 1
				}
			}

			val captor = new sequencer.Captor[VotingQuorumResult[ParticipantId]]()
			var highestTermSeen: Term = myStateInfo.currentTerm
			var oldMatching: Int = if myVote.isNonBlank && oldParticipants.contains(boundParticipantId) then 1 else 0
			var newMatching: Int = if myVote.isNonBlank && newParticipants.contains(boundParticipantId) then 1 else 0
			var oldRetiring: Int = if myVote.votedRank == ER_RETIREE && oldParticipants.contains(boundParticipantId) then 1 else 0
			var newJoining: Int = if myVote.votedRank == ER_JOINER && newParticipants.contains(boundParticipantId) then 1 else 0

			inline def checkTermination(): Boolean = {
				val oldWon = oldParticipants.isEmpty || (oldMatching + oldRetiring > halfOfOldParticipants)
				val newWon = newParticipants.isEmpty || (newMatching + newJoining > halfOfNewParticipants)
				if oldWon && newWon then {
					unsubscribeRemaining()
					captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Won, highestTermSeen, IArray.unsafeFromArray(replies)))
					true
				} else {
					val oldLost = oldParticipants.nonEmpty && (oldMatching + oldRetiring + unresolvedOld <= halfOfOldParticipants)
					val newLost = newParticipants.nonEmpty && (newMatching + newJoining + unresolvedNew <= halfOfNewParticipants)
					if oldLost || newLost then {
						unsubscribeRemaining()
						captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Lost, highestTermSeen, IArray.unsafeFromArray(replies)))
						true
					} else false
				}
			}

			if !checkTermination() then {
				var peerIndex = 0
				while peerIndex < numberOfPeers && captor.isPending do {
					val peerId = peers(peerIndex)
					val inquire = inquires(peerIndex)

					val ventureIndex = peerIndex
					val subscription = inquire.subscribeSyncCallbacks(
						success = { vote =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Success(vote)
							if oldParticipants.contains(peerId) then unresolvedOld -= 1
							if newParticipants.contains(peerId) then unresolvedNew -= 1

							if vote.term > highestTermSeen then highestTermSeen = vote.term

							if vote.term > myStateInfo.currentTerm then {
								unsubscribeRemaining()
								captor.captureSync(VotingQuorumResult(VotingQuorumOutcome_Stale(highestTermSeen), highestTermSeen, IArray.unsafeFromArray(replies)))
							} else {
								if vote.votedId == myVote.votedId && vote.isNonBlank then {
									if oldParticipants.contains(peerId) then oldMatching += 1
									if newParticipants.contains(peerId) then newMatching += 1
								} else {
									if vote.votedRank == ER_RETIREE && oldParticipants.contains(peerId) then oldRetiring += 1
									if vote.votedRank == ER_JOINER && newParticipants.contains(peerId) then newJoining += 1
								}
								checkTermination()
							}
						},
						error = { ex =>
							subscriptions(ventureIndex) = null
							replies(ventureIndex) = Failure(ex)
							if oldParticipants.contains(peerId) then unresolvedOld -= 1
							if newParticipants.contains(peerId) then unresolvedNew -= 1
							checkTermination()
						}
					)
					// If the reply is pending (not synchronously captured), then memorize the subscription to the corresponding inquire.
					if replies(ventureIndex) == null then subscriptions(ventureIndex) = subscription
					peerIndex += 1
				}
			}

			captor
		}
	}

	type TransitionalElectorate = JointElectorate

	def Electorate_from(
		electorateChange: ElectorateChange[ParticipantId],
		changeIndex: RecordIndex,
		clusterView: ClusterView
	): Electorate = {
		electorateChange match {
			case jec: JointElectorateChange[ParticipantId] =>
				new JointElectorate(jec, changeIndex, clusterView)
			case sec: SoleElectorateChange[ParticipantId] =>
				new SoleElectorate(sec, changeIndex, clusterView)
		}
	}


	//// Candidate decider ////

	/** Bitmask indicating membership in the active consensus configurations (`Cold` and `Cnew`). */
	type MembershipMask = Int

	/** Contender belongs to the new configuration set (`Cnew`). */
	final inline val MEMBERSHIP_NEW: 1 = 1
	/** Contender belongs to the old configuration set (`Cold`). */
	final inline val MEMBERSHIP_OLD: 2 = 2

	/** Evaluates and ranks candidate peers against local state to determine the best candidate to vote for.
	 *
	 * Contenders are compared using a deterministic, total-order hierarchy:
	 *  1. '''Term Monotonicity''' ([[StateInfo.currentTerm]]): Contenders with strictly higher terms always win.
	 *  2. '''Incumbent Leadership''' ([[StateInfo.rank]] == `ER_LEADING`): An active leader of the current term takes precedence over non-leaders to preserve leadership stability.
	 *  3. '''Log Completeness''' ([[StateInfo.compareCompleteness]]): Compares `lastRecordTerm`, then `lastRecordIndex`. Crucially, log completeness takes strict priority over all non-leading ranks (`ER_CANDIDATE`, `ER_FOLLOWER`, `ER_RETIREE`, `ER_JOINER`). This guarantees that a retiring or joining node holding a more complete log beats an `ER_CANDIDATE`, forcing active nodes to vote for it and safely absorb its committed log entries.
	 *  4. '''Candidate Preference''' ([[StateInfo.rank]] == `ER_CANDIDATE`): When log completeness is identical, active candidates take precedence over passive followers or retirees to avoid vote fragmentation.
	 *  5. '''Candidate Membership Tier''' (`membershipMask`): Bitmask encoding membership in target/new (`bit 0 = Cnew`, weight 1) and common/old (`bit 1 = Cold`, weight 2), establishing the 3-tier precedence: Surviving (`3`, `Cold ∩ Cnew`) > Retiring (`2`, `Cold \ Cnew`) > Joining (`1`, `Cnew \ Cold`) > Non-member (`0`). Old-set precedence (`Cold` over `Cnew \ Cold`) enforces election liveness when an uncommitted transitional entry creates divergent active electorates across peers, while surviving precedence (`Cold ∩ Cnew` over `Cold \ Cnew`) eliminates ghost leader churn and redundant handovers upon committing the sole configuration.
	 *  6. '''Deterministic Tie-Breaking''' (`participantId`): Total order on identifiers (`incumbentId < otherId`) ensures all participants observing an identical candidate subset converge on the exact same candidate.
	 *
	 * @param voterId the [[ParticipantId]] of the evaluating voter.
	 * @param voterStateInfo the [[StateInfo]] of the evaluating voter.
	 * @param voterMembershipMask bitmask indicating voter membership in new (bit 0) and old (bit 1) participant sets. */
	private final class CandidateDecider(voterId: ParticipantId, voterStateInfo: StateInfo, voterMembershipMask: MembershipMask) {
		private var chosenId = voterId
		private var chosenInfo = voterStateInfo
		private var chosenMembershipMask: MembershipMask = voterMembershipMask
		private var mostCompleteInfo = voterStateInfo

		/** Evaluates a candidate against the currently chosen candidate according to the evaluation hierarchy.
		 *
		 * @param contenderId the [[ParticipantId]] of the contender.
		 * @param contenderInfo the [[StateInfo]] of the contender.
		 * @param contenderMembershipMask bitmask indicating contender membership in new (bit 0) and old (bit 1) participant sets.
		 * @see [[CandidateDecider]] for the 6-point evaluation ranking hierarchy. */
		def contend(contenderId: ParticipantId, contenderInfo: StateInfo, contenderMembershipMask: MembershipMask): Unit = {
			val incumbentId = chosenId
			val incumbentInfo = chosenInfo
			val incumbentMembershipMask = chosenMembershipMask

			val theOtherWins =
				if incumbentInfo.currentTerm > contenderInfo.currentTerm then false
				else if incumbentInfo.currentTerm < contenderInfo.currentTerm then true
				else if incumbentInfo.rank == ER_LEADING && contenderInfo.rank != ER_LEADING then false
				else if incumbentInfo.rank != ER_LEADING && contenderInfo.rank == ER_LEADING then true
				else {
					val completenessComparison = incumbentInfo.compareCompleteness(contenderInfo)
					if completenessComparison > 0 then false
					else if completenessComparison < 0 then true
					else if incumbentInfo.rank == ER_CANDIDATE && contenderInfo.rank != ER_CANDIDATE then false
					else if incumbentInfo.rank != ER_CANDIDATE && contenderInfo.rank == ER_CANDIDATE then true
					else if incumbentMembershipMask > contenderMembershipMask then false
					else if incumbentMembershipMask < contenderMembershipMask then true
					else if incumbentId < contenderId then false
					else true
				}
			if theOtherWins then {
				chosenId = contenderId
				chosenInfo = contenderInfo
				chosenMembershipMask = contenderMembershipMask
			}
			if contenderInfo.compareCompleteness(mostCompleteInfo) > 0 then mostCompleteInfo = contenderInfo
		}

		/** Casts the vote for the best candidate if that candidate's log is at least as complete as the most complete log observed.
		 *
		 * @param reachableCommonParticipants count of reachable participants in the common/old set.
		 * @param reachableTargetParticipants count of reachable participants in the target/new set.
		 * @return a [[Maybe]] containing the cast [[Vote]], or [[Maybe.empty]] if the chosen candidate lacks sufficient completeness. */
		def castVote(reachableCommonParticipants: Int, reachableTargetParticipants: Int)(using Trace.Context): Maybe[Vote[ParticipantId]] = {
			val ci = chosenInfo
			val castedVote =
				if ci.compareCompleteness(mostCompleteInfo) >= 0 then Maybe(Vote(voterStateInfo.currentTerm, chosenId, reachableCommonParticipants, reachableTargetParticipants, ci.rank))
				else Maybe.empty
			Trace.debug(s"castedVote=$castedVote")
			castedVote
		}
	}
}
