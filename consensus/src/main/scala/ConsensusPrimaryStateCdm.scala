package readren.consensus

import protocol.*

import readren.common.*
import readren.common.Trace.Context

import scala.annotation.publicInBinary

/**
 * Component Definition Module (CDM) for consensus log persistence,
 * workspace management, and primary state operations.
 *
 * Requires [[ConsensusElectorateCdm]] for participant identification,
 * sequencer singleton access, and [[LogTermLookup]] contracts.
 */
trait ConsensusPrimaryStateCdm { this: ConsensusElectorateCdm =>

	//// PERSISTENCE SPI ////

	/**
	 * Specifies the unit of work that a consensus participant requires to manage its persistent state.
	 *
	 * Instances of this trait must be accessed only within a `primaryStateUpdater` passed to the
	 * [[sequencer.CausalFence.advance]] method of the [[sequencer.CausalFence]] instance.
	 */
	trait Workspace {

		/** The current term according to this participant.
		 * Initial value is zero (PRE_INIT). Zero means "before the first election". */
		def getCurrentTerm: Term

		def setCurrentTerm(term: Term): Unit

		/** The candidate participant id this participant voted for in [[getCurrentTerm]], or [[Maybe.empty]] if none. */
		def getVotedFor: Maybe[ParticipantId]

		/** Sets the candidate participant id this participant voted for in [[getCurrentTerm]]. */
		def setVotedFor(votedFor: Maybe[ParticipantId]): Unit

		/** Sets both the current term and the voted candidate atomically. */
		def setTermAndVote(term: Term, votedFor: Maybe[ParticipantId]): Unit = {
			setCurrentTerm(term)
			setVotedFor(votedFor)
		}

		/** The index of the oldest [[Record]] stored in the log buffer. The initial value is 1. */
		def logBufferOffset: RecordIndex

		/** The index of the first empty entry in the log. The initial value is 1. */
		def firstEmptyRecordIndex: RecordIndex

		def getRecordAt(index: RecordIndex): Record

		/** Returns the records in the log starting at `from` and up to `until` exclusive. */
		def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record]

		/** Appends a record. */
		def appendRecord(record: Record): Unit

		/** Discards all records in the log buffer starting from the specified index (inclusive). */
		def truncateSuffix(fromIndex: RecordIndex): Unit

		/** Replaces the whole log with the provided snapshot and tail records. */
		def resetLog(snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record]): Unit

		/** Truncates the log by replacing its earlier records with the provided snapshot.
		 * After this call, [[logBufferOffset]] must return `snapshot.lastIncludedRecordIndex + 1`.
		 * @param snapshot the consolidated [[SnapshotData]] to replace the truncated records */
		def truncatePrefix(snapshot: SnapshotData[ParticipantId]): Unit

		/** @return the [[SnapshotData]] produced by the last call to [[truncatePrefix]]. */
		def latestSnapshot: Maybe[SnapshotData[ParticipantId]]

		/** Called to inform that this [[Workspace]] instance will not be referenced anymore and may be purged. */
		def release(): sequencer.Capture[Unit]
	}

	/** The concrete workspace type provided by the host environment or test harness. */
	type WS <: Workspace

	/**
	 * Defines what a consensus participant requires from a persistence service to load and save its [[Workspace]].
	 *
	 * Implementations may assume that all methods of this trait are invoked within the [[sequencer]] thread.
	 */
	trait Storage {
		def load: sequencer.Capture[WS]

		/** Saves the workspace to persistent storage.
		 * Design Note: A failure to save the workspace should restart the [[ConsensusParticipant]] as if it had crashed and lost all non-persistent variables. */
		def save(workspace: WS): sequencer.Capture[Unit]
	}

	//// REPORTING ////

	/** Report receptacle mutated during record fusion to communicate state updates. */
	trait FusionReport {
		/** True if records were appended to the local log buffer. */
		var isFused: Boolean = false
		/** True if the local [[Term]] was updated to match or exceed the provider's term. */
		var isTermUpdated: Boolean = false
		/** True if the local snapshot was updated and tail records were appended. */
		var isSnapshotUpdated: Boolean = false
		/** True if the received snapshot is useful but the commands applier is currently running. */
		var haveToWaitCommandApplier: Boolean = false
	}

	//// PRIMARY STATE ////

	/** A view of the participant’s current primary state (log, term, snapshot), and trivially derived state.
	 *
	 * == Causal Observation Invariant ==
	 * Accessing mutable state of the [[PrimaryState]] must occur either:
	 *  - Within an updater passed to the causal fence `advance` method. In this case, the call must occur before or during the completion of the [[sequencer.Capture]] returned by the updater; once that task has completed, the causal fence is closed and later calls are unsafe.
	 *  - Within a causally anchored consumer (i.e. consumers subscribed to the [[sequencer.Capture]] returned by either `advance` or `causalAnchor`). In this case, the call must occur synchronously during the consumer’s execution; it must not be deferred to code scheduled after the consumer has returned, since such deferred code would no longer be causally anchored.
	 * Direct observation of [[PrimaryState]] outside these mechanisms breaks causal consistency guarantees.
	 *
	 * == Causal Mutation Invariant ==
	 * Mutating the [[PrimaryState]] must be executed strictly within the safe temporal windows provided by the causal fence `advance` mechanism.
	 *
	 * @param workspace the underlying persistence unit holding term, vote, and log buffer.
	 * @param storage the persistence storage SPI used to persist workspace mutations. */
	final class PrimaryState(
		@publicInBinary protected val workspace: WS,
		storage: Storage
	) extends LogTermLookup { thisPrimaryState =>

		/** The [[Term]] of this [[PrimaryState]]. Immutable after construction. */
		val currentTerm: Term = workspace.getCurrentTerm

		/** The participant id this participant voted for in [[currentTerm]]. */
		val votedFor: Maybe[ParticipantId] = workspace.getVotedFor

		/** Index of the first empty record in the log. Immutable after construction.
		 * This is trivially derived state. */
		val firstEmptyRecordIndex: RecordIndex = workspace.firstEmptyRecordIndex

		private var _indexOfLatestElectorateChange: RecordIndex = 0
		private var _maybeLatestElectorateChange: Maybe[ElectorateChange[ParticipantId]] = Maybe.empty

		{ // Constructor
			val offset = workspace.logBufferOffset
			var index = workspace.firstEmptyRecordIndex
			var record: Record | Null = null
			while index > offset && {
				index -= 1
				record = workspace.getRecordAt(index)
				!record.isInstanceOf[ElectorateChange[ParticipantId] @unchecked]
			} do ()
			record match {
				case cc: ElectorateChange[ParticipantId] @unchecked =>
					_indexOfLatestElectorateChange = index
					_maybeLatestElectorateChange = Maybe(cc)
				case _ =>
					if workspace.latestSnapshot.isDefined then {
						val snapshot = workspace.latestSnapshot.get
						_indexOfLatestElectorateChange = snapshot.latestElectorateChangeIndex
						_maybeLatestElectorateChange = Maybe(snapshot.latestElectorateChange)
					}
			}
		}

		/** @return the [[Record]] at the specified [[RecordIndex]].
		 * @note CAUTION: This method is not thread-safe. It must be called from within the [[sequencer]] thread.
		 * @throws java.lang.IndexOutOfBoundsException if the index is out of bounds.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		def getRecordAt(index: RecordIndex): Record = {
			if index >= logBufferOffset then workspace.getRecordAt(index)
			else latestSnapshot.fold(throw IndexOutOfBoundsException(s"Record at index $index is below lower bound 1.")) { snapshot =>
				if index == snapshot.latestElectorateChangeIndex then snapshot.latestElectorateChange
				else throw IndexOutOfBoundsException(s"Record at index $index is below logBufferOffset=$logBufferOffset and is not the latest electorate change.")
			}
		}

		/** @return the [[Term]] of the [[Record]] at the specified [[RecordIndex]].
		 * @note CAUTION: This method is not thread-safe. It must be called from within the [[sequencer]] thread.
		 * @throws java.lang.IndexOutOfBoundsException if the index is out of bounds.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		def getRecordTermAt(index: RecordIndex): Term = {
			if index >= logBufferOffset then getRecordAt(index).term
			else if index == 0 then PRE_INIT
			else latestSnapshot.fold(throw IndexOutOfBoundsException(s"Record's term at index $index is below lower bound zero.")) { snapshot =>
				if index == snapshot.latestElectorateChangeIndex then snapshot.latestElectorateChange.term
				else if index == snapshot.lastIncludedRecordIndex then snapshot.lastIncludedRecordTerm
				else throw IndexOutOfBoundsException(s"Record at index $index is below logBufferOffset=$logBufferOffset and is not the latest electorate change")
			}
		}

		/** @return the index of the last record included in the latest snapshot, or `0` if no snapshot exists.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		override def latestSnapshotLastIncludedRecordIndex: RecordIndex = {
			workspace.latestSnapshot.fold(0: RecordIndex)(_.lastIncludedRecordIndex)
		}

		/** @return the records in the log starting at `from` and up to `until` exclusive.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		inline def getRecordsBetween(from: RecordIndex, until: RecordIndex): IArray[Record] = {
			workspace.getRecordsBetween(from, until)
		}

		/** @return the index of the oldest [[Record]] stored in the log buffer.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		inline def logBufferOffset: RecordIndex = workspace.logBufferOffset

		/** @return the index of the latest [[ElectorateChange]] record in the log or latest snapshot.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		inline def indexOfLatestElectorateChange: RecordIndex = _indexOfLatestElectorateChange

		/** @return the latest [[ElectorateChange]] in the log or latest snapshot, or [[Maybe.empty]] if none exists.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		inline def latestElectorateChange: Maybe[ElectorateChange[ParticipantId]] = _maybeLatestElectorateChange

		/** Updates the term in the workspace, clearing the vote if the term increased.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withTermUpdated(newTerm: Term)(using Trace.Context): sequencer.Capture[PrimaryState] = {
			if newTerm <= workspace.getCurrentTerm then sequencer.Keeper(thisPrimaryState)
			else {
				workspace.setTermAndVote(newTerm, Maybe.empty)
				saveWorkspace()
			}
		}

		/** Updates both the term and the voted candidate in the workspace.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withTermAndVoteUpdated(newTerm: Term, newVotedFor: Maybe[ParticipantId])(using Trace.Context): sequencer.Capture[PrimaryState] = {
			if newTerm < workspace.getCurrentTerm then sequencer.Keeper(thisPrimaryState)
			else if newTerm == workspace.getCurrentTerm && newVotedFor == workspace.getVotedFor then sequencer.Keeper(thisPrimaryState)
			else {
				workspace.setTermAndVote(newTerm, newVotedFor)
				saveWorkspace()
			}
		}

		/** Appends a single record to the log buffer, updating the term and clearing vote if term advanced.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withSingleRecordAppended(term: Term, record: Record)(using Trace.Context): sequencer.Capture[PrimaryState] = {
			if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
			else workspace.setCurrentTerm(term)
			workspace.appendRecord(record)
			saveWorkspace()
		}

		/** Tries to fuse the provided batch of [[Record]]s with the local log.
		 *
		 * Fusing occurs when the batch of records matches the local log up to `prevRecordIndex`, and either continues from there or overwrites conflicting uncommitted records.
		 *
		 * Design Notes:
		 *  - If the inquirer's term is greater than the local term, the local [[currentTerm]] is updated and the [[votedFor]] is cleared before attempting fusion.
		 *  - Suffix truncation only occurs if an existing record conflicts with a new record in the batch (i.e. same index, different term).
		 *  - If the batch extends the log without conflict, the new records are appended without truncating.
		 *  - Truncation never removes committed records because the leader's commit index is guaranteed to be consistent with all committed entries.
		 *
		 * @param term the [[Term]] of the provider of the batch of records.
		 * @param prevRecordIndex the index immediately before the first entry in the batch.
		 * @param prevRecordTerm the term of the entry immediately before the first entry in the batch.
		 * @param batch the [[Record]]s to fuse.
		 * @param commitIndex the participant's current commit watermark, preventing suffix truncation below committed records.
		 * @param reportReceptacle a [[FusionReport]] to be mutated by this method to communicate what happened during the update: The [[FusionReport.isFused]] is set if a record was fused; the [[FusionReport.isTermUpdated]] is set if the local term was updated.
		 * @return a [[Maybe]] containing either:
		 *  - a [[sequencer.Capture]] that yields the updated [[PrimaryState]] after successfully saving it in the [[Storage]];
		 *  - a failed [[sequencer.Capture]] if the saving failed;
		 *  - nothing ([[Maybe.empty]]) if either earlier [[Record]]s are needed, the term mismatches, or the batch fully predates the latest snapshot.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def tryFusingRecords(
			term: Term,
			prevRecordIndex: RecordIndex,
			prevRecordTerm: Term,
			batch: IArray[Record],
			commitIndex: RecordIndex,
			reportReceptacle: FusionReport
		)(using Trace.Context): Maybe[sequencer.Capture[PrimaryState]] = {
			var isMutated = false // memorizes if the workspace is mutated.

			// If the inquirer term is greater, update local term and clear vote
			if term > workspace.getCurrentTerm then {
				isMutated = true
				workspace.setTermAndVote(term, Maybe.empty)
				reportReceptacle.isTermUpdated = true
			}

			var indexInBatchOfFirstRecordToFuse: Int = 0
			// 1. Verify consistency assuming the Log Matching invariant holds.
			val isConsistent = {
				if prevRecordIndex >= thisPrimaryState.workspace.logBufferOffset then {
					prevRecordIndex < firstEmptyRecordIndex && prevRecordTerm == workspace.getRecordAt(prevRecordIndex).term
				} else {
					workspace.latestSnapshot.fold {
						prevRecordIndex == 0 && prevRecordTerm == PRE_INIT
					} { snapshot =>
						indexInBatchOfFirstRecordToFuse = (snapshot.lastIncludedRecordIndex - prevRecordIndex).toInt
						if indexInBatchOfFirstRecordToFuse == 0 then prevRecordTerm == snapshot.lastIncludedRecordTerm
						else indexInBatchOfFirstRecordToFuse <= batch.length && batch(indexInBatchOfFirstRecordToFuse - 1).term == snapshot.lastIncludedRecordTerm
					}
				}
			}

			if isConsistent then {
				reportReceptacle.isFused = true
				// Find the first index where the local log conflicts with the batch
				var readIndex = indexInBatchOfFirstRecordToFuse
				var compareIndex = prevRecordIndex + indexInBatchOfFirstRecordToFuse + 1
				while readIndex < batch.length && compareIndex < workspace.firstEmptyRecordIndex && {
					if workspace.getRecordAt(compareIndex).term == batch(readIndex).term then true
					else {
						isMutated = true
						workspace.truncateSuffix(compareIndex)
						false
					}
				} do {
					readIndex += 1
					compareIndex += 1
				}
				// If the batch has records that are not in the local log, append them
				if readIndex < batch.length then {
					isMutated = true
					while {
						workspace.appendRecord(batch(readIndex))
						readIndex += 1
						readIndex < batch.length
					} do ()
				} else {
					if compareIndex <= commitIndex then compareIndex = commitIndex + 1
					// If the local log has records with lower terms than the inquirer's term that are not in the batch, truncate them.
					if compareIndex < workspace.firstEmptyRecordIndex && workspace.getRecordAt(compareIndex).term < term then {
						isMutated = true
						workspace.truncateSuffix(compareIndex)
					}
				}
			}

			if isMutated then Maybe(saveWorkspace()) else Maybe.empty
		}

		/** Truncates the log by replacing earlier records (up to and including the provided [[RecordIndex]]) with the provided snapshot.\
		 * The snapshot must be taken immediately after the last applied command of the removed records.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withLogTruncated(term: Term, lastIncludedRecordIndex: RecordIndex, stateMachineSnapshot: IArray[Byte])(using Trace.Context): sequencer.Capture[PrimaryState] = {
			if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
			else workspace.setCurrentTerm(term)
			val lastIncludedRecordTerm = getRecordTermAt(lastIncludedRecordIndex)

			val snapshot = if _indexOfLatestElectorateChange <= lastIncludedRecordIndex then {
				new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, _maybeLatestElectorateChange.get, _indexOfLatestElectorateChange, stateMachineSnapshot)
			} else {
				var relativeIndex = lastIncludedRecordIndex
				while relativeIndex >= workspace.logBufferOffset && !workspace.getRecordAt(relativeIndex).isInstanceOf[ElectorateChange[ParticipantId] @unchecked] do {
					relativeIndex -= 1
				}

				if relativeIndex >= workspace.logBufferOffset then {
					val cc = workspace.getRecordAt(relativeIndex).asInstanceOf[ElectorateChange[ParticipantId]]
					new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, cc, relativeIndex, stateMachineSnapshot)
				} else {
					val cc = workspace.latestSnapshot.get.latestElectorateChange
					val ccIndex = workspace.latestSnapshot.get.latestElectorateChangeIndex
					new SnapshotData[ParticipantId](lastIncludedRecordIndex, lastIncludedRecordTerm, cc, ccIndex, stateMachineSnapshot)
				}
			}

			workspace.truncatePrefix(snapshot)
			saveWorkspace()
		}

		/** Replaces the whole log with the provided snapshot followed by the provided records.\
		 * The snapshot must have been taken immediately after the last applied command of the removed records.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withLogReplaced(term: Term, snapshot: SnapshotData[ParticipantId], tailRecords: IArray[Record])(using Trace.Context): sequencer.Capture[PrimaryState] = {
			workspace.resetLog(snapshot, tailRecords)
			if term > workspace.getCurrentTerm then workspace.setTermAndVote(term, Maybe.empty)
			else workspace.setCurrentTerm(term)
			saveWorkspace()
		}

		/** @return the [[SnapshotData]] produced by the last call to [[truncatePrefix]], or [[Maybe.empty]] if none exists.
		 * @see [[PrimaryState]] for causal observation safety invariants. */
		inline def latestSnapshot: Maybe[SnapshotData[ParticipantId]] = workspace.latestSnapshot

		/** Informs that this [[PrimaryState]]'s [[Workspace]] will not be referenced anymore and may be purged.
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		def withWorkspaceReleased(): sequencer.Capture[PrimaryState] = {
			workspace.release()
			sequencer.Failed(new GracefullyReleased)
		}

		/** Saves the [[Workspace]] of this [[PrimaryState]] in persistent storage.
		 * @return the [[sequencer.Capture]] that yields the saved [[PrimaryState]].
		 * @see [[PrimaryState]] for causal mutation temporal safety invariants. */
		private def saveWorkspace()(using Trace.Context): sequencer.Capture[PrimaryState] = {
			new sequencer.Captor[PrimaryState] with sequencer.MonoObserver[Unit] { thisCaptor =>
				{ // Constructor
					storage.save(workspace).triggerSync(thisCaptor)
				}

				override def onSuccess(a: Unit): Unit = {
					thisCaptor.captureSync(new PrimaryState(workspace, storage))
				}

				override def onError(e: Throwable): Unit = {
					workspace.release()
					thisCaptor.trapSync(e)
				}
			}
		}

		override def toString: String = {
			s"PrimaryState(currentTerm=$currentTerm, votedFor=$votedFor, firstEmptyRecordIndex=$firstEmptyRecordIndex, indexOfTopElectorateChange=$indexOfLatestElectorateChange)"
		}
	}
}
