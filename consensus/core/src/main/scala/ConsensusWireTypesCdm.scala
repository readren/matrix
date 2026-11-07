package readren.consensus

import protocol.*
import java.util.Comparator
import scala.collection.immutable.ListSet
import scala.reflect.ClassTag

/**
 * Component Definition Module (CDM) for consensus domain data types,
 * cluster view abstractions, and client response contracts.
 *
 * This module defines abstract participant, client, and state machine types along with
 * wire-transmitted responses to clients and narrow cluster views.
 * It contains no threading or sequencer execution model dependencies.
 */
trait ConsensusWireTypesCdm {

	/** The type of participant ids. */
	type ParticipantId <: AnyRef: {Ordering, ClassTag}

	val participantIdComparator: Comparator[ParticipantId] = summon[Ordering[ParticipantId]]

	/** The type of the client identifier.
	 *
	 * Each client interacting with the consensus system must be uniquely identifiable.
	 * This identifier is used to associate commands with their origin and to enforce per-client deduplication and retry semantics.
	 */
	type ClientId

	/** The type of commands received from clients.
	 *
	 * Commands must carry enough information to support deduplication, ordering, and conflict detection. Typically, this includes a `ClientId` and a monotonically increasing request identifier or timestamp.
	 */
	type ClientCommand <: AnyRef

	/** The type of the state-machine's responses. */
	type StateMachineResponse

	//// CLUSTER VIEW

	/** Narrow view of the host cluster bridge required by consensus components to identify the local participant and retrieve the cluster tier's memorized set of probable peers. */
	trait ClusterView {
		val boundParticipantId: ParticipantId

		/** Returns the memorized set of other cluster participants (excluding [[boundParticipantId]]) that the cluster tier currently considers stable, up, and intended for consensus. */
		def getOtherProbableParticipants: ListSet[ParticipantId]
	}

	//// RESPONSE TO CLIENT

	/** The response to a client command. */
	sealed trait ResponseToClient

	/** The command was appended to a majority of the participants persistent logs, and applied to the leader's state machine which responded with the specified [[content]]. */
	final case class Processed(recordIndex: RecordIndex, content: StateMachineResponse) extends ResponseToClient

	/** The client has to repeat the command to the first participant in [[otherParticipants]] (the leader followed by this participant), or fall back to the remaining participants in order. */
	final case class RedirectTo(otherParticipants: ListSet[ParticipantId]) extends ResponseToClient

	/** Indicates the participant cannot process the command because its state is non-leading or isolated.
	 * Upon receiving this response, the client should retry the request with one of the [[otherParticipants]] using the provided `nextAttemptFlag`.
	 * @param nextAttemptFlag The value the client must pass in the `attemptFlag` parameter of the command-delivery RPC method.
	 * @param otherParticipants A set of alternative participant identifiers for the client to attempt. */
	final case class Unable(nextAttemptFlag: CommandAttemptFlag, otherParticipants: ListSet[ParticipantId]) extends ResponseToClient
}
