package readren.matrix
package cluster.service

import cluster.*
import cluster.serialization.MacroTools.showCode
import cluster.serialization.NestedSumMatchMode.FLAT
import cluster.serialization.{Deserializer, DiscriminationCriteria, ProtocolVersion, Serializer}
import cluster.service.Protocol.*

import readren.taskflow.SchedulingExtension.MilliDuration

import java.net.SocketAddress
import scala.compiletime.erasedValue

sealed trait Protocol

/** A message sent to a peer that is self-initiated, rather than being a response to a prior message from that peer. */
sealed trait InitiationMsg extends Protocol

/** A message that spontaneously affirms a fact without expecting a response. */
sealed trait Affirmation extends InitiationMsg

/** A message that asks for a response from the receiver, but may affirm something too.  */
sealed trait Request extends InitiationMsg {
	val requestId: RequestId
}
/** A message that responds a [[Request]] message to that [[Request]]'s sender. */
sealed trait Response extends Protocol {
	val toRequest: RequestId
}

/** First message that a delegate must send to the peer after the first successful connection to it. Its purpose is to:
 * 	- propagate the knowledge of its existence to the participants it knows;
 * 	- be replied with [[Welcome]] if there is version compatibility or with [[SupportedVersionsMismatch]] otherwise.
 * 	The receiver may assume that the sender is an aspirant.
 * 	@param versionsISupport the set of versions supported by the sender.
 * 	@param otherParticipantsIKnow the address of the participants known by the sender, not including itself. */
case class HelloIExist(override val requestId: RequestId, versionsISupport: Set[ProtocolVersion], myCreationInstant: Instant, otherParticipantsIKnow: Set[ContactAddress]) extends Request

/** First message that a participant must send to the peer after a successful reconnection to allow the peer to update his viewpoint of the sender.
 * Aspirants reply with [[Welcome]] if there is version compatibility or with [[SupportedVersionsMismatch]] otherwise.
 * The reply from members depends on the sender membership. */
case class HelloIAmBack(override val requestId: RequestId, versionsISupport: Set[ProtocolVersion], myMembershipStatus: MembershipStatus, myCreationInstant: Instant) extends Request

/**
 * Response to the [[HelloIExist]] message that is sent by a participant to indicate that it does not support any of the [[ProtocolVersion]]s specified in the received [[HelloIExist]] message.
 * */
case class SupportedVersionsMismatch(override val toRequest: RequestId) extends Response

/** Response to the [[HelloIExist]] and [[HelloIAmBack]] messages when there is version compatibility.
 *
 * @param myMembershipStatus the [[MembershipStatus]] of the sender.
 * @param versionsISupport the versions supported by the sender.
 * @param otherParticipants the participants known by the sender, excluding itself.
 */
case class Welcome(override val toRequest: RequestId, myMembershipStatus: MembershipStatus, versionsISupport: Set[ProtocolVersion], myCreationInstant: Instant, otherParticipants: Set[ContactAddress]) extends Response

/** Message sent by an aspirant A to an aspirant B when A starts or stops proposing B to be the creator of the cluster.
 * An aspirant starts proposing a candidate when, from his viewpoint, he is communicated to all the seeds he knows and the connection to all the others he knows is completed (successfully or not).
 * The chosen aspirant is the one with the lowest [[ContactAddress]] that supports the newest [[ProtocolVersion]] among all the versions supported by the aspirants he knows.
 * @param proposedCandidate the [[ContactAddress]] of the cluster's creator candidate proposed by the sender.
 * */
case class ClusterCreatorProposal(proposedCandidate: ContactAddress | Null) extends Affirmation

/** Informs that the sender has created a cluster.
 * This message is sent to all known aspirants after having created a cluster, which happens after all aspirants known by the sender have sent [[ClusterCreatorProposal]] message to the sender proposing him.
 *
 * @param myViewpoint the [[MemberViewpoint]]s of the sender, which is the cluster creator. */
case class ICreatedACluster(myViewpoint: MemberViewpoint) extends Affirmation

/** Command message that the aspirant has to send to each member of the cluster to get its permission to join. */
@deprecated
case class RequestApprovalToJoin(override val requestId: RequestId) extends Request

/** Response to the [[RequestApprovalToJoin]]
 *
 * @param joinToken a value that constates the responding member approves the join request. */
@deprecated
case class JoinApprovalGranted(override val toRequest: RequestId, joinToken: JoinToken) extends Response


/** Command message that an aspirant has to send to any member in order to join the cluster.
 *
 * @param joinTokenByMemberAddress the join-token provided by each approving member. */
case class RequestToJoin(override val requestId: RequestId, joinTokenByMemberAddress: Map[ContactAddress, JoinToken]) extends Request


/** Response to the [[RequestToJoin]] message when the join is successful.
 * @param participantInfoByItsAddress the state of all the participant according to the sender, including his own view of himself (which is the single source of that information)
 */
case class JoinGranted(override val toRequest: RequestId, clusterCreationInstant: Instant, participantInfoByItsAddress: Map[ContactAddress, ParticipantInfo]) extends Response

/** Response to the [[RequestToJoin]] message when the join is not successful.
 *
 * @param reason motive of the rejection. */
case class JoinRejected(override val toRequest: RequestId, youHaveToRetry: Boolean, reason: String) extends Response

/**
 * Informs the receiver the membership-status of the sender and reveals his memory of the membership-status of other participants.
 * Sent by participant A to participant B when A notices that B's memory of the membership-status of A is incorrect. For example, it is sent by the receiver of a [[ClusterCreatorProposal]] when he knows of the existence of a cluster, and by the receiver of a [[RequestToJoin]] when he is an aspirant.
 * @param membershipStatusOfParticipantsIKnow the [[MembershipStatus]] of the participants that the sender knows according to him, including the receiver.
 */
case class WaitMyMembershipStatusIs(myMembershipStatus: MembershipStatus, membershipStatusOfParticipantsIKnow: Map[ContactAddress, MembershipStatus]) extends Affirmation

/** The message that a participant should send to as many other participants as possible before leaving the network or shooting down. */
case class Farewell(myCreationInstant: Instant) extends Affirmation

case class AnotherParticipantGone(goneParticipantAddress: ContactAddress, goneParticipantCreationInstant: Instant) extends Affirmation

case class AreYouInSyncWithMe(override val requestId: RequestId, myMembershipStatus: MembershipStatus, yourMembershipStatusAccordingToMe: MembershipStatus) extends Request

case class AreWeInSyncResponse(override val toRequest: RequestId, yourMembershipStatusAccordingToMeMatches: Boolean) extends Response

/** The message that a participant should send to all other participants it knows when it notices the communication between it and one or more other participants is not working properly. */
case class ILostCommunicationWith(participantsAddress: ContactAddress) extends Affirmation

case class ConversationStartedWith(participantAddress: ContactAddress) extends Affirmation

/** Message that a participant A should send to all other participants when a participant B sends him the [[HelloIExist]] message a second time, which is a symptom that B has restarted. */
case class AnotherParticipantHasBeenRebooted(restartedParticipantAddress: ContactAddress, restartedParticipantCreationInstant: Instant) extends Affirmation

/** The message that every participant sends to every other participant it knows to verify the communication channel that connects them is working. */
case class Heartbeat(delayUntilNextHeartbeat: MilliDuration) extends Affirmation

/** A message that every participant must send when his state, or his viewpoint of another participant's state, changes.
 *
 * @param participantInfoByAddress the information, according to the sender, about each participant by its address, including the sender. */
case class ClusterStateChanged(serial: RingSerial, takenOn: Instant, participantInfoByAddress: Map[ContactAddress, ParticipantInfo]) extends Affirmation


/** The message that a participant's delegate sends to inform the peer delegate that he called [[AsynchronousSocketChannel.shutdownInput]] because the channel was discarded due to duplicate connections.
 * @param isClientSide `true` when the sender is at the client side of the discarded channel; `false` otherwise. */
case class ChannelDiscarded(isClientSide: Boolean) extends Affirmation

case class WeHaveToResolveBrainJoin(myViewPoint: MemberViewpoint) extends Affirmation

case class WeHaveToResolveBrainSplit(myViewPoint: MemberViewpoint) extends Affirmation

case class ApplicationMsg(bytes: Array[Byte]) extends Affirmation


object Protocol {
	import cluster.channel.Nio2Serializers.given

	import serialization.CommonSerializers.given

	type ContactAddress = SocketAddress
	type JoinToken = Long
	/** Milliseconds since 1970-01-01T00:00:00Z */
	type Instant = Long
	type RequestId = Int

	val UNSPECIFIED_INSTANT: Instant = Long.MaxValue

	enum MembershipStatus {
		case UNKNOWN, ASPIRANT, MEMBER
	}
	given Serializer[MembershipStatus] = Serializer.derive[MembershipStatus](FLAT)
	given Deserializer[MembershipStatus] = Deserializer.derive[MembershipStatus](FLAT)

	enum IncommunicabilityReason {
		case IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE
	}
	given Serializer[IncommunicabilityReason] = Serializer.derive[IncommunicabilityReason](FLAT)
	given Deserializer[IncommunicabilityReason] = Deserializer.derive[IncommunicabilityReason](FLAT)

	/**
	 * Information that tells how a participant sees another participant
	 * @param communicationStatus the communication status that the sender of this information has toward the referred participant
	 * @param membershipStatus the membership status of the referred participant according to the sender of this information */
	case class ParticipantInfo(communicationStatus: CommunicationStatus, membershipStatus: MembershipStatus)
	given Serializer[ParticipantInfo] = Serializer.derive[ParticipantInfo](FLAT)
	given Deserializer[ParticipantInfo] = Deserializer.derive[ParticipantInfo](FLAT)

	/** Information about a participant according to itself.
	 * This information has a single source of truth: the peer. */
	case class MemberViewpoint(serial: RingSerial, takenOn: Instant, clusterCreationInstant: Instant, participantsInfo: Map[ContactAddress, ParticipantInfo])
	given Serializer[MemberViewpoint] = Serializer.derive[MemberViewpoint](FLAT)
	given Deserializer[MemberViewpoint] = Deserializer.derive[MemberViewpoint](FLAT)

	enum CommunicationStatus {
		case HANDSHOOK, CONNECTED, CONNECTING, INCOMPATIBLE, UNREACHABLE
	}
	given Serializer[CommunicationStatus] = Serializer.derive[CommunicationStatus](FLAT)
	given Deserializer[CommunicationStatus] = Deserializer.derive[CommunicationStatus](FLAT)

	private val protocolSerializer: Serializer[Protocol] = showCode(Serializer.derive[Protocol](FLAT))
	given Serializer[Protocol] = protocolSerializer

	private val protocolDeserializer: Deserializer[Protocol] = showCode(Deserializer.derive[Protocol](FLAT))
	given Deserializer[Protocol] = protocolDeserializer


	object protocolDc extends DiscriminationCriteria[Protocol] {
		override transparent inline def discriminator[P <: Protocol]: RequestId =
			inline erasedValue[P] match {
				case _: HelloIExist => 10
				case _: HelloIAmBack => 11
				case _: SupportedVersionsMismatch => 13
				case _: Welcome => 14
				case _: ClusterCreatorProposal => 15
				case _: ICreatedACluster => 16
				case _: RequestApprovalToJoin => 17
				case _: JoinApprovalGranted => 18
				case _: RequestToJoin => 19
				case _: JoinGranted => 20
				case _: JoinRejected => 21
				case _: WaitMyMembershipStatusIs => 22
				case _: Farewell => 23
				case _: AnotherParticipantGone => 24
				case _: AreYouInSyncWithMe => 25
				case _: AreWeInSyncResponse => 26
				case _: ILostCommunicationWith => 27
				case _: ConversationStartedWith => 28
				case _: AnotherParticipantHasBeenRebooted => 29
				case _: Heartbeat => 30
				case _: ClusterStateChanged => 31
				case _: ChannelDiscarded => 32
				case _: WeHaveToResolveBrainJoin => 33
				case _: WeHaveToResolveBrainSplit => 34
				case _: ApplicationMsg => 35

				case _: Affirmation => 3
				case _: Request => 2
				case _: Response => 1
				case _: InitiationMsg => 0
			}
	}

	transparent inline given DiscriminationCriteria[Protocol] = protocolDc

}