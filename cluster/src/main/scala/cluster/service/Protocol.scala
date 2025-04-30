package readren.matrix
package cluster.service

import cluster.*
import cluster.channel.Deserializer.DeserializationException
import cluster.channel.{Deserializer, Serializer}
import cluster.service.Protocol.*
import cluster.service.ProtocolVersion

import readren.taskflow.SchedulingExtension.MilliDuration

import java.net.SocketAddress

sealed trait Protocol

sealed trait InitiationMsg extends Protocol

sealed trait Affirmation extends InitiationMsg

sealed trait Request extends InitiationMsg {
	val requestId: RequestId
}

sealed trait Response extends Protocol {
	val toRequest: RequestId
}

/** First message that a delegate must send to the peer after the first successful connection to:
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

/** Response to the [[HelloIExist]] message when there is version compatibility.
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
 * @param participantInfoByItsAddress the state of all the participant according to the sender, including hiw own view of himself (which is the single source of that information)
 */
case class JoinGranted(override val toRequest: RequestId, clusterCreationInstant: Instant, participantInfoByItsAddress: Map[ContactAddress, ParticipantInfo]) extends Response

/** Response to the [[RequestToJoin]] message when the join is not successful.
 *
 * @param reason motive of the rejection. */
case class JoinRejected(override val toRequest: RequestId, youHaveToRetry: Boolean, reason: String) extends Response

/** Response to [[ClusterCreatorProposal]] when the receiver knows of the existence of a cluster, and to [[RequestToJoin]] when the receiver's is an aspirant.
 * @param membershipStatusOfParticipantsIKnow the [[MembershipStatus]] of the participants that the sender knows, including the receiver.
 */
case class ResolveMembershipConflict(myMembershipStatus: MembershipStatus, membershipStatusOfParticipantsIKnow: Map[ContactAddress, MembershipStatus]) extends Affirmation

/** The message that a participant should send to as many other participants as possible before leaving the network or shooting down. */
case class Farewell(myCreationInstant: Instant) extends Affirmation

case class AnotherParticipantGone(goneParticipantAddress: ContactAddress, goneParticipantCreationInstant: Instant) extends Affirmation

case class AreYouInSyncWithMe(override val requestId: RequestId, myMembershipStatus: MembershipStatus, yourMembershipStatusAccordingToMe: MembershipStatus) extends Request

case class YesImAInSyncWithYou(override val toRequest: RequestId) extends Response

/** The message that a participant should send to all other participants it knows when it notices the communication between it and one or more other participants is not working properly. */
case class ILostCommunicationWith(participantsAddress: ContactAddress) extends Affirmation

case class ConversationStartedWith(participantAddress: ContactAddress) extends Affirmation

/** Message that a participant A should send to all other participants when a participant B sends him the [[HelloIExist]] message a second time, which is a symptom that B has restarted. */
case class AnotherParticipantHasBeenRebooted(restartedParticipantAddress: ContactAddress, restartedParticipantCreationInstant: Instant) extends Affirmation

/** Message that every participant sends to every other participant it knows to verify the communication channel that connects them is working. */
case class Heartbeat(delayUntilNextHeartbeat: MilliDuration) extends Affirmation

/** A message that every participant must send when his state, or his viewpoint of the state of other participant, changes.
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

	type ContactAddress = SocketAddress
	type JoinToken = Long
	/** Milliseconds since 1970-01-01T00:00:00Z */
	type Instant = Long
	type RequestId = Int

	val UNSPECIFIED_INSTANT = Long.MaxValue

	enum MembershipStatus {
		case UNKNOWN, ASPIRANT, MEMBER
	}

	enum IncommunicabilityReason {
		case IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE
	}

	/**
	 * Information that tells how a participant sees another participant
	 * @param communicationStatus the communication status that the sender of this information has toward the referred participant
	 * @param membershipStatus the membership status of the referred participant according to the sender of this information */
	case class ParticipantInfo(supportedVersions: Set[ProtocolVersion], communicationStatus: CommunicationStatus, membershipStatus: MembershipStatus)

	/** Information about a participant according to itself.
	 * This information has a single source of truth: the peer. */
	case class MemberViewpoint(serial: RingSerial, takenOn: Instant, clusterCreationInstant: Instant, participantsInfo: Map[ContactAddress, ParticipantInfo])

	enum CommunicationStatus {
		case HANDSHOOK, CONNECTED, CONNECTING, INCOMPATIBLE, UNREACHABLE
	}

	given Serializer[Protocol] = (message: Protocol, writer: Serializer.Writer) => {
		message match {
			case hello: HelloIExist =>
				writer.putByte(DISCRIMINATOR_Hello)
				writer.writeFull(hello)

			case SupportedVersionsMismatch =>
				writer.putByte(DISCRIMINATOR_Welcome)

			case welcome: Welcome =>
				writer.putByte(DISCRIMINATOR_Welcome)
				writer.writeFull(welcome)

			case ysc: ClusterCreatorProposal =>
				writer.putByte(DISCRIMINATOR_YouShouldCreateTheCluster)
				writer.writeFull(ysc)

			case icc: ICreatedACluster =>
				writer.putByte(DISCRIMINATOR_ICreatedACluster)
				writer.writeFull(icc)

			case oi: RequestApprovalToJoin =>
				writer.putByte(DISCRIMINATOR_RequestApprovalToJoin)
				writer.writeFull(oi)

			case jag: JoinApprovalGranted =>
				writer.putByte(DISCRIMINATOR_JoinApprovalGranted)
				writer.writeFull(jag)

			case rtj: RequestToJoin =>
				writer.putByte(DISCRIMINATOR_RequestToJoin)
				writer.writeFull(rtj)

			case jg: JoinGranted =>
				writer.putByte(DISCRIMINATOR_JoinGranted)
				writer.writeFull(jg)

			case jr: JoinRejected =>
				writer.putByte(DISCRIMINATOR_JoinRejected)
				writer.writeFull(jr)

			case lcw: ILostCommunicationWith =>
				writer.putByte(DISCRIMINATOR_ILostCommunicationWith)
				writer.writeFull(lcw)

			case Farewell =>
				writer.putByte(DISCRIMINATOR_I_AM_LEAVING)

			case hb: Heartbeat =>
				writer.putByte(DISCRIMINATOR_Heartbeat)
				writer.writeFull(hb)

			case sc: ClusterStateChanged =>
				writer.putByte(DISCRIMINATOR_StateChanged)
				writer.writeFull(sc)

			case phr: AnotherParticipantHasBeenRebooted =>
				writer.putByte(DISCRIMINATOR_AnotherParticipantHasBeenRestarted)
				writer.writeFull(phr)

		}
	}

	given Deserializer[Protocol] = (reader: Deserializer.Reader) => {
		reader.readByte() match {
			case DISCRIMINATOR_Heartbeat =>
				reader.read[Heartbeat]
			case DISCRIMINATOR_StateChanged =>
				reader.read[ClusterStateChanged]
			case DISCRIMINATOR_Hello =>
				reader.read[HelloIExist]
			case DISCRIMINATOR_SupportedVersionsMismatch =>
				reader.read[SupportedVersionsMismatch]
			case DISCRIMINATOR_Welcome =>
				reader.read[Welcome]
			case DISCRIMINATOR_YouShouldCreateTheCluster =>
				reader.read[ClusterCreatorProposal]
			case DISCRIMINATOR_ICreatedACluster =>
				reader.read[ICreatedACluster]
			case DISCRIMINATOR_RequestApprovalToJoin =>
				reader.read[RequestApprovalToJoin]
			case DISCRIMINATOR_JoinApprovalGranted =>
				reader.read[JoinApprovalGranted]
			case DISCRIMINATOR_RequestToJoin =>
				reader.read[RequestToJoin]
			case DISCRIMINATOR_JoinGranted =>
				reader.read[JoinGranted]
			case DISCRIMINATOR_JoinRejected =>
				reader.read[JoinRejected]
			case DISCRIMINATOR_ILostCommunicationWith =>
				reader.read[ILostCommunicationWith]
			case DISCRIMINATOR_AnotherParticipantHasBeenRestarted =>
				reader.read[AnotherParticipantHasBeenRebooted]

			case x =>
				throw new DeserializationException(reader.position, s"Invalid discriminator value ($x) for the Protocol type: no matching product type found")
		}
	}

	inline val DISCRIMINATOR_Hello = 0
	inline val DISCRIMINATOR_SupportedVersionsMismatch = 1
	inline val DISCRIMINATOR_Welcome = 2
	inline val DISCRIMINATOR_YouShouldCreateTheCluster = 3
	inline val DISCRIMINATOR_ICreatedACluster = 4
	inline val DISCRIMINATOR_RequestApprovalToJoin = 5
	inline val DISCRIMINATOR_JoinApprovalGranted = 6
	inline val DISCRIMINATOR_RequestToJoin = 7
	inline val DISCRIMINATOR_JoinGranted = 8
	inline val DISCRIMINATOR_JoinRejected = 9
	inline val DISCRIMINATOR_I_AM_LEAVING = 10
	inline val DISCRIMINATOR_ILostCommunicationWith = 11
	inline val DISCRIMINATOR_Heartbeat = 12
	inline val DISCRIMINATOR_StateChanged = 13
	inline val DISCRIMINATOR_AnotherParticipantHasBeenRestarted = 14


	given Serializer[HelloIExist] = (message: HelloIExist, writer: Serializer.Writer) => ???

	given Serializer[SupportedVersionsMismatch] = (message: SupportedVersionsMismatch, writer: Serializer.Writer) => ???

	given Serializer[Welcome] = (message: Welcome, writer: Serializer.Writer) => ???

	given Serializer[ClusterCreatorProposal] = (message: ClusterCreatorProposal, writer: Serializer.Writer) => ???

	given Serializer[ICreatedACluster] = (message: ICreatedACluster, writer: Serializer.Writer) => ???

	given Serializer[RequestApprovalToJoin] = (message: RequestApprovalToJoin, writer: Serializer.Writer) => ???

	given Serializer[JoinApprovalGranted] = (message: JoinApprovalGranted, writer: Serializer.Writer) => ???

	given Serializer[RequestToJoin] = (message: RequestToJoin, writer: Serializer.Writer) => ???

	given Serializer[JoinGranted] = (message: JoinGranted, writer: Serializer.Writer) => ???

	given Serializer[JoinRejected] = (message: JoinRejected, writer: Serializer.Writer) => ???

	given Serializer[ILostCommunicationWith] = (message: ILostCommunicationWith, writer: Serializer.Writer) => ???

	given Serializer[Heartbeat] = (message: Heartbeat, writer: Serializer.Writer) => ???

	given Serializer[ClusterStateChanged] = (message: ClusterStateChanged, writer: Serializer.Writer) => ???

	given Serializer[AnotherParticipantHasBeenRebooted] = (message: AnotherParticipantHasBeenRebooted, writer: Serializer.Writer) => ???


	given Deserializer[HelloIExist] = (reader: Deserializer.Reader) => ???

	given Deserializer[SupportedVersionsMismatch] = (reader: Deserializer.Reader) => ???

	given Deserializer[Welcome] = (reader: Deserializer.Reader) => ???

	given Deserializer[ClusterCreatorProposal] = (reader: Deserializer.Reader) => ???

	given Deserializer[ICreatedACluster] = (reader: Deserializer.Reader) => ???

	given Deserializer[RequestApprovalToJoin] = (reader: Deserializer.Reader) => ???

	given Deserializer[JoinApprovalGranted] = (reader: Deserializer.Reader) => ???

	given Deserializer[RequestToJoin] = (reader: Deserializer.Reader) => ???

	given Deserializer[JoinGranted] = (reader: Deserializer.Reader) => ???

	given Deserializer[JoinRejected] = (reader: Deserializer.Reader) => ???

	given Deserializer[ILostCommunicationWith] = (reader: Deserializer.Reader) => ???

	given Deserializer[Heartbeat] = (reader: Deserializer.Reader) => ???

	given Deserializer[ClusterStateChanged] = (reader: Deserializer.Reader) => ???

	given Deserializer[AnotherParticipantHasBeenRebooted] = (reader: Deserializer.Reader) => ???
}