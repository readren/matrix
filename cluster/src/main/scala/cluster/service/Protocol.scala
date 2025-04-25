package readren.matrix
package cluster.service

import cluster.*
import cluster.channel.CommonSerializers.given
import cluster.channel.Deserializer.DeserializationException
import cluster.channel.Nio2Serializers.given
import cluster.channel.{Deserializer, Serializer}
import cluster.service.Protocol.*
import cluster.service.Protocol.MembershipStatus.{ASPIRANT, MEMBER, UNKNOWN}
import cluster.service.ProtocolVersion
import cluster.service.ProtocolVersion.given

import readren.taskflow.Maybe

import java.net.SocketAddress
import java.nio.channels.AsynchronousSocketChannel

sealed trait Protocol

/** Command message an aspirant must send to the participants it connects to (initially the seeds) in order to:
 * 	- propagate the knowledge of the existence of other participants it knows;
 * 	- be replied with [[Welcome]] if a cluster was already created;
 * 	- be replied with [[NoClusterIAmAwareOf]] if no cluster exists to participate in the election of the cluster creator.
 * 	The receiver may assume that the sender is an aspirant.
 * 	@param versionsISupport the set of versions supported by the sender.
 * 	@param otherParticipantsIKnow the address of the participants known by the sender, not including itself. */
case class Hello(versionsISupport: Set[ProtocolVersion], otherParticipantsIKnow: Set[ContactAddress]) extends Protocol

/** First message that a participant must send to the peer after a successful reconnection to allow the peer to update his viewpoint of the sender.   */
case class IHaveReconnected(versionsISupport: Set[ProtocolVersion], myMembershipStatus: MembershipStatus) extends Protocol

/**
 * Response to the [[Hello]] message that is sent by a participant to indicate that it does not support any of the [[ProtocolVersion]]s specified in the received [[Hello]] message.
 * */
case object SupportedVersionsMismatch extends Protocol

/** Response to the [[Hello]] message when there is version compatibility.
 *
 * @param membershipStatus the [[MembershipStatus]] of the sender.
 * @param supportedVersions the versions supported by the sender.
 * @param otherParticipants the participants known by the sender, excluding itself.
 */
case class Welcome(membershipStatus: MembershipStatus, supportedVersions: Set[ProtocolVersion], otherParticipants: Set[ContactAddress]) extends Protocol

/** Message sent by an aspirant A to an aspirant B when A starts or stops proposing B to be the creator of the cluster.
 * An aspirant starts proposing a candidate when, from his viewpoint, he is communicated to all the seeds he knows and the connection to all the others he knows is completed (successfully or not).
 * The chosen aspirant is the one with the lowest [[ContactAddress]] that supports the newest [[ProtocolVersion]] among all the versions supported by the aspirants he knows.
 * @param proposedCandidate the [[ContactAddress]] of the cluster's creator candidate proposed by the sender.
 * */
case class ClusterCreatorProposal(proposedCandidate: ContactAddress | Null) extends Protocol

/** Informs that the sender has created a cluster.
 * This message is sent to all known aspirants after having created a cluster, which happens after all aspirants known by the sender have sent [[ClusterCreatorProposal]] message to the sender proposing him.
 *
 * @param myViewpoint the [[MemberViewpoint]]s of the sender, which is the cluster creator. */
case class ICreatedACluster(myViewpoint: MemberViewpoint) extends Protocol

/** Command message that the aspirant has to send to each member of the cluster to get its permission to join. */
@deprecated
case class RequestApprovalToJoin() extends Protocol

/** Response to the [[RequestApprovalToJoin]]
 *
 * @param joinToken a value that constates the responding member approves the join request. */
@deprecated
case class JoinApprovalGranted(joinToken: JoinToken) extends Protocol


/** Command message that an aspirant has to send to any member in order to join the cluster.
 *
 * @param joinTokenByMemberAddress the join-token provided by each approving member. */
case class RequestToJoin(joinTokenByMemberAddress: Map[ContactAddress, JoinToken]) extends Protocol


/** Response to the [[RequestToJoin]] message when the join is successful.
 * @param participantInfoByItsAddress the state of all the participant according to the sender, including hiw own view of himself (which is the single source of that information)
 */
case class JoinGranted(clusterCreationInstant: Instant, participantInfoByItsAddress: Map[ContactAddress, ParticipantInfo]) extends Protocol

/** Response to the [[RequestToJoin]] message when the join is not successful.
 *
 * @param reason motive of the rejection. */
case class JoinRejected(youHaveToRetry: Boolean, reason: String) extends Protocol

/** Response to [[ClusterCreatorProposal]] when the receiver knows of the existence of a cluster, and to [[RequestToJoin]] when the receiver's is an aspirant.
*/
case class ResolveAspirantMembershipConflict(myMembershipStatus: MembershipStatus, versionsISupport: Set[ProtocolVersion], membershipStatusOfOtherParticipantsIKnow: Map[ContactAddress, MembershipStatus]) extends Protocol

/** The message that a participant should send to as many other participants as possible before closing its communication channels. */
case object IAmLeaving extends Protocol

/** The message that a participant should send to all other participants it knows when it notices the communication between it and one or more other participants is not working properly. */
case class ILostCommunicationWith(participantsAddress: ContactAddress) extends Protocol

case class ConversationStartedWith(participantAddress: ContactAddress) extends Protocol

/** Message that a participant A should send to all other participants when a participant B sends him the [[Hello]] message a second time, which is a symptom that B has restarted. */
case class AnotherParticipantHasBeenRestarted(restartedParticipantAddress: ContactAddress) extends Protocol

/** Message that every participant sends to every other participant it knows to verify the communication channel that connects them is working. */
case class Heartbeat(delayUntilNextHeartbeat: DurationMillis) extends Protocol

/** A message that every participant must send when his state, or his viewpoint of the state of other participant, changes.
 *
 * @param participantInfoByAddress the information, according to the sender, about each participant by its address, including the sender. */
case class StateChanged(serial: RingSerial, takenOn: Instant, participantInfoByAddress: Map[ContactAddress, ParticipantInfo]) extends Protocol

/** The message that a participant's delegate sends to inform the peer delegate that he called [[AsynchronousSocketChannel.shutdownInput]] because the channel was discarded due to duplicate connections.
 * @param isClientSide `true` when the sender is at the client side of the discarded channel; `false` otherwise. */
case class ChannelDiscarded(isClientSide: Boolean) extends Protocol

case class WeHaveToResolveBrainJoin(myViewPoint: MemberViewpoint) extends Protocol
case class WeHaveToResolveBrainSplit(myViewPoint: MemberViewpoint) extends Protocol

case class ApplicationMsg(bytes: Array[Byte]) extends Protocol

object Protocol {

	type ContactAddress = SocketAddress
	type JoinToken = Long
	/** Wait time in milliseconds */
	type DurationMillis = Int
	/** Milliseconds since 1970-01-01T00:00:00Z */
	type Instant = Long

	enum MembershipStatus {
		case UNKNOWN, ASPIRANT, MEMBER
	}
	
	enum IncommunicabilityReason {
		case IS_CONNECTING_AS_CLIENT, IS_INCOMPATIBLE
	}

	/**
	 * Information that tells how a participant sees another participant
	 * @param communicationStatus the communication status that the sender of this information has toward the referred participant
	 * @param membershipStatus the membership status of the referred participant according to the sender of this information                           */
	case class ParticipantInfo(supportedVersions: Set[ProtocolVersion], communicationStatus: CommunicationStatus, membershipStatus: MembershipStatus)	
	/** Information about a participant according to itself.
	 * This information has a single source of truth: the peer. */
	case class MemberViewpoint(serial: RingSerial, takenOn: Instant, clusterCreationInstant: Instant, participantsInfo: Map[ContactAddress, ParticipantInfo])

	enum CommunicationStatus {
		case HANDSHOOK, CONNECTED, CONNECTING, INCOMPATIBLE, UNREACHABLE
	}

	given Serializer[Protocol] = (message: Protocol, writer: Serializer.Writer) => {
		message match {
			case hello: Hello =>
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

			case IAmLeaving =>
				writer.putByte(DISCRIMINATOR_I_AM_LEAVING)

			case hb: Heartbeat =>
				writer.putByte(DISCRIMINATOR_Heartbeat)
				writer.writeFull(hb)

			case sc: StateChanged =>
				writer.putByte(DISCRIMINATOR_StateChanged)
				writer.writeFull(sc)

			case phr: AnotherParticipantHasBeenRestarted =>
				writer.putByte(DISCRIMINATOR_AnotherParticipantHasBeenRestarted)
				writer.writeFull(phr)

		}
	}

	given Deserializer[Protocol] = (reader: Deserializer.Reader) => {
		reader.readByte() match {
			case DISCRIMINATOR_Heartbeat =>
				reader.read[Heartbeat]
			case DISCRIMINATOR_StateChanged =>
				reader.read[StateChanged]
			case DISCRIMINATOR_Hello =>
				reader.read[Hello]
			case DISCRIMINATOR_SupportedVersionsMismatch =>
				SupportedVersionsMismatch
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
				reader.read[AnotherParticipantHasBeenRestarted]

			case x =>
				throw new DeserializationException(reader.position, s"Invalid discriminator value ($x) for the Protocol type: no matching product type found")
		}
	}

	inline val DISCRIMINATOR_Hello = 0
	inline val DISCRIMINATOR_SupportedVersionsMismatch = 12
	inline val DISCRIMINATOR_Welcome = -1
	inline val DISCRIMINATOR_YouShouldCreateTheCluster = -2
	inline val DISCRIMINATOR_ICreatedACluster = -3
	inline val DISCRIMINATOR_RequestApprovalToJoin = 2
	inline val DISCRIMINATOR_JoinApprovalGranted = 3
	inline val DISCRIMINATOR_RequestToJoin = 4
	inline val DISCRIMINATOR_JoinGranted = 5
	inline val DISCRIMINATOR_JoinRejected = 7
	inline val DISCRIMINATOR_I_AM_LEAVING = 8
	inline val DISCRIMINATOR_ILostCommunicationWith = 9
	inline val DISCRIMINATOR_Heartbeat = 10
	inline val DISCRIMINATOR_StateChanged = 11
	inline val DISCRIMINATOR_AnotherParticipantHasBeenRestarted = 13


	given Serializer[Hello] = (message: Hello, writer: Serializer.Writer) => ???

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

	given Serializer[StateChanged] = (message: StateChanged, writer: Serializer.Writer) => ???

	given Serializer[AnotherParticipantHasBeenRestarted] = (message: AnotherParticipantHasBeenRestarted, writer: Serializer.Writer) => ???


	given Deserializer[Hello] = (reader: Deserializer.Reader) => ???

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

	given Deserializer[StateChanged] = (reader: Deserializer.Reader) => ???

	given Deserializer[AnotherParticipantHasBeenRestarted] = (reader: Deserializer.Reader) => ???
}