package readren.matrix
package cluster.service

import cluster.*
import cluster.channel.CommonSerializers.given
import cluster.channel.Deserializer.DeserializationException
import cluster.channel.Nio2Serializers.given
import cluster.channel.{Deserializer, Serializer}
import cluster.service.Protocol.*
import cluster.service.ProtocolVersion
import cluster.service.ProtocolVersion.given

import java.net.SocketAddress

sealed trait Protocol

/** Command message an aspirant must send to all the participants it is aware of (initially the seeds) in order to:
 * 	- propagate the knowledge of its existence;
 * 	- be replied with [[JoinApprovalMembers]] if a cluster was already created;
 * 	- be replied with [[NoClusterIAmAwareOf]] if no cluster exists to participate in the election of the cluster creator.
 * 	@param myContactAddress the [[ContactAddress]] of the sender.
 * 	@param versionsISupport the set of versions supported by the sender.
 * 	@param cardsOfOtherParticipantsIKnow the [[ContactCard]]s of the participants known by the sender, not including itself. */
case class Hello(myContactAddress: ContactAddress, versionsISupport: Set[ProtocolVersion], cardsOfOtherParticipantsIKnow: Map[ContactAddress, Set[ProtocolVersion]]) extends Protocol

/**
 * Response to the [[Hello]] message that is sent by a participant to indicate that it does not support any of the [[ProtocolVersion]]s specified in the received [[Hello]] message.
 * */
case object SupportedVersionsMismatch extends Protocol

/**
 * Response to the [[Hello]] message when received by an aspirant that it not aware of the existence of a cluster.
 *
 * @param knowAspirantsCards the [[ContactCard]]s of the aspirants known by the sender, not including itself, and may include the receiver. */
case class NoClusterIAmAwareOf(knowAspirantsCards: Map[ContactAddress, Set[ProtocolVersion]]) extends Protocol


/** Message sent by an aspirant A to an aspirant B when A starts or stops proposing B should be the creator of the cluster.
 * An aspirant starts proposing a candidate when, from his viewpoint, he knows all other aspirants.
 * The chosen aspirant is the one with the highest [[ContactAddress]] that supports the newest [[ProtocolVersion]] among all the versions supported by the aspirants he knows.
 * @param proposedCandidate the [[ContactAddress]] of the cluster's creator candidate proposed by the sender.
 * @param supportedVersions the [[ProtocolVersion]]s supported by the proposed candidate. Uses only if the receiver didn't know about the proposed candidate.
 * */
case class ClusterCreatorProposal(proposedCandidate: ContactAddress, supportedVersions: Set[ProtocolVersion]) extends Protocol


/** Informs that the sender has created a cluster.
 * This message is sent to all known aspirants after having created a cluster, which happens after all aspirants known by the sender have sent [[ClusterCreatorProposal]] message to the sender.
 *
 * @param recommendedWaitTime the recommended time that the receiver should wait before starting the join process with [[Hello]]. */
case class ICreatedACluster(recommendedWaitTime: DurationMillis) extends Protocol


/** Response to the [[Hello]] message when received by a participant, that is aware of the existence of a cluster.
 *
 * @param membersCards The contact cards of the set of members that must be consulted for approval to join the cluster.
 * This value is greater than zero when another aspirant (which may include the responding participant) is currently in the process of joining.
 */
case class JoinApprovalMembers(membersCards: Map[ContactAddress, Set[ProtocolVersion]]) extends Protocol


/** Command message that the aspirant has to send to each member of the cluster to get its permission to join. */
case class RequestApprovalToJoin() extends Protocol


/** Response to the [[RequestApprovalToJoin]]
 *
 * @param joinToken a value that constates the responding member approves the join request. */
case class JoinApprovalGranted(joinToken: JoinToken) extends Protocol


/** Command message that an aspirant has to send to any member in order to join the cluster.
 *
 * @param joinTokenByParticipantAddress the join-token provided by each approving member. */
case class RequestToJoin(joinTokenByParticipantAddress: Map[ContactAddress, JoinToken]) extends Protocol


/** Response to the [[RequestToJoin]] message when the join is successful.
 * @param participantInfoByItsAddress the state of all the participant according to the sender, including hiw own view of himself (which is the single source of that information)
 */
case class JoinGranted(participantInfoByItsAddress: Map[ContactAddress, MyInfoAboutOtherParticipant]) extends Protocol

/** Response to the [[RequestToJoin]] message when the join is not successful.
 *
 * @param reason motive of the rejection. */
case class JoinRejected(youHaveToRetry: Boolean, reason: String) extends Protocol

/** Message that a participant should send to as many other participants as possible before closing its communication channels. */
case object IAmLeaving extends Protocol

/** Message that a participant should send to all other participants it knows when it notices the communication between it and one or more other participants is not working properly. */
case class ILostCommunicationWith(stateId: RingSerial, participantsAddresses: Set[ContactAddress]) extends Protocol

/** Message that a participant A should send to all other participants when a participant B sends him the [[Hello]] message a second time, which is a symptom that B has restarted. */
case class ParticipantHasBeenRestarted(restartedParticipantAddress: ContactAddress) extends Protocol

/** Message that every participant sends to every other participant it knows to verify the communication channel that connects them is working. */
case class Heartbeat(delayUntilNextHeartbeat: DurationMillis) extends Protocol

/** Message that every participant must send when his state, or his viewpoint of the state of other participant, changes.
 *
 * @param participants the state of all the participant according to the sender, including hiw own view of himself (which is the single source of that information) */
case class StateChanged(participants: Map[ContactAddress, MyInfoAboutOtherParticipant]) extends Protocol


object Protocol {

	type ContactAddress = SocketAddress
	type JoinToken = Long
	/** Wait time in milliseconds */
	type DurationMillis = Int
	/** Milliseconds since 1970-01-01T00:00:00Z */
	type Instant = Long

	sealed trait MembershipStatus

	case object Aspirant extends MembershipStatus

	case object Member extends MembershipStatus

	sealed trait MyInfoAboutOtherParticipant

	case object Incompatible extends MyInfoAboutOtherParticipant

	case object Unreachable extends MyInfoAboutOtherParticipant

	case class Connected(hisMembershipAccordingToMe: MembershipStatus, hisViewpoint: ParticipantViewpoint) extends MyInfoAboutOtherParticipant

	case class ParticipantViewpoint(stateSerial: RingSerial, lastChangeInstant: Instant, membership: MembershipStatus, communicationStatusByParticipants: Map[ContactAddress, CommunicationStatus])

	enum CommunicationStatus {
		case connected, incompatible, unreachable
	}

	/**
	 * A cross-version contact information card that a participant use to share and propagate its existence among other participants.
	 *
	 * A `ContactCard` consist of the contact address of a participant and the set protocol-versions it supports.
	 * This enables backward-compatible communication between participants running different versions of the cluster service.
	 *
	 * Participants share their `ContactCard` with others to allow discovery and propagation of their presence.
	 */
	type ContactCard = (ContactAddress, Set[ProtocolVersion])

	extension (contactCard: ContactCard) {
		inline def address: ContactAddress = contactCard._1
		inline def supportedVersions: Set[ProtocolVersion] = contactCard._2
	}

	given Serializer[Protocol] = (message: Protocol, writer: Serializer.Writer) => {
		message match {
			case hello: Hello =>
				writer.putByte(DISCRIMINATOR_Hello)
				writer.writeFull(hello)

			case SupportedVersionsMismatch =>
				writer.putByte(DISCRIMINATOR_NoClusterIAmAwareOf)

			case noAware: NoClusterIAmAwareOf =>
				writer.putByte(DISCRIMINATOR_NoClusterIAmAwareOf)
				writer.writeFull(noAware)

			case ysc: ClusterCreatorProposal =>
				writer.putByte(DISCRIMINATOR_YouShouldCreateTheCluster)
				writer.writeFull(ysc)

			case icc: ICreatedACluster =>
				writer.putByte(DISCRIMINATOR_ICreatedACluster)
				writer.writeFull(icc)

			case jam: JoinApprovalMembers =>
				writer.putByte(DISCRIMINATOR_JoinApprovalMembers)
				writer.writeFull(jam)

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

			case phr: ParticipantHasBeenRestarted =>
				writer.putByte(DISCRIMINATOR_ParticipantHasBeenRestarted)
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
			case DISCRIMINATOR_NoClusterIAmAwareOf =>
				reader.read[NoClusterIAmAwareOf]
			case DISCRIMINATOR_YouShouldCreateTheCluster =>
				reader.read[ClusterCreatorProposal]
			case DISCRIMINATOR_ICreatedACluster =>
				reader.read[ICreatedACluster]
			case DISCRIMINATOR_JoinApprovalMembers =>
				reader.read[JoinApprovalMembers]
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
			case DISCRIMINATOR_ParticipantHasBeenRestarted =>
				reader.read[ParticipantHasBeenRestarted]

			case x =>
				throw new DeserializationException(reader.position, s"Invalid discriminator value ($x) for the Protocol type: no matching product type found")
		}
	}

	inline val DISCRIMINATOR_Hello = 0
	inline val DISCRIMINATOR_SupportedVersionsMismatch = 12
	inline val DISCRIMINATOR_NoClusterIAmAwareOf = -1
	inline val DISCRIMINATOR_YouShouldCreateTheCluster = -2
	inline val DISCRIMINATOR_ICreatedACluster = -3
	inline val DISCRIMINATOR_JoinApprovalMembers = 1
	inline val DISCRIMINATOR_RequestApprovalToJoin = 2
	inline val DISCRIMINATOR_JoinApprovalGranted = 3
	inline val DISCRIMINATOR_RequestToJoin = 4
	inline val DISCRIMINATOR_JoinGranted = 5
	inline val DISCRIMINATOR_JoinRejected = 7
	inline val DISCRIMINATOR_I_AM_LEAVING = 8
	inline val DISCRIMINATOR_ILostCommunicationWith = 9
	inline val DISCRIMINATOR_Heartbeat = 10
	inline val DISCRIMINATOR_StateChanged = 11
	inline val DISCRIMINATOR_ParticipantHasBeenRestarted = 13


	given Serializer[Hello] = (message: Hello, writer: Serializer.Writer) => ???

	given Serializer[JoinApprovalMembers] = (message: JoinApprovalMembers, writer: Serializer.Writer) => ???

	given Serializer[NoClusterIAmAwareOf] = (message: NoClusterIAmAwareOf, writer: Serializer.Writer) => {
		writer.putByte(DISCRIMINATOR_NoClusterIAmAwareOf)
		writer.write(message.knowAspirantsCards)
	}

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

	given Serializer[ParticipantHasBeenRestarted] = (message: ParticipantHasBeenRestarted, writer: Serializer.Writer) => ???


	given Deserializer[Hello] = (reader: Deserializer.Reader) => ???

	given Deserializer[JoinApprovalMembers] = (reader: Deserializer.Reader) => ???

	given Deserializer[NoClusterIAmAwareOf] = (reader: Deserializer.Reader) => ???

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

	given Deserializer[ParticipantHasBeenRestarted] = (reader: Deserializer.Reader) => ???
}