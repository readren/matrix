package readren.matrix
package cluster.service

import java.nio.channels.AsynchronousSocketChannel

/** The origin of the [[AsynchronousSocketChannel]] instance owned by instances of [[CommunicableDelegate]]. */
enum ChannelOrigin {
	/** The channel was initiated by the [[ParticipantService]] that owns the [[CommunicableDelegate]]. It is on the client side. */
	case INITIATED
	/** The channel was accepted by the [[ParticipantService]] that owns the [[CommunicableDelegate]]). It is on the server side. */
	case ACCEPTED
}