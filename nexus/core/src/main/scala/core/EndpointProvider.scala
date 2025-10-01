package readren.nexus
package core

class EndpointProvider[-U](receiver: Receiver[U]) {

	/** Thread-safe */
	def local[M <: U]: Endpoint[M] = LocalEndpoint(receiver)

	/** Thread-safe */
	def remote[M <: U]: Endpoint[M] = RemoteEndpoint(receiver.uri)

	/** Thread-safe */
	def forSpuron[M <: U](otherSpuronEndpoint: Endpoint[?]): Endpoint[M] = {
		if otherSpuronEndpoint.isLocal then LocalEndpoint(receiver)
		else RemoteEndpoint(receiver.uri)
	}
}
