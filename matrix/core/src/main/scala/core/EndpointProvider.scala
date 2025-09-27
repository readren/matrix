package readren.matrix
package core

class EndpointProvider[-U](receiver: Receiver[U]) {

	/** Thread-safe */
	def local[M <: U]: Endpoint[M] = LocalEndpoint(receiver)

	/** Thread-safe */
	def remote[M <: U]: Endpoint[M] = RemoteEndpoint(receiver.uri)

	/** Thread-safe */
	def forReactant[M <: U](otherReactantEndpoint: Endpoint[?]): Endpoint[M] = {
		if otherReactantEndpoint.isLocal then LocalEndpoint(receiver)
		else RemoteEndpoint(receiver.uri)
	}
}
