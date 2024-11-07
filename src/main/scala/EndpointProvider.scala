package readren.matrix

import readren.taskflow.Maybe

class EndpointProvider[-U](receiver: Receiver[U]) {

	def local[M <: U]: Endpoint[M] = LocalEndpoint(receiver)
	
	def remote[M <: U]: Endpoint[M] = RemoteEndpoint(receiver.uri)

	def forReactant[M <: U](otherReactantEndpoint: Endpoint[?]): Endpoint[M] = {
		if otherReactantEndpoint.isLocal then LocalEndpoint(receiver)
		else RemoteEndpoint(receiver.uri)
	}
}
