package readren.matrix

import readren.taskflow.Maybe

class EndpointProvider[-U](receiver: Receiver[U]) {
	def forReactant[M <: U](otherReactantEndpoint: Endpoint[?]): Endpoint[M] = {
		if otherReactantEndpoint.isLocal then LocalEndpoint(receiver)
		else RemoteEndpoint(receiver.uri) 
	}
	def forMyCreator[M<:U]: Endpoint[M] = LocalEndpoint(receiver)
}
