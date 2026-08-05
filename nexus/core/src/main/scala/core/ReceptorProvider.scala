package readren.nexus
package core

class ReceptorProvider[-U](receiver: Inqueue[U]) {

	/** Thread-safe */
	def local[M <: U]: Receptor[M] = LocalReceptor(receiver)

	/** Thread-safe */
	def remote[M <: U]: Receptor[M] = RemoteReceptor(receiver.uri)

	/** Thread-safe */
	def forOwnerOf[M <: U](foreignReceptor: Receptor[?]): Receptor[M] = {
		if foreignReceptor.isLocal then LocalReceptor(receiver)
		else RemoteReceptor(receiver.uri)
	}
}
