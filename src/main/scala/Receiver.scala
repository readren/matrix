package readren.matrix

import java.net.URI

/** Facade of the [[ReactantFactory.MsgBuffer]]. Exposes the receiving aspect of it to the [[Endpoint]]s. */
trait Receiver[U] {
	
	/** Should be thread-safe. */
	def submit(message: U): Unit
	
	def uri: URI
}
