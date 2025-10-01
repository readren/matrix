package readren.nexus
package core

import java.net.URI

/** Facade of the [[SpuronFactory.MsgBuffer]]. Exposes the receiving aspect of it to the [[Endpoint]]s. */
trait Receiver[-U] {
	
	/** Should be thread-safe. */
	def submit(message: U): Unit
	
	def uri: URI
}
