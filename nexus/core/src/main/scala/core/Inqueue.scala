package readren.nexus
package core

import java.net.URI

/** Facade exposed to the [[Receptor]]s of the buffer where all the messages received from them are enqueued. */
trait Inqueue[-U] {
	
	/** Should be thread-safe. */
	def submit(message: U): Unit
	
	def uri: URI
}
