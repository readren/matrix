package readren.matrix

/** Facade of the message buffer that exposes the receiving side to the [[EndpointController]]s. */
trait Receiver[D] {
	
	/** Should be thread-safe. */
	def submit(message: D): Unit
}
