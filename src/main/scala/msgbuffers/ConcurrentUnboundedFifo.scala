package readren.matrix
package msgbuffers

import readren.taskflow.Maybe

import java.net.URI
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class ConcurrentUnboundedFifo[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] {
	private val queue = new ConcurrentLinkedQueue[M]()
	private val ownerIsReadyToProcess: AtomicBoolean = new AtomicBoolean(false)

	override val uri: URI = {
		val mu = owner.admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		if ownerIsReadyToProcess.getAndSet(false) then owner.admin.queueForSequentialExecution(owner.processMessages(message))
		else queue.offer(message)
	}

	override def setOwnerReadyToProcessState(newState: Boolean): Unit = ownerIsReadyToProcess.set(newState)

	override def withdraw(): Maybe[M] = {
		Maybe.apply(queue.poll())
	}

	override def nonEmpty: Boolean = {
		!queue.isEmpty
	}
}
