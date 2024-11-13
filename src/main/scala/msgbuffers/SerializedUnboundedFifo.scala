package readren.matrix
package msgbuffers

import readren.taskflow.Maybe

import java.net.URI
import scala.collection.mutable

/**
 * @param owner the [[Reactant]] that owns this [[Inbox]] */
class SerializedUnboundedFifo[M](owner: Reactant[M]) extends Receiver[M], Inbox[M] { thisFifoInbox =>

	/** Should be accessed only within the [[admin]] */
	private val queue: mutable.ArrayDeque[M] = mutable.ArrayDeque.empty
	private var ownerIsReadyToProcess: Boolean = false

	val admin: MatrixAdmin = owner.admin

	override val uri: URI = {
		val mu = admin.matrix.uri
		URI(mu.getScheme, null, mu.getHost, mu.getPort, mu.getPath + owner.path, null, null)
	}

	override def submit(message: M): Unit = {
		admin.queueForSequentialExecution {
			// The next line stimulates the reactant in order to continue processing the messages that were submitted to the Receiver but were enqueued in the work queue of the executor instead of in this Receiver's queue.
			// TODO try delegating the decision of processing readiness to the Receiver. This may allow to use AtomicBoolean for the isReadyToProcessAMsg flag only when necessary.
			if ownerIsReadyToProcess then {
				assert(queue.isEmpty)
				owner.processMessages(message)
			}
			else queue.append(message)
		}
	}

	override def setOwnerReadyToProcessState(newState: Boolean): Unit = ownerIsReadyToProcess = newState

	override def withdraw(): Maybe[M] = {
		admin.checkWithin()
		if queue.isEmpty then Maybe.empty
		else Maybe.some(queue.removeHead())
	}

	override def nonEmpty: Boolean = {
		admin.checkWithin()
		queue.nonEmpty
	}
}
