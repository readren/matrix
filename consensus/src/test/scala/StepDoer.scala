package readren.consensus

import readren.common.Maybe
import readren.sequencer.Doer
import readren.sequencer.Doer.ExecutionSerial

import scala.collection.mutable

object StepDoer {
	val currentDoerThreadLocal: ThreadLocal[Doer] = new ThreadLocal[Doer]()
}

/** A deterministic, single-threaded [[Doer]] implementation for discrete-event testing.
 * Does not spawn or rely on background threads. All tasks enqueued via [[executeSequentially]]
 * are held in an in-memory queue until explicitly executed via [[step]] or [[drain]].
 *
 * @param tag human-readable tag identifying this sequencer instance.
 */
class StepDoer(override val tag: String) extends Doer { thisDoer =>
	override type Tag = String

	private var executionSequencer: ExecutionSerial = 0
	private val taskQueue: mutable.Queue[Runnable] = mutable.Queue.empty

	override def executeSequentially(runnable: Runnable): Unit = {
		taskQueue.enqueue(runnable)
	}

	override def currentExecutionSerial: ExecutionSerial = executionSequencer

	override def currentlyRunningDoer: Maybe[Doer] = Maybe(StepDoer.currentDoerThreadLocal.get())

	/** Number of runnable tasks currently waiting in this sequencer's queue. */
	def pendingTasksCount: Int = taskQueue.size

	/** True if this sequencer has at least one pending task. */
	def hasPendingTasks: Boolean = taskQueue.nonEmpty

	/** Dequeues and executes exactly one task from the queue within this sequencer's context.
	 * Increments [[currentExecutionSerial]] and sets [[currentlyRunningDoer]] so that
	 * [[Doer.checkWithin]] assertions succeed during execution.
	 *
	 * @return true if a task was executed; false if the queue was empty.
	 */
	def step(): Boolean = {
		if taskQueue.isEmpty then false
		else {
			val runnable = taskQueue.dequeue()
			val prevDoer = StepDoer.currentDoerThreadLocal.get()
			StepDoer.currentDoerThreadLocal.set(thisDoer)
			executionSequencer += 1
			try {
				runnable.run()
				true
			} finally {
				StepDoer.currentDoerThreadLocal.set(prevDoer)
			}
		}
	}

	/** Executes all pending tasks in the queue until empty or until [[maxSteps]] is reached.
	 *
	 * @param maxSteps safeguard against infinite loops from mutually recursive task submissions.
	 * @return the number of tasks executed.
	 */
	def drain(maxSteps: Int = 1000): Int = {
		var executedCount = 0
		while hasPendingTasks && executedCount < maxSteps do {
			step()
			executedCount += 1
		}
		executedCount
	}

	/** Clears all pending tasks from the queue without executing them. */
	def clear(): Unit = {
		taskQueue.clear()
	}
}
