package readren.matrix

import readren.taskflow.TaskDomain

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable



object Progenitor {
	type TaskSerialNumber = Int

	trait Aide {
		val taskDomains: IArray[TaskDomain]

		def pickTaskDomain(serialNumber: TaskSerialNumber): TaskDomain
	}

}

import Progenitor.*

/** A progenitor of [[Doer]]s */
trait Progenitor(serialNumber: TaskSerialNumber, aide: Aide) {

	private val adminDomain = aide.pickTaskDomain(serialNumber)
	private val inboxSerialNumberSequencer: AtomicInteger = new AtomicInteger(0)

	/** Private to the [[newDoerSerialNumber()]] method. Never use directly. */
	private var doerSerialNumberSequencer: Doer.SerialNumber = 0
	/** Should be accessed only within the [[adminDomain]] */
	private inline def newDoerSerialNumber(): Doer.SerialNumber = {
		doerSerialNumberSequencer += 1
		doerSerialNumberSequencer
	}
	/** Should be accessed only within the [[adminDomain]] */
	private val children: mutable.LongMap[Doer] = mutable.LongMap.empty

	def createDoer[A](behavior: Behavior): Inbox[A] = {
		val inboxDomain = aide.pickTaskDomain(inboxSerialNumberSequencer.incrementAndGet())
		val inbox = behavior.kind.createInbox[A](inboxDomain)
		adminDomain.queueForSequentialExecution{
			val doerSerialNumber = newDoerSerialNumber()
			val doer = behavior.kind.createDoer(doerSerialNumber, inbox)
			children.addOne(doerSerialNumber, doer)
		}
		inbox
	}

}
