package readren.matrix

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable



object Progenitor {
	type TaskSerialNumber = Int

	trait Aide {
		val doers: IArray[Doer]

		def pickDoer(serialNumber: TaskSerialNumber): Doer
	}

}

import Progenitor.*

/** A progenitor of [[Reactant]]s */
trait Progenitor(serialNumber: TaskSerialNumber, aide: Aide) {

	private val adminDoer = aide.pickDoer(serialNumber)
	private val inboxSerialNumberSequencer: AtomicInteger = new AtomicInteger(0)

	/** Private to the [[newReactantSerial()]] method. Never use directly. */
	private var reactantSerialSequencer: Reactant.SerialNumber = 0
	/** Should be accessed only within the [[adminDoer]] */
	private inline def newReactantSerial(): Reactant.SerialNumber = {
		reactantSerialSequencer += 1
		reactantSerialSequencer
	}
	/** Should be accessed only within the [[adminDoer]] */
	private val children: mutable.LongMap[Reactant] = mutable.LongMap.empty

	def createReactant[A](behavior: Behavior): Inbox[A] = {
		val inboxDoer = aide.pickDoer(inboxSerialNumberSequencer.incrementAndGet())
		val inbox = behavior.kind.createInbox[A](inboxDoer)
		adminDoer.queueForSequentialExecution{
			val reactantSerial = newReactantSerial()
			val reactant = behavior.kind.createReactant(reactantSerial, inbox)
			children.addOne(reactantSerial, reactant)
		}
		inbox
	}

}
