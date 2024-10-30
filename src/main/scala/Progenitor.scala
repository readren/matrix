package readren.matrix

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable



object Progenitor {
	type SerialNumber = Int


}

import Progenitor.*

/** A progenitor of [[Reactant]]s */
trait Progenitor(progenitorSerial: SerialNumber, matrixAdmins: IArray[MatrixAdmin]) { thisProgenitor =>


	private val thisProgenitorAdmin = pickAdmin(progenitorSerial)

	private val reactantSerialSequencer: AtomicInteger = new AtomicInteger(0)

	/** Should be accessed only within the [[thisProgenitorAdmin]] */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty

	inline def pickAdmin(serialNumber: SerialNumber): MatrixAdmin = matrixAdmins(serialNumber % matrixAdmins.length)

	def createReactant[U, M <: U](behavior: Behavior[M]): Inbox[M] = {
		val reactantSerial = reactantSerialSequencer.incrementAndGet()
		val reactantAdmin = pickAdmin(reactantSerial)
		val inbox = behavior.kind.createInbox[M](reactantAdmin)
		thisProgenitorAdmin.queueForSequentialExecution{
			val reactant = behavior.kind.createReactant(reactantSerial, reactantAdmin, inbox)
			children.addOne(reactantSerial, reactant)
			inbox.setOwner(reactant, thisProgenitorAdmin)
		}
		inbox
	}

}
