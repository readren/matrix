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

	private val reactantSerialSequencer: AtomicInteger = new AtomicInteger(progenitorSerial)

	/** Should be accessed only within the [[thisProgenitorAdmin]] */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty

	inline def pickAdmin(serialNumber: SerialNumber): MatrixAdmin = matrixAdmins(serialNumber % matrixAdmins.length)

	def createReactant[U, M <: U](behavior: Behavior[U], reactantKind: ReactantKind): Inbox[M] = {
		val reactantSerial = reactantSerialSequencer.incrementAndGet()
		val reactantAdmin = pickAdmin(reactantSerial)
		val inbox = reactantKind.createInbox[M](reactantAdmin)
		thisProgenitorAdmin.queueForSequentialExecution {
			val reactant = reactantKind.createReactant(reactantSerial, thisProgenitor, reactantAdmin, inbox)
			children.addOne(reactantSerial, reactant)
			inbox.setOwner(reactant, thisProgenitorAdmin)
		}
		inbox
	}

}
