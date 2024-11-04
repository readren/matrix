package readren.matrix

import readren.taskflow.Doer

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable


object Progenitor {
	type SerialNumber = Int


}

import Progenitor.*

/** A progenitor of [[Reactant]]s.
 * @param admin the [[MatrixAdmin]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createReactant]].
 * It is preferably it be the same [[MatrixAdmin]] instance assigned to the [[Reactant]] that contains this [[Progenitor]] to minimize thread switching.
 * @param initialReactantSerial child reactants' serial will start at this value. Allows to distribute the load of [[MatrixAdmin]] more evenly among siblings.
 * @param matrixAdmins the array with all the [[MatrixAdmin]] instances of the [[Matrix]] to which this [[Progenitor]] belongs. Needed to be able to distribute the [[MatrixAdmin]] instances among the children.
 *
 * */
class Progenitor(val admin: MatrixAdmin, initialReactantSerial: SerialNumber, matrixAdmins: IArray[MatrixAdmin]) { thisProgenitor =>


	/** Should be accessed only within the [[admin]] */
	private var reactantSerialSequencer: Int = initialReactantSerial

	/** Should be accessed only within the [[admin]] */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty

	inline def pickAdmin(serialNumber: SerialNumber): MatrixAdmin = matrixAdmins(serialNumber % matrixAdmins.length)

	def createReactant[D, M <: D](behavior: Behavior[D], reactantKind: ReactantFactory): admin.Duty[Endpoint[M]] = {
		admin.Duty.mine { () =>
			reactantSerialSequencer += 1
			val reactantSerial = reactantSerialSequencer
			val reactantAdmin = pickAdmin(reactantSerial)
			val msgBuffer = reactantKind.createMsgBuffer[D](reactantAdmin)
			val reactant = reactantKind.createReactant(reactantSerial, thisProgenitor, reactantAdmin, msgBuffer)
			children.addOne(reactantSerial, reactant)
			new EndpointController[M, D](msgBuffer)
		}
	}
}
