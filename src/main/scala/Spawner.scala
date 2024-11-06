package readren.matrix

import readren.taskflow.Maybe

import scala.collection.mutable


object Spawner {

}

/** A progenitor of [[Reactant]]s.
 * @param admin the [[MatrixAdmin]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createReactant]].
 * If this [[Spawner]] has an owner then the [[MatrixAdmin]] referred by this parameter should be the same as the assigned to the `owner`.
 * @param initialReactantSerial child reactants' serial will start at this value. Allows to distribute the load of [[MatrixAdmin]] more evenly among siblings.
 * @tparam MA the singleton type of the [[MatrixAdmin]] assigned to the `owner`.  
 * */
class Spawner[+MA <: MatrixAdmin](val owner: Maybe[Reactant[?]], val admin: MA, initialReactantSerial: Reactant.SerialNumber) { thisSpawner =>
	assert(owner.fold(true)(_.admin eq admin))

	/** Should be accessed only within the [[admin]] */
	private var reactantSerialSequencer: Reactant.SerialNumber = initialReactantSerial

	/** Should be accessed only within the [[admin]] */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty

	/** thread-safe */
	def createReactant[U, M <: U](behavior: Behavior[U], reactantFactory: ReactantFactory[U]): admin.Duty[Endpoint[M]] = {
		admin.Duty.mine { () =>
			reactantSerialSequencer += 1
			val reactantSerial = reactantSerialSequencer
			val reactantAdmin = admin.matrix.pickAdmin(reactantSerial)
			val reactant = reactantFactory.createReactant(reactantSerial, thisSpawner, reactantAdmin)
			children.addOne(reactantSerial, reactant)
			reactant.endpointProvider.forMyCreator[M]
		}
	}

	/** should be called withing the admin */
	def stopChildren(): admin.Duty[Unit] = {
		val stopDuties = children.values.map(child => admin.Duty.foreign(child.admin)(child.stop()))
		admin.Duty.sequence(stopDuties.toList).map(_ => ())
	}

	/** should be called withing the admin */
	def removeChild(childSerial: Long): Unit = children -= childSerial
}
