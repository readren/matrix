package readren.matrix

import readren.taskflow.Maybe

import scala.collection.{MapView, mutable}


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

	/** Should be accessed within the [[admin]] only. */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty
	
	/** A view of the children that aren't fully stopped.
	 * Should be accessed withing the [[admin]]. */
	val childrenView: MapView[Long, Reactant[?]] = children.view

	/** Should be called withing the [[admin]] only. */
	def createReactant[U](reactantFactory: ReactantFactory, isSignalTest: IsSignalTest[U], initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]): admin.Duty[ReactantRelay[U]] = {
		admin.checkWithin()
		reactantSerialSequencer += 1
		val reactantSerial = reactantSerialSequencer
		val reactantAdmin = admin.matrix.pickAdmin(reactantSerial)
		reactantFactory.createReactant(reactantSerial, thisSpawner, reactantAdmin, isSignalTest, initialBehaviorBuilder)
			.onBehalfOf(admin)
			.map { reactant =>
				children.addOne(reactantSerial, reactant)
				reactant
			}
	}

	/** should be called withing the admin */
	def stopChildren(): admin.Duty[Array[Unit]] = {
		admin.checkWithin()
		val stopDuties = childrenView.values.map(child => admin.Duty.foreign(child.admin)(child.stop()))
		admin.Duty.sequenceToArray(stopDuties)
	}

	/** should be called withing the admin */
	def removeChild(childSerial: Long): Unit = {
		admin.checkWithin()
		children -= childSerial
	}
}
