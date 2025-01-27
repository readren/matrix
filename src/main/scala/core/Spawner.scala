package readren.matrix
package core

import readren.taskflow.{Doer, Maybe}

import scala.collection.{MapView, mutable}


object Spawner {

}

/** A progenitor of [[Reactant]]s.
 * @param doer the [[MatrixDoer]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createReactant]].
 * If this [[Spawner]] has an owner then the [[MatrixDoer]] referred by this parameter should be the same as the assigned to the `owner`.
 * @param initialReactantSerial child reactants' serial will start at this value. Allows to distribute the load of [[MatrixDoer]] more evenly among siblings.
 * @tparam MD the singleton type of the [[MatrixDoer]] assigned to the `owner`.
 * */
class Spawner[+MD <: MatrixDoer](val owner: Maybe[Reactant[?]], val doer: MD, initialReactantSerial: Reactant.SerialNumber) { thisSpawner =>
	assert(owner.fold(true)(_.doer eq doer))

	/** Should be accessed only within the [[doer]] */
	private var reactantSerialSequencer: Reactant.SerialNumber = initialReactantSerial

	/** Should be accessed within the [[doer]] only. */
	private val children: mutable.LongMap[Reactant[?]] = mutable.LongMap.empty

	/** A view of the children that aren't fully stopped.
	 * Should be accessed withing the [[doer]]. */
	val childrenView: MapView[Long, Reactant[?]] = children.view

	/** Should be called withing the [[doer]] only. */
	def createReactant[U](
		childFactory: ReactantFactory, 
		childDoer: MatrixDoer, 
		isSignalTest: IsSignalTest[U], 
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): doer.Duty[ReactantRelay[U]] = {
		doer.checkWithin()
		reactantSerialSequencer += 1
		val reactantSerial = reactantSerialSequencer
		childFactory.createReactant(reactantSerial, thisSpawner, childDoer, isSignalTest, initialBehaviorBuilder)
			.onBehalfOf(doer)
			.map { reactant =>
				children.addOne(reactantSerial, reactant)
				reactant
			}
	}

	/** should be called withing the doer */
	def stopChildren(): doer.Duty[Array[Unit]] = {
		doer.checkWithin()
		val stopDuties = childrenView.values.map(child => doer.Duty.foreign(child.doer)(child.stop()))
		doer.Duty.sequenceToArray(stopDuties)
	}

	/** should be called withing the doer */
	def removeChild(childSerial: Long): Unit = {
		doer.checkWithin()
		children -= childSerial
	}
}
