package readren.matrix
package core

import readren.sequencer.Doer

import scala.collection.{MapView, mutable}


object Spawner {

}

/** A spawner of [[Reactant]]s.
 * @param owner the [[Procreative]] that owns this [[Spawner]]. A [[Spawner]] is owned by either a [[Reactant]] or a [[Matrix]]. 
 * The [[Doer]] referred by this parameter should be the same as the assigned to the `owner`.
 * @param initialReactantSerial child reactants' serial will start at this value.
 * @tparam D the singleton type of the [[Doer]] assigned to the `owner`.
 * */
class Spawner[D <: Doer](val owner: Procreative, val doer: D, initialReactantSerial: Reactant.SerialNumber) { thisSpawner =>

	/**
	 * The [[Doer]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createsReactant]]
	 * Is the same [[Doer]] instance as the owner's.
	 */

	/** Access must be within the [[doer]]. */
	private var reactantSerialSequencer: Reactant.SerialNumber = initialReactantSerial

	/** Access must be within the [[doer]]. */
	private val children: mutable.LongMap[Reactant[?, ?]] = mutable.LongMap.empty

	/** A view of the children that aren't fully stopped.
	 * Access must be within the [[doer]]. */
	val childrenView: MapView[Long, Reactant[?, ?]] = children.view

	/** Creates a [[Duty]] that creates a new [[Reactant]].
	 * Calls must be within the [[doer]]. */
	def createsReactant[U, CD <: Doer](
		childFactory: ReactantFactory,
		childDoer: CD,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: ReactantGate[U, CD] => Behavior[U]
	): doer.Duty[ReactantGate[U, CD]] = {
		doer.checkWithin()
		reactantSerialSequencer += 1
		val reactantSerial = reactantSerialSequencer
		childFactory.createsReactant(reactantSerial, thisSpawner, childDoer, isSignalTest, initialBehaviorBuilder)
			.onBehalfOf(doer)
			.map { reactant =>
				children.addOne(reactantSerial, reactant)
				reactant
			}
	}

	/** Calls must be within the [[doer]]. */
	def stopsChildren(): doer.Duty[Array[Unit]] = {
		doer.checkWithin()
		val stopDuties = childrenView.values.map(child => doer.Duty_foreign(child.doer)(child.stop()))
		doer.Duty_sequenceToArray(stopDuties)
	}

	/** Calls must be within the [[doer]]. */
	def removeChild(childSerial: Long): Unit = {
		doer.checkWithin()
		children -= childSerial
	}
}
