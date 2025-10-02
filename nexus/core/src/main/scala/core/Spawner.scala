package readren.nexus
package core

import readren.sequencer.Doer

import scala.collection.{MapView, mutable}


object Spawner {

}

/** A spawner of [[ActantCore]]s.
 * @param owner the [[Procreative]] that owns this [[Spawner]]. A [[Spawner]] is owned by either a [[ActantCore]] or a [[NexusTyped]].
 * The [[Doer]] referred by this parameter should be the same as the assigned to the `owner`.
 * @param initialSerial child actant's serial will start at this value.
 * @tparam D the singleton type of the [[Doer]] assigned to the `owner`.
 * */
class Spawner[D <: Doer](val owner: Procreative, val doer: D, initialSerial: ActantCore.SerialNumber) { thisSpawner =>

	/**
	 * The [[Doer]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createsActant]]
	 * Is the same [[Doer]] instance as the owner's.
	 */

	/** Access must be within the [[doer]]. */
	private var lastChildSerial: ActantCore.SerialNumber = initialSerial

	/** Access must be within the [[doer]]. */
	private val children: mutable.LongMap[ActantCore[?, ?]] = mutable.LongMap.empty

	/** A view of the children that aren't fully stopped.
	 * Access must be within the [[doer]]. */
	val childrenView: MapView[Long, ActantCore[?, ?]] = children.view

	/** Creates a [[Duty]] that creates a new [[ActantCore]].
	 * Calls must be within the [[doer]]. */
	def createsActant[U, CD <: Doer](
		childFactory: ActantFactory,
		childDoer: CD,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Actant[U, CD] => Behavior[U]
	): doer.Duty[Actant[U, CD]] = {
		doer.checkWithin()
		lastChildSerial += 1
		val childSerial = lastChildSerial
		childFactory.createsActant(childSerial, thisSpawner, childDoer, isSignalTest, initialBehaviorBuilder)
			.onBehalfOf(doer)
			.map { childActant =>
				children.addOne(childSerial, childActant)
				childActant
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
