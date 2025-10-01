package readren.nexus
package core

import readren.sequencer.Doer

import scala.collection.{MapView, mutable}


object Spawner {

}

/** A spawner of [[SpuronCore]]s.
 * @param owner the [[Procreative]] that owns this [[Spawner]]. A [[Spawner]] is owned by either a [[SpuronCore]] or a [[NexusTyped]].
 * The [[Doer]] referred by this parameter should be the same as the assigned to the `owner`.
 * @param initialSpuronSerial child spurons' serial will start at this value.
 * @tparam D the singleton type of the [[Doer]] assigned to the `owner`.
 * */
class Spawner[D <: Doer](val owner: Procreative, val doer: D, initialSpuronSerial: SpuronCore.SerialNumber) { thisSpawner =>

	/**
	 * The [[Doer]] within which the mutable members of this class are mutated; and the [[Doer]] that contains the [[Duty]] returned by [[createsSpuron]]
	 * Is the same [[Doer]] instance as the owner's.
	 */

	/** Access must be within the [[doer]]. */
	private var spuronSerialSequencer: SpuronCore.SerialNumber = initialSpuronSerial

	/** Access must be within the [[doer]]. */
	private val children: mutable.LongMap[SpuronCore[?, ?]] = mutable.LongMap.empty

	/** A view of the children that aren't fully stopped.
	 * Access must be within the [[doer]]. */
	val childrenView: MapView[Long, SpuronCore[?, ?]] = children.view

	/** Creates a [[Duty]] that creates a new [[SpuronCore]].
	 * Calls must be within the [[doer]]. */
	def createsSpuron[U, CD <: Doer](
		childFactory: SpuronFactory,
		childDoer: CD,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Spuron[U, CD] => Behavior[U]
	): doer.Duty[Spuron[U, CD]] = {
		doer.checkWithin()
		spuronSerialSequencer += 1
		val spuronSerial = spuronSerialSequencer
		childFactory.createsSpuron(spuronSerial, thisSpawner, childDoer, isSignalTest, initialBehaviorBuilder)
			.onBehalfOf(doer)
			.map { spuron =>
				children.addOne(spuronSerial, spuron)
				spuron
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
