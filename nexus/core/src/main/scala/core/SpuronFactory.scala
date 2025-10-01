package readren.nexus
package core

import readren.sequencer.Doer

trait SpuronFactory {

	/** Creates a [[Duty]] that yields a new [[SpuronCore]].
	 * The implementation should be thread-safe, doing its job withing the received [[Doer]].
	 * @param serial the identifier to be assigned to the created [[SpuronCore]] to identify it among its siblings.
	 * @param progenitor the [[Spawner]] that creates the [[SpuronCore]]. The progenitor of a [[SpuronCore]] knows the set of its children, and every [[SpuronCore]] knows its progenitor.
	 * @param spuronDoer the [[Doer]] instance to be assigned to the created [[SpuronCore]].
	 * @param isSignalTest knows which [[Signal]]s will the [[SpuronCore]] understand. In other words, knows which concrete [[Signal]] types are assignable to `U`. This information is obtained from the `U` type parameter at compile time.
	 * @param initialBehaviorBuilder a builder of the [[Behavior]] that the created [[SpuronCore]] will host when is born.
	 * @tparam U the type of the messages that the created [[SpuronCore]] understands.
	 * @tparam D the type of the [[Doer]] assigned to the created [[SpuronCore]]. */
	def createsSpuron[U, D <: Doer](
		serial: SpuronCore.SerialNumber,
		progenitor: Spawner[?],
		spuronDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Spuron[U, D] => Behavior[U]
	): spuronDoer.Duty[SpuronCore[U, D]]


}
