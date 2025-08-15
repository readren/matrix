package readren.matrix
package core

import readren.sequencer.Doer

trait ReactantFactory {

	/** Creates a [[Duty]] that creates a new [[Reactant]].
	 * The implementation should be thread-safe, doing its job withing the received [[Doer]].
	 * @param serial the identifier to be assigned to the created [[Reactant]] to identify it among its siblings.
	 * @param progenitor the [[Spawner]] that creates the [[Reactant]]. The progenitor of a [[Reactant]] knows the set of its children, and every [[Reactant]] knows its progenitor.           
	 * @param reactantDoer the [[Doer]] instance to be assigned to the created [[Reactant]].
	 * @param isSignalTest knows which [[Signal]]s will the [[Reactant]] understand. In other words, knows which concrete [[Signal]] types are assignable to `U`. This information is obtained from the `U` type parameter at compile time.
	 * @param initialBehaviorBuilder a builder of the [[Behavior]] that the created [[Reactant]] will host when is born.                     
	 * @tparam U the type of the messages that the created [[Reactant]] understands.
	 * @tparam D the type of the [[Doer]] assigned to the created [[Reactant]]. */
	def createsReactant[U, D <: Doer](
		serial: Reactant.SerialNumber,
		progenitor: Spawner[?],
		reactantDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): reactantDoer.Duty[Reactant[U]]


}
