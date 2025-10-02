package readren.nexus
package core

import readren.sequencer.Doer

trait ActantFactory {

	/** Creates a [[Duty]] that yields a new [[ActantCore]].
	 * The implementation should be thread-safe, doing its job withing the received [[Doer]].
	 * @param serial the identifier to be assigned to the created [[ActantCore]] to identify it among its siblings.
	 * @param progenitor the [[Spawner]] that creates the [[ActantCore]]. The progenitor of a [[ActantCore]] knows the set of its children, and every [[ActantCore]] knows its progenitor.
	 * @param actantDoer the [[Doer]] instance to be assigned to the created [[ActantCore]].
	 * @param isSignalTest knows which [[Signal]]s will the [[ActantCore]] understand. In other words, knows which concrete [[Signal]] types are assignable to `U`. This information is obtained from the `U` type parameter at compile time.
	 * @param initialBehaviorBuilder a builder of the [[Behavior]] that the created [[ActantCore]] will host when is born.
	 * @tparam U the type of the messages that the created [[ActantCore]] understands.
	 * @tparam D the type of the [[Doer]] assigned to the created [[ActantCore]]. */
	def createsActant[U, D <: Doer](
		serial: ActantCore.SerialNumber,
		progenitor: Spawner[?],
		actantDoer: D,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: Actant[U, D] => Behavior[U]
	): actantDoer.Duty[ActantCore[U, D]]


}
