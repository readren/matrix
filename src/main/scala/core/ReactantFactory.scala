package readren.matrix
package core

import readren.sequencer.Doer

trait ReactantFactory {

	/** Creates a [[Duty]] that creates a new [[Reactant]].
	 * The implementation should be thread-safe, doing its job withing the received [[MatrixDoer]]. */
	def createsReactant[U, MD <: MatrixDoer](
		id: Reactant.SerialNumber,
		progenitor: Spawner[?],
		reactantDoer: MD,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): reactantDoer.Duty[Reactant[U]]


}
