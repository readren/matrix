package readren.matrix
package core

trait ReactantFactory {

	/** Creates a new [[Reactant]].
	 * The implementation should be thread-safe, doing its job withing the received [[MatrixDoer]].  */
	def createReactant[U](
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixDoer],
		reactantDoer: MatrixDoer,
		isSignalTest: IsSignalTest[U],
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): reactantDoer.Duty[Reactant[U]]


}
