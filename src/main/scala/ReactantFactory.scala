package readren.matrix


trait ReactantFactory {

	/** Creates a new [[Reactant]].
	 * The implementation should be thread-safe, doing its job withing the received [[MatrixAdmin]].  */
	def createReactant[U](
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		reactantAdmin: MatrixAdmin,
		initialBehaviorBuilder: ReactantRelay[U] => Behavior[U]
	): reactantAdmin.Duty[Reactant[U]]


}
