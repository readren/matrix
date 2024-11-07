package readren.matrix


trait ReactantFactory {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer[U]

	/** Creates the pending messages buffer needed by the [[createReactant]] method. */
	protected def createMsgBuffer[U](reactant: Reactant[U]): MsgBuffer[U]

	protected def createEndpointProvider[U](msgBuffer: MsgBuffer[U]): EndpointProvider[U]

	/** Creates a new [[Reactant]] */
	def createReactant[U](
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		reactantAdmin: MatrixAdmin,
		initialBehaviorBuilder: Reactant[U] => Behavior[U]
	): Reactant[U]


}
