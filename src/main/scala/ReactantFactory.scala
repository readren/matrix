package readren.matrix


trait ReactantFactory[U] {
	/** Design note: Allows delegating the construction to subsidiary methods without losing type safety. */
	type MsgBuffer

	/** Creates the pending messages buffer needed by the [[createReactant]] method. */
	protected def createMsgBuffer(reactant: Reactant[U]): MsgBuffer

	protected def createEndpointProvider(msgBuffer: MsgBuffer): EndpointProvider[U]

	/** Creates a new [[Reactant]] */
	def createReactant(
		id: Reactant.SerialNumber,
		progenitor: Spawner[MatrixAdmin],
		reactantAdmin: MatrixAdmin,
		initialBehavior: Behavior[U]
	): Reactant[U]


}
