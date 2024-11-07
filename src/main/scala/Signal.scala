package readren.matrix

/** Type of the signal sent to the [[Behavior.handleSignal]] method. */
sealed trait Signal {
	def isInitialization: Boolean

	def isStarted: Boolean

	def isRestarted: Boolean

	def isCmdReceived: Boolean

	def isStopReceived: Boolean

	def isRestartReceived: Boolean
}

/** After the reactant was started and before the first message is handled. */
sealed trait Initialization extends Signal {
	override final def isInitialization: Boolean = true

	override final def isCmdReceived: Boolean = false

	override final def isStopReceived: Boolean = false

	override final def isRestartReceived: Boolean = false
}

/** After the reactant was started for the first time but before the first message is handled. */
case object Started extends Initialization {
	override final def isStarted: Boolean = true

	override final def isRestarted: Boolean = false
}

/** After the reactant was restarted and before the first message after the one that caused the restart is handled. */
case object Restarted extends Initialization {
	override final def isStarted: Boolean = false

	override final def isRestarted: Boolean = true
}

/** After a stop or restart command but before the reactant is terminated. */
sealed trait CmdReceived extends Signal {
	override final def isInitialization: Boolean = false

	override final def isStarted: Boolean = false

	override final def isRestarted: Boolean = false

	override final def isCmdReceived: Boolean = true
}

/** After a stop command but before the reactant is terminated. */
case object StopReceived extends CmdReceived {
	override final def isStopReceived: Boolean = true

	override final def isRestartReceived: Boolean = false
}

/** After a restart command but before the reactant is terminated. */
case object RestartReceived extends CmdReceived {
	override final def isStopReceived: Boolean = false

	override final def isRestartReceived: Boolean = true
}
