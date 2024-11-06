package readren.matrix

sealed trait Signal {
	def isInitialization: Boolean

	def isPostStarted: Boolean

	def isPostRestarted: Boolean

	def isFinalization: Boolean

	def isPreStop: Boolean

	def isPreRestart: Boolean
}

/** After the reactant was started but before the first message is handled. */
sealed trait Initialization extends Signal {
	override final def isInitialization: Boolean = true

	override final def isFinalization: Boolean = false

	override final def isPreStop: Boolean = false

	override final def isPreRestart: Boolean = false
}

/** After the reactant was started for the first time but before the first message is handled. */
case object PostStarted extends Initialization {
	override final def isPostStarted: Boolean = true

	override final def isPostRestarted: Boolean = false
}

/** After the reactant was restarted but before the first message after the one that caused the restart is handled. */
case object PostRestarted extends Initialization {
	override final def isPostStarted: Boolean = false

	override final def isPostRestarted: Boolean = true
}

/** After a stop or restart command but before the reactant is terminated. */
sealed trait Finalization extends Signal {
	override final def isInitialization: Boolean = false

	override final def isPostStarted: Boolean = false

	override final def isPostRestarted: Boolean = false

	override final def isFinalization: Boolean = true
}

/** After a stop command but before the reactant is terminated. */
case object PreStop extends Finalization {
	override final def isPreStop: Boolean = true

	override final def isPreRestart: Boolean = false
}

/** After a restart command but before the reactant is terminated. */
case object PreRestart extends Finalization {
	override final def isPreStop: Boolean = false

	override final def isPreRestart: Boolean = true
}
