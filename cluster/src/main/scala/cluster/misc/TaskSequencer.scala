package readren.matrix
package cluster.misc

import readren.taskflow.{AbstractDoer, SchedulingExtension}

abstract class TaskSequencer extends AbstractDoer, SchedulingExtension {
	/**
	 * @return true if the current [[java.lang.Thread]] is the currently assigned to this [[TaskSequencer]] thread (the thread used to execute the block passed to [[executeSequentially]] and the runnable passed to [[scheduleSequentially]]); or false otherwise. */
	inline def isInSequence: Boolean = assistant.isWithinDoSiThEx	
}