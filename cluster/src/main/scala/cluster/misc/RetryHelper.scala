package readren.matrix
package cluster.misc

import readren.taskflow.SchedulingExtension.MilliDuration

import java.util.function.Consumer
import scala.util.{Failure, Success, Try}

class RetryHelper(sequencer: TaskSequencer, startingDelay: MilliDuration, maxAttempts: Int) {
	private var attemptsDone: Int = 0
	
	inline def numAttemptsDone: Int = attemptsDone 
	
	def hasMoreTries: Boolean = attemptsDone < maxAttempts
	
	def tryAgainDelayed(action: Runnable): Unit = {
		attemptsDone += 1
		val retryDelay = startingDelay * attemptsDone * attemptsDone
		val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
		sequencer.scheduleSequentially(schedule)(action)
	}
}
