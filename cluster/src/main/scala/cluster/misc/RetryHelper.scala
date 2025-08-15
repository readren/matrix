package readren.matrix
package cluster.misc

import cluster.service.ParticipantService.TaskSequencer

import readren.sequencer.SchedulingExtension.MilliDuration

@deprecated("not used")
class RetryHelper(sequencer: TaskSequencer, startingDelay: MilliDuration, maxAttempts: Int) {
	private var attemptsDone: Int = 0
	
	inline def numAttemptsDone: Int = attemptsDone 
	
	def hasMoreTries: Boolean = attemptsDone < maxAttempts
	
	def tryAgainDelayed(action: Runnable): Unit = {
		attemptsDone += 1
		val retryDelay = startingDelay * attemptsDone * attemptsDone
		val schedule: sequencer.Schedule = sequencer.newDelaySchedule(retryDelay)
		sequencer.schedule(schedule)(action)
	}
}
