package readren.matrix

import readren.taskflow.Doer

import scala.annotation.tailrec

object MsgHandlerExecutorsManager {
	trait Aide {
		def buildDoerAssistantForMsgHandlerExecutor(): Doer.Assistant
	}
}

class MsgHandlerExecutorsManager(aide: MsgHandlerExecutorsManager.Aide) {
	private val msgHandlerExecutors: IArray[MsgHandlerExecutor] = {
		val availableProcessors = Runtime.getRuntime.availableProcessors()
		IArray.fill(availableProcessors) {
			new MsgHandlerExecutor(aide.buildDoerAssistantForMsgHandlerExecutor())
		}
	}

	private val picksPerUpdate = msgHandlerExecutors.length * 7
	/** This variable is accessed (read and mutated) concurrently by many threads, which may cause its value being corrupted, but it doesn't matter. See the comments in the implementation of [[pickExecutor]] */
	private var remainingPicksUntilUpdate: Int = picksPerUpdate
	/** These elements in this array are accessed (read and mutated) concurrently by many threads, which may cause its value being corrupted, but it doesn't matter. See the comments in the implementation of [[pickExecutor]] */
	private val loadByIndex: Array[Integer] = Array.fill(msgHandlerExecutors.length)(0)

	/** Picks the [[MsgHandlerExecutor]] with the least load. */
	def pickExecutor(): MsgHandlerExecutor = {

		@tailrec
		def findLeastLoadedDoerUsingCurrentMetrics(previousIndex: Int = msgHandlerExecutors.length, minLoad: Int = Integer.MAX_VALUE, leastLoadedDoer: MsgHandlerExecutor = msgHandlerExecutors(0)): MsgHandlerExecutor = {
			if previousIndex == 0 then leastLoadedDoer
			else {
				val index = previousIndex - 1
				val doer = msgHandlerExecutors(index)
				val load = doer.load
				loadByIndex(index) = load
				if load < minLoad then {
					findLeastLoadedDoerUsingCurrentMetrics(index, load, doer)
				} else {
					findLeastLoadedDoerUsingCurrentMetrics(index, minLoad, leastLoadedDoer)
				}
			}
		}

		@tailrec
		def findLeastLoadedDoerUsingOldMetrics(previousIndex: Int = msgHandlerExecutors.length, minLoad: Int = Integer.MAX_VALUE, leastLoadedDoerIndex: Int = 0): Int = {
			if previousIndex == 0 then leastLoadedDoerIndex
			else {
				val index = previousIndex - 1
				val doer = msgHandlerExecutors(index)
				val load = loadByIndex(index)
				if load == 0 then index
				else if load < minLoad then {
					findLeastLoadedDoerUsingOldMetrics(index, load, index)
				} else {
					findLeastLoadedDoerUsingOldMetrics(index, minLoad, leastLoadedDoerIndex)
				}
			}
		}

		// if the next variable value fetch gets an old value the worst that can happen is to do more picks per update than the established, which is nothing to worry about.
		if remainingPicksUntilUpdate > 0 then {
			// if the next assignment is overridden by another thread, the worst that can happen is to miss a pick count, which is nothing to worry about.
			remainingPicksUntilUpdate -= 1
			val pickedDoerIndex = findLeastLoadedDoerUsingOldMetrics()
			// if the next assignment is overridden by another thread, the worst that can happen is to miss a metric increment, which is nothing to worry about.
			loadByIndex(pickedDoerIndex) += 2
			msgHandlerExecutors(pickedDoerIndex)
		} else {
			// if the next assignment is overridden by another thread, it will be done again in the next pick, so no worries.
			remainingPicksUntilUpdate = picksPerUpdate
			findLeastLoadedDoerUsingCurrentMetrics()
		}
	}

}
