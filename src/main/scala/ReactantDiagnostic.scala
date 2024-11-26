package readren.matrix

import readren.taskflow.Maybe

class ReactantDiagnostic(val isReadyToProcess: Boolean, val isMarkedToStop: Boolean, val stopWasStarted: Boolean, val inboxMaybeHaveAPendingMsg: Boolean, val inboxSize: Int, val pendingMessages: Iterator[Any]) {
	override def toString: String = {
		s"ReactantDiagnostic(ready=$isReadyToProcess, markedToStop=$isMarkedToStop, stopStarted=$stopWasStarted, mayHaveAPendingMsg=$inboxMaybeHaveAPendingMsg, inboxSize=$inboxSize, firstPendingMsj=${if pendingMessages.hasNext then Maybe.some(pendingMessages.next()) else Maybe.empty})"
	}
}
