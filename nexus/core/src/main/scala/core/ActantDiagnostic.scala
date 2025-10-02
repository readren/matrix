package readren.nexus
package core


class ActantDiagnostic(val isReadyToProcess: Boolean, val isMarkedToStop: Boolean, val stopWasStarted: Boolean, val inboxSize: Int, val pendingMessages: Iterator[Any], val childrenDiagnostic: Array[ActantDiagnostic]) {
	override def toString: String = {
		s"ActantDiagnostic(ready=$isReadyToProcess, markedToStop=$isMarkedToStop, stopStarted=$stopWasStarted, inboxSize=$inboxSize, firstPendingMsj=${if pendingMessages.hasNext then pendingMessages.next() else "empty"}, children=${if childrenDiagnostic.length == 0 then "[]" else childrenDiagnostic.mkString("[\n\t", "\n\t", "\n]")})"
	}
}
