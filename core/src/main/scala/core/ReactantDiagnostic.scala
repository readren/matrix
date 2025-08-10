package readren.matrix
package core

import readren.sequencer.Maybe

class ReactantDiagnostic(val isReadyToProcess: Boolean, val isMarkedToStop: Boolean, val stopWasStarted: Boolean, val inboxSize: Int, val pendingMessages: Iterator[Any], val childrenDiagnostic: Array[ReactantDiagnostic]) {
	override def toString: String = {
		s"ReactantDiagnostic(ready=$isReadyToProcess, markedToStop=$isMarkedToStop, stopStarted=$stopWasStarted, inboxSize=$inboxSize, firstPendingMsj=${if pendingMessages.hasNext then pendingMessages.next() else "empty"}, children=${if childrenDiagnostic.length == 0 then "[]" else childrenDiagnostic.mkString("[\n\t", "\n\t", "\n]")})"
	}
}
