package readren.nexus
package core


class SpuronDiagnostic(val isReadyToProcess: Boolean, val isMarkedToStop: Boolean, val stopWasStarted: Boolean, val inboxSize: Int, val pendingMessages: Iterator[Any], val childrenDiagnostic: Array[SpuronDiagnostic]) {
	override def toString: String = {
		s"SpuronDiagnostic(ready=$isReadyToProcess, markedToStop=$isMarkedToStop, stopStarted=$stopWasStarted, inboxSize=$inboxSize, firstPendingMsj=${if pendingMessages.hasNext then pendingMessages.next() else "empty"}, children=${if childrenDiagnostic.length == 0 then "[]" else childrenDiagnostic.mkString("[\n\t", "\n\t", "\n]")})"
	}
}
