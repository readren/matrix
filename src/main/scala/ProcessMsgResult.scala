package readren.matrix

sealed trait ProcessMsgResult[M] 

case class ContinueWith[M](behavior: Behavior[M]) extends ProcessMsgResult[M]

case object Stop extends ProcessMsgResult[Nothing]

case object Restart extends ProcessMsgResult[Nothing]

case object Ignore extends ProcessMsgResult[Nothing]