package readren.sequencer

import scala.util.Failure

extension [A](failure: Failure[A]) {
	inline def castTo[B]: Failure[B] = failure.asInstanceOf[Failure[B]]
}


inline def deriveToString[A](a: A): String = {
	${ ToolsMacro.deriveToStringImpl[A]('a) }
}


