package readren.common

import scala.util.Failure


extension [A](failure: Failure[A]) {
	inline def castTo[B]: Failure[B] = failure.asInstanceOf[Failure[B]]
}

extension [T](inline expr: T) {
	inline def shouldBe[E](using expected: T =:= E): T = expr
}

inline def deriveToString[A](a: A): String = {
	${ ToolsMacro.deriveToStringImpl[A]('a) }
}