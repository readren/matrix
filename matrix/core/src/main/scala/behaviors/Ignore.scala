package readren.matrix
package behaviors

import core.{Behavior, Continue, HandleResult}

object Ignore extends Behavior[Any] {
	override def handle(message: Any): HandleResult[Any] = Continue
}