package readren.matrix
package timed

import core.{AbstractMatrix, MatrixDoer}
import providers.assistant.TimedAssistantProvider

import readren.taskflow.{Doer, TimersExtension}

class MatrixTimedDoer(id: MatrixDoer.Id, anAssistant: TimedAssistantProvider#ProvidedAssistant, matrix: AbstractMatrix) extends MatrixDoer(id, anAssistant, matrix), TimersExtension {
	override type TimedAssistant = TimedAssistantProvider#ProvidedAssistant
	override val timedAssistant: TimedAssistant = anAssistant
}
