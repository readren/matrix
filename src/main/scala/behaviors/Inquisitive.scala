package readren.matrix
package behaviors

import behaviors.Inquisitive.{Answer, Question}
import core.{Behavior, Continue, Endpoint, HandleResult, Unhandled}

import readren.taskflow.Doer

import scala.collection.mutable
import scala.reflect.TypeTest


object Inquisitive {
	type QuestionId = Long

	trait Question {
		val questionId: QuestionId
	}

	trait Answer {
		val toQuestion: QuestionId
	}

	extension [Q <: Question](endpoint: Endpoint[Q]) {
		def ask[A <: Answer, D <: Doer](questionBuilder: Inquisitive.QuestionId => Q)(using inquisitive: Inquisitive[A, D]): inquisitive.doer.SubscriptableDuty[A] = {
			inquisitive.ask(endpoint, questionBuilder)
		}
	}	
}

class Inquisitive[A <: Answer, D <: Doer](val doer: D, unaskedAnswersBehavior: Behavior[A] = Ignore) extends Behavior[A] {
	private var lastQuestionId = 0L
	private val pendingQuestions: mutable.LongMap[doer.Covenant[A]] = mutable.LongMap.empty

	override final def handle(answer: A): HandleResult[A] = {
		pendingQuestions.getOrElse(answer.toQuestion, null) match {
			case covenant: doer.Covenant[A] =>
				pendingQuestions.subtractOne(answer.toQuestion)
				covenant.fulfill(answer)()
				Continue
			case null =>
				unaskedAnswersBehavior.handle(answer)
		}
	}

	// TODO add a timeout
	def ask[Q <: Question](endpoint: Endpoint[Q], questionBuilder: Inquisitive.QuestionId => Q): doer.SubscriptableDuty[A] = {
		assert(doer.assistant.isCurrentAssistant)
		val covenant = new doer.Covenant[A]
		lastQuestionId += 1
		pendingQuestions.update(lastQuestionId, covenant)
		val question = questionBuilder(lastQuestionId)
		endpoint.tell(question)
		covenant.subscriptableDuty
	}
}
