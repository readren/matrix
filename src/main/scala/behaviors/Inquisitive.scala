package readren.matrix
package behaviors

import behaviors.Inquisitive.{Answer, Question}
import core.{Behavior, Continue, Endpoint, HandleResult}

import readren.taskflow.Doer

import scala.collection.mutable


object Inquisitive {
	type QuestionId = Long

	/**
	 * Specifies the requirements for questions asked by means of the [[ask]] method.
	 *
	 * The `Question` trait defines the structure and constraints that a message must satisfy to be processed as a valid question.
	 */
	trait Question[A <: Answer] {
		val questionId: QuestionId
		val replyTo: Endpoint[A]
	}

	/**
	 * Specifies the requirements for a message to be used as an answer to a `Question`.
	 *
	 * The `Answer` trait defines the structure and constraints that a message must satisfy to be processed as a valid answer to a corresponding `Question`.
	 */
	trait Answer {
		/**
		 * The identifier of the [[Question]] this [[Answer]] corresponds to.
		 *
		 * **Purpose**: This field is required to allow the [[Inquisitive]] behavior, when acting as an interceptor, to determine which specific [[Question]] this [[Answer]] corresponds to.
		 *
		 * **Usage**: When creating an [[Answer]], set this value to the `questionId` of the [[Question]] being answered.
		 */
		val toQuestion: QuestionId
	}

	extension [A <: Answer, Q <: Question[A]](endpoint: Endpoint[Q]) {
		def ask[D <: Doer](questionBuilder: Inquisitive.QuestionId => Q)(using inquisitive: Inquisitive[A, D]): inquisitive.reactantDoer.SubscriptableDuty[A] = {
			inquisitive.ask(endpoint, questionBuilder)
		}
	}	
}

/**
 * A behavior that represents an entity capable of asking questions and handling corresponding answers.
 *
 * @param reactantDoer The `Doer` instance responsible for fulfilling covenants associated with questions.
 * @param unaskedAnswersBehavior A fallback behavior to handle answers that were not explicitly asked for. Defaults to `Ignore`.
 * @tparam A The type of answers handled by this behavior. Must extend the `Answer` trait.
 * @tparam D The type of doer used within this behavior. Must extend the `Doer` trait.
 */
class Inquisitive[A <: Answer, D <: Doer](val reactantDoer: D, unaskedAnswersBehavior: Behavior[A] = Ignore) extends Behavior[A] {
	private var lastQuestionId = 0L
	private val pendingQuestions: mutable.LongMap[reactantDoer.Covenant[A]] = mutable.LongMap.empty

	override final def handle(answer: A): HandleResult[A] = {
		pendingQuestions.getOrElse(answer.toQuestion, null) match {
			case covenant: reactantDoer.Covenant[A] =>
				pendingQuestions.subtractOne(answer.toQuestion)
				covenant.fulfill(answer)()
				Continue
			case null =>
				unaskedAnswersBehavior.handle(answer)
		}
	}

	// TODO add a timeout
	def ask[Q <: Question[A]](endpoint: Endpoint[Q], questionBuilder: Inquisitive.QuestionId => Q): reactantDoer.SubscriptableDuty[A] = {
		assert(reactantDoer.assistant.isCurrentAssistant)
		val covenant = new reactantDoer.Covenant[A]
		lastQuestionId += 1
		pendingQuestions.update(lastQuestionId, covenant)
		val question = questionBuilder(lastQuestionId)
		endpoint.tell(question)
		covenant.subscriptableDuty
	}
}
