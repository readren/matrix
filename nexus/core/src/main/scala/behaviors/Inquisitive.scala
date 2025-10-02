package readren.nexus
package behaviors

import behaviors.Inquisitive.{Answer, Question}
import core.*

import scala.collection.mutable


object Inquisitive {
	type QuestionId = Long

	/**
	 * Specifies the requirements for questions sent using the [[ask]] method.
	 *
	 * @tparam A The type of the corresponding answer.
	 */
	trait Question[A <: Answer] {
		val questionId: QuestionId
		val replyTo: Receptor[A]
	}

	/**
	 * Specifies the requirements for a valid response to a `Question`.
	 *
	 * **Usage**: Always set `toQuestion` to the `questionId` of the corresponding `Question`.
	 */
	trait Answer {
		/**
		 * The identifier of the [[Question]] this [[Answer]] corresponds to.
		 */
		val toQuestion: QuestionId
	}

	extension [A <: Answer, Q <: Question[A], U >: A](receptor: Receptor[Q])
		/**
		 * Sends a question constructed by the provided `questionBuilder` to the specified [[Receptor]], and returns an instance of {{{ inquisitive.agent.doer.SubscriptableDuty[A] }}} that will be completed when the corresponding answer is received.
		 *
		 * @param questionBuilder A function that takes a unique [[QuestionId]] and builds a [[Question]] of type `Q`.
		 * @param inquisitive The instance of [[Inquisitive]] responsible for managing the interaction. It is the interceptor 
		 * @return A subscriptable duty of type `SubscriptableDuty[A]`, representing the eventual answer to the question.
		 */
		def ask(questionBuilder: QuestionId => Q)(using inquisitive: Inquisitive[A, U]): inquisitive.agent.doer.SubscriptableDuty[A] = {
			inquisitive.ask(receptor, questionBuilder)
		}
	
}

/**
 * Represents a behavior capable of asking questions and handling corresponding answers.
 * 
 * This class is intended to be used as the interceptor behavior of an [[UnitedNest]]. See [[behaviors.inquisitiveNest]].
 *
 * The `Inquisitive` class encapsulates the logic for managing the lifecycle of questions and answers.
 * It allows components to send **questions** to specific [[Receptor]]s, track the pending requests, and handle **answers** when they are received.
 *
 * @param agent The [[Actant]] responsible for relaying answers.
 * @param unaskedAnswersBehavior A fallback behavior to handle unexpected answers (default is `Ignore`).
 * @tparam A The type of answers that this behavior manages. Must extend the `Answer` trait.
 * @tparam U A supertype of `A`, representing the broader category of compatible answers.
 */
class Inquisitive[A <: Answer, U >: A](val agent: Actant[U, ?], unaskedAnswersBehavior: Behavior[A] = Ignore) extends Behavior[A] {
	private var lastQuestionId = 0L
	private val pendingQuestions: mutable.LongMap[agent.doer.Covenant[A]] = mutable.LongMap.empty

	/**
	 * Handles answers received for previously sent questions.
	 *
	 * **Behavior**:
	 * - If the `Answer` corresponds to a tracked `Question` (via its `toQuestion` field):
	 *   - Removes the corresponding question from the `pendingQuestions` map.
	 *   - Fulfills the associated `Covenant`.
	 *   - Returns `Continue` to indicate successful processing.
	 * - If the `Answer` does NOT correspond to any known `Question`:
	 *   - Delegates handling to the `unaskedAnswersBehavior` fallback.
	 *
	 * @param answer The `Answer` to handle.
	 * @return A `HandleResult` indicating the status of handling this message.
	 */
	override final def handle(answer: A): HandleResult[A] = {
		pendingQuestions.getOrElse(answer.toQuestion, null) match {
			case covenant: agent.doer.Covenant[A] =>
				pendingQuestions.subtractOne(answer.toQuestion)
				covenant.fulfill(answer)()
				Continue
			case null =>
				unaskedAnswersBehavior.handle(answer)
		}
	}

	/**
	 * Sends a question to a specified `Receptor`
	 *
	 * @param receptor The target `Receptor` to send the question to.
	 * @param questionBuilder A function that builds the `Question` using a `QuestionId`.
	 * @tparam Q The type of the `Question`, constrained to match `A`.
	 * @return A {{{ actant.doer.SubscriptableDuty[A] }}} instance that will be completed when the answer is received.
	 */
	def ask[Q <: Question[A]](receptor: Receptor[Q], questionBuilder: Inquisitive.QuestionId => Q): agent.doer.SubscriptableDuty[A] = {
		assert(agent.doer.isInSequence)
		val covenant = new agent.doer.Covenant[A]
		lastQuestionId += 1
		pendingQuestions.update(lastQuestionId, covenant)
		val question = questionBuilder(lastQuestionId)
		receptor.tell(question)
		covenant.subscriptableDuty
	}
}
