package readren.matrix
package behaviors

import behaviors.Inquisitive.Answer
import core.{Behavior, ReactantRelay}

import readren.sequencer.Doer

import scala.reflect.TypeTest

inline def ignore: Behavior[Any] = Ignore

/**
 * Enhances the `nestedBehavior` by adding the [[Inquisitive.ask]] capability, allowing the nested behavior to make structured questions and handle corresponding answers seamlessly.
 *
 * The `inquisitiveNesting` achieves this by:
 * 1. Wrapping the provided `nestedBehavior` within an [[Inquisitive]] interceptor,
 *    which manages the lifecycle of questions and their corresponding answers.
 * 2. Automatically handling responses that were not explicitly asked for via a
 *    configurable fallback `unaskedAnswerBehavior` (default: [[Ignore]]).
 *
 * **Purpose**:
 * - Simplify question-response workflows without requiring explicit state management.
 * - Enable nested behaviors to seamlessly issue questions and handle responses inline using the `ask` capability, reducing boilerplate and improving clarity.
 *
 * **Parameters**:
 * @param reactant              The `ReactantRelay` representing the context in which the questions and answers are exchanged.
 * @param nestedBehavior        The primary behavior of the reactant, extended with the ability to make structured questions.
 * @param unaskedAnswerBehavior A fallback behavior used to handle unexpected answers that do not correspond to a previously asked question. Defaults to [[Ignore]].
 * @tparam M The type of messages handled by the nested behavior.
 * @tparam A The type of answers, constrained to extend [[Answer]].
 * @tparam U A supertype of `A`, representing the broader type of messages that can be handled by the [[Inquisitive]] interceptor.
 *
 * @return A new behavior that combines the `nestedBehavior`'s message-handling capabilities with the [[Inquisitive.ask]] mechanism for structured question-response handling.
 */
inline def inquisitiveNest[M, A <: Answer, U >: A](reactant: ReactantRelay[U, ?])(nestedBehavior: Inquisitive[A, U] ?=> Behavior[M])(unaskedAnswerBehavior: Behavior[A] = Ignore): Behavior[A | M] = {
	val inquisitiveInterceptor = new Inquisitive[A, U](reactant, unaskedAnswerBehavior)
	unitedNest[A, M](inquisitiveInterceptor, nestedBehavior(using inquisitiveInterceptor))
}

inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] =
	new RestartNest(initializer(), initializer, cleaner)

inline def supervised[A](behavior: Behavior[A]): SupervisedNest[A] =
	new SupervisedNest[A](behavior, SupervisedNest.defaultCatcher)

inline def unitedNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
	new UnitedNest(bA, bB)

	
	
