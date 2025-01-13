package readren.matrix
package behaviors

import behaviors.Inquisitive.Answer
import core.Behavior

import readren.taskflow.Doer

import scala.reflect.TypeTest

inline def ignore: Behavior[Any] = Ignore

inline def inquisitiveNest[M, A <: Answer, D <: Doer](doer: D)(nestedBehavior: Inquisitive[A, D] ?=> Behavior[M])(unaskedAnswerBehavior: Behavior[A] = Ignore): Behavior[A | M] = {
	val inquisitiveInterceptor = new Inquisitive[A, D](doer, unaskedAnswerBehavior)
	unitedNest[A, M](inquisitiveInterceptor, nestedBehavior(using inquisitiveInterceptor))
}

inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] =
	new RestartNest(initializer(), initializer, cleaner)

inline def supervised[A](behavior: Behavior[A]): SupervisedNest[A] =
	new SupervisedNest[A](behavior, SupervisedNest.defaultCatcher)

inline def unitedNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
	new UnitedNest(bA, bB)

	
	
