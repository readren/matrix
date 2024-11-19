package readren.matrix

import readren.taskflow.Maybe

import scala.reflect.TypeTest
import scala.util.NotGiven

/** For some obscure reason [[TypeTest]] does not work as expected: unapply returns [[Some]] even if [[Signal]] is not assignable to `T` and then a class-class exception is thrown).
 * This type class is a workaround to that problem. 
 * Type class to provide a cast from [[Signal]] to `T` if the first is assignable to the second. */
sealed trait IsSignalTest[T] {
	def unapply(signal: Signal): Option[T]
}

// Companion object to define instances
object IsSignalTest {
	// Instance when T is a subtype of Signal
	given [T](using ev: T <:< Signal, ttSignal: TypeTest[Signal, T]): IsSignalTest[T] = new IsSignalTest[T] {
		def unapply(signal: Signal): Option[T] = ttSignal.unapply(signal)
	}

	// Instance when T is not a subtype of Signal
	given [T](using ev: NotGiven[T <:< Signal]): IsSignalTest[T] = new IsSignalTest[T] {
		def unapply(signal: Signal): Option[T] = None
	}
}