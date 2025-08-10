package readren.matrix
package core

import scala.reflect.TypeTest
import scala.util.NotGiven


/** Type class to provide a cast from a specific [[Signal]] subtype to `T` if said signal subtype is assignable to `T`. */
sealed trait IsSignalTest[T] {
	def started: Option[T]
	def restarted: Option[T]
	def restartReceived: Option[T]
	def stopReceived: Option[T]
}

object IsSignalTest {

	given [T] => (
		ttStarted: MyTypeTest[Started.type, T],
		ttRestarted: MyTypeTest[Restarted.type, T],
		ttRestartReceived: MyTypeTest[RestartReceived.type, T],
		ttStopReceived: MyTypeTest[StopReceived.type, T]
	) => IsSignalTest[T] = new IsSignalTest[T] {
		def started: Option[T] = ttStarted.unapply(Started)

		def restarted: Option[T] = ttRestarted.unapply(Restarted)

		def restartReceived: Option[T] = ttRestartReceived.unapply(RestartReceived)

		def stopReceived: Option[T] = ttStopReceived.unapply(StopReceived)
	}

	/** For some obscure reason [[TypeTest]] does not work as expected: unapply returns [[Some]] even if [[Signal]] is not assignable to `T` and then a class-class exception is thrown).
	 * This type class is a workaround to that problem.
	 * Type class to provide a cast from [[Signal]] to `T` if the first is assignable to the second. */
	sealed trait MyTypeTest[-S <: Signal, T] {
		def unapply(signal: S): Option[T]
	}

	// Companion object to define instances
	object MyTypeTest {
		// Instance when T is a subtype of Signal
		given [S <: Signal, T] => (ev: S <:< T, ttSignal: TypeTest[S, T]) => MyTypeTest[S, T] = new MyTypeTest[S, T] {
			def unapply(signal: S): Option[T] = ttSignal.unapply(signal)
			override def toString: String = ttSignal.toString
		}

		// Instance when T is not a subtype of Signal
		given [S <: Signal, T] => (ev: NotGiven[S <:< T]) => MyTypeTest[S, T] = new MyTypeTest[S, T] {
			def unapply(signal: S): Option[T] = None
			override def toString: String = "not assignable"
		}
	}
}