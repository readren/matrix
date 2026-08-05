package readren.sequencer
package sandbox

import readren.common.{Maybe, shouldBe}

import scala.collection.mutable
import scala.util.Try

/** A minimalist version of [[Doer]] intended to try other designs. */
trait Doer2 {

	/** Queues the provided procedure for execution after previous ones. */
	def run(procedure: => Unit): Unit

	/** Abstracts over how a supplier is wired into an effect type `F` within the context of this [[Doer]].
	 *
	 * Instances determine the strategy by which a supplier `() => A` is captured or scheduled into `F[A]`. This may happen eagerly (e.g. queuing execution via [[Doer.run]] immediately) or lazily (e.g. deferring until the effect is engaged).
	 *
	 * @tparam A the type of value produced by the supplier
	 * @tparam F the effect type into which the supplier is wired
	 */
	trait WirableSoft[A, +F[_]] {
		/** Wires the given supplier into an instance of `F[A]`.
		 *
		 * The wiring strategy is defined by the implementing instance: it may schedule execution immediately via [[Doer.run]], defer it until the effect is engaged, or apply any other strategy consistent with this [[Doer]]'s execution model.
		 * @param supplier the computation to wire into `F`
		 * @return an `M[A]` that captures or schedules the supplier according to this instance's strategy
		 */
		inline def wire(inline supplier: () => A): F[A]
	}

	/** Wires the given supplier into an effect `F[A]` using the [[WirableSoft]] instance corresponding to `F`. The [[WirableSoft]] instances for effect types defined in [[Doer]] are provided by [[Doer]] itself.
	 * @param supplier the computation to submit
	 * @param wirableTask the type-class instance that determines how the supplier is wired
	 * @tparam A the type of value produced
	 * @tparam F the effect type into which the supplier is wired
	 * @return an `F[A]` wired according to the [[WirableSoft]] instance in scope
	 */
	inline def submit[A, F[_]](inline supplier: () => A)(using wirableTask: WirableSoft[A, F]): F[A] = {
		wirableTask.wire(supplier)
	}

	trait Task[+A] {
		def subscribe(onComplete: A => Unit): Unit
	}

	final class Task_Mine[A](supplier: () => A) extends Task[A] {
		override def subscribe(onComplete: A => Unit): Unit =
			run(onComplete(supplier()))
	}

	inline given [A] =>WirableSoft[A, Task] {
		override inline def wire(inline supplier: () => A): Task[A] =
			new Task_Mine(supplier)
	}

	trait WirableHardy[A, +F[_]] {
		inline def wire(inline supplier: () => Try[A]): F[A]
	}

	inline def submitHardy[A, F[_]](inline supplier: () => Try[A])(using wirableTask: WirableHardy[A, F]): F[A] = {
		wirableTask.wire(supplier)
	}

	trait Venture[+A] extends Task[Try[A]]

	private final class Venture_Own[A](supplier: () => Try[A]) extends Venture[A] {
		override def subscribe(onComplete: Try[A] => Unit): Unit =
			run(onComplete(supplier()))
	}

	inline given [A] =>WirableHardy[A, Venture] {
		override inline def wire(inline supplier: () => Try[A]): Venture[A] =
			new Venture_Own(supplier)
	}

	trait LatchingTask[+A] extends Task[A] {
		def maybeValue: Maybe[A]
	}

	final class Covenant[A] extends LatchingTask[A] {
		private var oValue: Maybe[A] = Maybe.empty
		private val consumers: mutable.Buffer[A => Unit] = mutable.Buffer.empty

		override def subscribe(onComplete: A => Unit): Unit =
			maybeValue.fold(consumers.addOne(onComplete))(onComplete)

		override def maybeValue: Maybe[A] = oValue

		def fulfill(value: A): Unit =
			if oValue.isEmpty then oValue = Maybe(value)
	}

	inline given [A] =>WirableSoft[A, LatchingTask] {
		override inline def wire(inline supplier: () => A): LatchingTask[A] = {
			val covenant = new Covenant[A]
			run(covenant.fulfill(supplier()))
			covenant
		}
	}

	trait LatchingVenture[+A] extends Venture[A] {
		def maybeValue: Maybe[Try[A]]
	}

	final class Commitment[A] extends LatchingVenture[A] {
		private var oValue: Maybe[Try[A]] = Maybe.empty
		private val consumers: mutable.Buffer[Try[A] => Unit] = mutable.Buffer.empty

		override def subscribe(onComplete: Try[A] => Unit): Unit =
			maybeValue.fold(consumers.addOne(onComplete))(onComplete)

		override def maybeValue: Maybe[Try[A]] = oValue

		def fulfill(value: Try[A]): Unit =
			if oValue.isEmpty then oValue = Maybe(value)
	}

	inline given [A] =>WirableHardy[A, LatchingVenture] {
		override inline def wire(inline supplier: () => Try[A]): LatchingVenture[A] = {
			val commitment = new Commitment[A]
			run(commitment.fulfill(supplier()))
			commitment
		}
	}
}

object Doer2 {
	@main def doer2Usage(): Unit = {
		val doer = new Doer2 {
			override def run(procedure: => Unit): Unit = procedure
		}

		val task: doer.Task[Int] = doer.submit(() => 3).shouldBe[doer.Task[Int]]
		println(task.getClass.getName)

		val latchingTask: doer.LatchingTask[Int] = doer.submit(() => 3)
		println(latchingTask.getClass.getName)
	}
}
