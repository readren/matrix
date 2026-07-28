package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen, Prop}
import readren.common.{Maybe, ScribeConfig}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


/** Abstract test suite for testing [[DoerProvider]] implementations that provide [[Doer]] instances extended with [[FluxExtension]].
 *
 * This suite checks if the instances provided by a [[DoerProvider]] implementation respect the contract of [[Doer]] with [[FluxExtension]], without being tied to a specific implementation.
 * Subclasses extend this abstract class to verify that the [[Doer]] & [[FluxExtension]] instances provided by the [[DoerProvider]] satisfy all invariants tested here.
 *
 * @tparam D The type of Doer being tested, must extend both [[Doer]] and [[FluxExtension]].
 */
abstract class FluxDoerProviderTest[D <: Doer & FluxExtension : ClassTag] extends ScalaCheckEffectSuite {

	type DP <: DoerProvider[D]

	@volatile private var unhandledExceptionObserver: Null | ((Doer, Throwable) => Unit) = null

	private var sharedDoerProvider: DP = uninitialized
	private var sharedDoer: D = uninitialized
	private var sharedGenerators: GeneratorsForDoerTests[D] = uninitialized

	@volatile private var observingSession: Int = 0

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	protected def buildDoerProvider: DP

	/** The implementation should release the specified [[DoerProvider]]. */
	protected def releaseDoerProvider(doerProvider: DP): Unit

	/**
	 * Should be invoked by the [[DoerProvider]] instances returned by [[buildDoerProvider]] whenever their [[DoerProvider.onUnhandledException]] callback is triggered.
	 */
	protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
		if doer.isInSequence then {
			if unhandledExceptionObserver ne null then unhandledExceptionObserver(doer, exception)
		} else {
			val trace = new RuntimeException(exception)
			scribe.error(s"TEST FAILED - DO NOT IGNORE: `onUnhandledException` was called outside the provided doer's thread.", trace)
		}
	}

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(15, "seconds")

	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)

		val sharedDoerProvider = buildDoerProvider
		this.sharedDoerProvider = sharedDoerProvider
		val sharedDoer = sharedDoerProvider.provide(sharedDoerProvider.tagFromText("main-doer"))
		this.sharedDoer = sharedDoer
		val sharedGenerators = GeneratorsForDoerTests(sharedDoer, sharedDoerProvider)
		this.sharedGenerators = sharedGenerators
	}

	override def afterAll(): Unit = {
		println("Shutting down...")
		releaseDoerProvider(getSharedDoerProvider)
	}

	override def beforeEach(context: BeforeEach): Unit = {
		println(s"[START] ${context.test.name}")
		super.beforeEach(context)
	}

	override def afterEach(context: AfterEach): Unit = {
		println(s"[DONE]  ${context.test.name}")
		super.afterEach(context)
	}

	protected def getSharedDoerProvider: DP = sharedDoerProvider

	protected def buildDoer(tag: String): D = {
		val provider = sharedDoerProvider
		provider.provide(provider.tagFromText(tag))
	}

	protected def getSharedDoer: D = sharedDoer

	protected def getGenerators: GeneratorsForDoerTests[D] = sharedGenerators

	class BreakException(cause: Throwable) extends RuntimeException(cause)

	protected def break[P](message: String)(using promise: Promise[P]): Nothing = {
		val error = new AssertionError(message)
		promise.tryFailure(error)
		throw new BreakException(error)
	}

	protected def gate[P](using promise: Promise[P]): Future[P] = {
		promise.future.map(result => result)(using scala.concurrent.ExecutionContext.Implicits.global)
	}

	protected def breakAfterWaiting[P](duration: Int, message: String)(using promise: Promise[P]): Future[P] = {
		val latch = CountDownLatch(1)
		promise.future.andThen(_ => latch.countDown())(using scala.concurrent.ExecutionContext.Implicits.global)
		if !latch.await(duration, TimeUnit.MILLISECONDS) then break(message)
		gate
	}

	protected def observingUnhandledExceptionsDo[R, P](supplier: () => Future[R])(onUnhandledException: (Doer, Throwable) => Unit)(using promise: Promise[P]): Future[R] = {
		if unhandledExceptionObserver ne null then break("Nesting `observingAsyncUnhandledExceptionsDo` is not supported")
		observingSession += 1
		unhandledExceptionObserver = onUnhandledException
		try {
			supplier().andThen { _ =>
				unhandledExceptionObserver = null
			}
		} catch {
			case e: Throwable =>
				unhandledExceptionObserver = null
				Future.failed(e)
		}
	}

	protected def collectFlux[A](doer: D)(flux: doer.Flux[A]): Future[(List[(A, Int)], Option[Throwable], Boolean)] = {
		val promise = Promise[(List[(A, Int)], Option[Throwable], Boolean)]()
		val items = ArrayBuffer[(A, Int)]()

		flux.subscribeSync(new doer.FluxObserver[A] {
			override def onNext(a: A, index: Int): Unit = {
				items.append((a, index))
			}

			override def onError(ex: Throwable): Unit = {
				promise.trySuccess((items.toList, Some(ex), false))
			}

			override def onComplete(): Unit = {
				promise.trySuccess((items.toList, None, true))
			}
		})
		promise.future
	}

	////////// FACTORY METHODS //////////

	test("Flux_empty: emits no elements and completes immediately") {
		val doer = getSharedDoer
		collectFlux(doer)(doer.Flux_empty).map { case (items, error, completed) =>
			assert(items.isEmpty, "Flux_empty should not emit elements")
			assert(error.isEmpty, "Flux_empty should not emit error")
			assert(completed, "Flux_empty should complete")
		}
	}

	test("Flux_apply and Flux_fromIterable: emit elements sequentially with correct indices") {
		val doer = getSharedDoer
		val elements = List(10, 20, 30, 40)

		for {
			resApply <- collectFlux(doer)(doer.Flux_apply(elements *))
			resIter <- collectFlux(doer)(doer.Flux_fromIterable(elements))
		} yield {
			val expected = List((10, 0), (20, 1), (30, 2), (40, 3))
			assert(resApply._1 == expected, s"Flux_apply emitted ${resApply._1}, expected $expected")
			assert(resApply._3, "Flux_apply should complete")
			assert(resIter._1 == expected, s"Flux_fromIterable emitted ${resIter._1}, expected $expected")
			assert(resIter._3, "Flux_fromIterable should complete")
		}
	}

	test("Flux_fromIterableGuarded: catches non-fatal exception thrown during iteration") {
		val doer = getSharedDoer
		val expectedError = new RuntimeException("Iterator failed")
		val faultyIterable = new Iterable[Int] {
			override def iterator: Iterator[Int] = new Iterator[Int] {
				private var count = 0

				override def hasNext: Boolean = true

				override def next(): Int = {
					if count < 2 then {
						count += 1
						count * 100
					} else throw expectedError
				}
			}
		}

		collectFlux(doer)(doer.Flux_fromIterableGuarded(faultyIterable)).map { case (items, error, completed) =>
			assert(items == List((100, 0), (200, 1)), s"Emitted items: $items")
			assert(error.contains(expectedError), s"Expected $expectedError, got $error")
			assert(!completed, "Should not report onComplete on error")
		}
	}

	test("Flux_generate: generates sequence until Maybe.empty") {
		val doer = getSharedDoer
		val flux = doer.Flux_generate[Int] { idx =>
			if idx < 3 then Maybe(idx * 5) else Maybe.empty
		}

		collectFlux(doer)(flux).map { case (items, error, completed) =>
			val expected = List((0, 0), (5, 1), (10, 2))
			assert(items == expected, s"Got $items, expected $expected")
			assert(completed, "Should complete when Maybe.empty is returned")
		}
	}

	test("Flux_generateStatefully: maintains state per subscription") {
		val doer = getSharedDoer
		val flux = doer.Flux_generateStatefully[Int] { () =>
			var state = 100
			idx => {
				if idx < 3 then {
					val current = state
					state += 10
					Maybe(current)
				} else Maybe.empty
			}
		}

		for {
			sub1 <- collectFlux(doer)(flux)
			sub2 <- collectFlux(doer)(flux)
		} yield {
			val expected = List((100, 0), (110, 1), (120, 2))
			assert(sub1._1 == expected, s"Sub1 got ${sub1._1}")
			assert(sub2._1 == expected, s"Sub2 got ${sub2._1} (should restart state for new subscriber)")
		}
	}

	test("Flux_fromMonosSequentially and Flux_fromMonos: assemble Monos into stream") {
		val doer = getSharedDoer
		val mono1 = doer.Task_ready("A")
		val mono2 = doer.Task_ready("B")
		val mono3 = doer.Task_ready("C")
		val monos = IArray(mono1, mono2, mono3)

		for {
			resSeq <- collectFlux(doer)(doer.Flux_fromMonosSequentially(monos))
			resParallel <- collectFlux(doer)(doer.Flux_fromMonos(monos))
		} yield {
			val expectedSeq = List(("A", 0), ("B", 1), ("C", 2))
			assert(resSeq._1 == expectedSeq, s"Sequentially got ${resSeq._1}")
			assert(resParallel._1.size == 3, s"Parallel got ${resParallel._1.size} items")
		}
	}

	test("StreamEmitter: emits elements, completes, and supports observers") {
		val doer = getSharedDoer
		val emitter = new doer.StreamEmitter[Int]

		val f1 = collectFlux(doer)(emitter)
		emitter.emit(1)
		emitter.emit(2)
		emitter.end()

		f1.map { case (items, error, completed) =>
			assert(items == List((1, 0), (2, 1)), s"StreamEmitter emitted $items")
			assert(completed, "StreamEmitter should be completed")
		}
	}

	test("StreamEmitter: propagates failure to observers") {
		val doer = getSharedDoer
		val emitter = new doer.StreamEmitter[Int]
		val testError = new RuntimeException("Stream failed")

		val f1 = collectFlux(doer)(emitter)
		emitter.emit(42)
		emitter.fail(testError)

		f1.map { case (items, error, completed) =>
			assert(items == List((42, 0)), s"Emitted before fail: $items")
			assert(error.contains(testError), s"Should receive failure $testError")
			assert(!completed, "Should not be completed on error")
		}
	}

	////////// OPERATORS //////////

	test("Flux.map and mapWithIndex: transform elements and preserve indices") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(1, 2, 3)

		val mapped = base.map(x => x * 10)
		val mappedIdx = base.mapWithIndex((x, idx) => s"$x@$idx")

		for {
			resMap <- collectFlux(doer)(mapped)
			resMapIdx <- collectFlux(doer)(mappedIdx)
		} yield {
			assert(resMap._1 == List((10, 0), (20, 1), (30, 2)), s"map got ${resMap._1}")
			assert(resMapIdx._1 == List(("1@0", 0), ("2@1", 1), ("3@2", 2)), s"mapWithIndex got ${resMapIdx._1}")
		}
	}

	test("Flux.scan: accumulates state across stream elements") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(1, 2, 3, 4)
		val scanned = base.scan(0)((acc, elem, _) => acc + elem)

		collectFlux(doer)(scanned).map { case (items, _, completed) =>
			val expected = List((1, 0), (3, 1), (6, 2), (10, 3))
			assert(items == expected, s"scan got $items, expected $expected")
			assert(completed, "scan should complete")
		}
	}

	test("Flux.buffer: chunks stream into IArray of target size") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(1, 2, 3, 4, 5)
		val buffered = base.buffer[Int](2)

		collectFlux(doer)(buffered).map { case (items, _, completed) =>
			assert(items.size == 3, s"Buffer chunks count: ${items.size}")
			assert(items(0)._1.toList == List(1, 2), s"Chunk 0: ${items(0)._1.toList}")
			assert(items(1)._1.toList == List(3, 4), s"Chunk 1: ${items(1)._1.toList}")
			assert(items(2)._1.toList == List(5), s"Chunk 2: ${items(2)._1.toList}")
			assert(completed, "buffer should complete")
		}
	}

	test("Flux.take: limits number of emitted elements") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(10, 20, 30, 40, 50)
		val taken = base.take(3)

		collectFlux(doer)(taken).map { case (items, _, completed) =>
			val expected = List((10, 0), (20, 1), (30, 2))
			assert(items == expected, s"take got $items, expected $expected")
			assert(completed, "take should complete early")
		}
	}

	test("Flux.takeWhile: emits elements while predicate evaluates to true") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(1, 2, 3, 4, 5)
		val takeWhileFlux = base.takeWhile((a, _, _) => a < 4)

		collectFlux(doer)(takeWhileFlux).map { case (items, _, completed) =>
			val expected = List((1, 0), (2, 1), (3, 2))
			assert(items == expected, s"takeWhile got $items, expected $expected")
			assert(completed, "takeWhile should complete when predicate is false")
		}
	}

	test("Flux.foldWhile: reduces stream into Mono Task and stops on Maybe.empty") {
		val doer = getSharedDoer
		val base = doer.Flux_apply(1, 2, 3, 4, 5)
		val folded = base.foldWhile[Int](0, isGuarded = true) { (acc, elem, _) =>
			if acc + elem <= 6 then Maybe(acc + elem) else Maybe.empty
		}

		val promise = Promise[Int]()
		folded.subscribeSync(new doer.MonoObserver[Int] {
			override def onSuccess(value: Int): Unit = promise.trySuccess(value)

			override def onError(ex: Throwable): Unit = promise.tryFailure(ex)
		})

		promise.future.map { result =>
			assert(result == 6, s"foldWhile accumulated result: $result, expected 6")
		}
	}

	test("Flux.zip: pairs elements from two streams index-by-index") {
		val doer = getSharedDoer
		val fluxA = doer.Flux_apply("A", "B", "C")
		val fluxB = doer.Flux_apply(1, 2, 3)

		val zipped = fluxA.zip(fluxB)((a, b, idx) => s"$a-$b@$idx")

		collectFlux(doer)(zipped).map { case (items, _, completed) =>
			val expected = List(("A-1@0", 0), ("B-2@1", 1), ("C-3@2", 2))
			assert(items == expected, s"zip got $items, expected $expected")
			assert(completed, "zipped flux should complete")
		}
	}

	test("Flux.flatMap and Tensor flatteners: flattens nested streams") {
		val doer = getSharedDoer
		val outer = doer.Flux_apply(1, 2)
		val tensor = outer.flatMap(x => doer.Flux_apply(x * 10, x * 10 + 1))

		val seqFlux = tensor.flattenSequential

		collectFlux(doer)(seqFlux).map { case (items, _, completed) =>
			val values = items.map(_._1)
			assert(values == List(10, 11, 20, 21), s"flattenSequential got $values")
			assert(completed, "Tensor flattenSequential should complete")
		}
	}

	////////// PROPERTY-BASED TESTS //////////

	test("Flux generator property: all generated successful fluxes yield their elements") {
		val generators = getGenerators
		val doer = getSharedDoer
		PropF.forAllNoShrinkF { (elements: List[Int]) =>
			val flux = generators.genSuccessfulFluxFrom(elements).sample.get.asInstanceOf[doer.Flux[Int]]
			collectFlux(doer)(flux).map { case (items, error, completed) =>
				val values = items.map(_._1)
				assert(values == elements, s"Generated flux emitted $values, expected $elements")
				assert(error.isEmpty, "Successful flux should have no error")
				assert(completed, "Successful flux should complete")
			}
		}
	}
}
