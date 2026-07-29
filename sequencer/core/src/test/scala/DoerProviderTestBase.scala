package readren.sequencer

import GeneratorsForDoerTests.{*, given}

import munit.ScalaCheckEffectSuite
import org.scalacheck.effect.PropF
import readren.common.ScribeConfig

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.compiletime.uninitialized
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/** Abstract base test suite for testing [[DoerProvider]] implementations and their provided [[Doer]] instances.
 *
 * This class provides the suite lifecycle, shared fixture setup, and common helper utilities.
 *
 * @tparam D The type of Doer being tested, must extend [[Doer]].
 */
abstract class DoerProviderTestBase[D <: Doer : ClassTag] extends ScalaCheckEffectSuite {

	type DP <: DoerProvider[D]

	@volatile private var unhandledExceptionObserver: Null | ((Doer, Throwable) => Unit) = null

	private var sharedDoerProvider: DP = uninitialized
	private var sharedDoer: D = uninitialized
	private var sharedGenerators: GeneratorsForDoerTests[D] = uninitialized

	@volatile private var observingSession: Int = 0

	/** The implementation should build an instance of the [[DoerProvider]] implementation under test. */
	protected def buildDoerProvider: DP

	/** The implementation should release the specified [[DoerProvider]].
	 * The implementation may assume that the provided instance was obtained calling [[buildDoerProvider]]. */
	protected def releaseDoerProvider(doerProvider: DP): Unit

	/**
	 * This method should be invoked by the [[DoerProvider]] instances returned by [[buildDoerProvider]] whenever their [[DoerProvider.onUnhandledException]] callback is triggered.
	 * The extending class is responsible for ensuring this linkage.
	 */
	protected def onUnhandledException(doer: Doer, exception: Throwable): Unit = {
		if doer.isInSequence then {
			if unhandledExceptionObserver ne null then unhandledExceptionObserver(doer, exception)
		} else {
			val trace = new RuntimeException(exception)
			scribe.error(s"TEST FAILED - DO NOT IGNORE: `onUnhandledException` was called outside the provided doer's thread.", trace)
		}
	}

	//// Suite lifecycle ////

	override val munitTimeout: Duration = scala.concurrent.duration.Duration(15, "seconds")

	/** Creates the shared instances that depend on the abstract methods of this class in a deferred way to ensure the concrete subclass is fully constructed before said methods are invoked. */
	override def beforeAll(): Unit = {
		ScribeConfig.init(deleteLogFilesOnLaunch = true)

		val sharedDoerProvider = buildDoerProvider
		this.sharedDoerProvider = sharedDoerProvider
		val sharedDoer = sharedDoerProvider.provide(sharedDoerProvider.tagFromText("main-doer"))
		this.sharedDoer = sharedDoer
		val sharedGenerators = GeneratorsForDoerTests(sharedDoer, sharedDoerProvider)
		this.sharedGenerators = sharedGenerators
	}

	/** Clean up resources after tests. */
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

	//// Shared instance's getters ////

	/** Gets the shared instance of the [[DoerProvider]] implementation under test. */
	protected def getSharedDoerProvider: DP = sharedDoerProvider

	/** Builds an instance of [[Doer]] using the shared [[DoerProvider]]. */
	protected def buildDoer(tag: String): D = {
		val provider = sharedDoerProvider
		provider.provide(provider.tagFromText(tag))
	}

	/** Gets the shared instance of [[Doer]] provided by the shared doer provider. */
	protected def getSharedDoer: D = sharedDoer

	/** Get the shared instance of [[GeneratorsForDoerTests]] built using the [[DoerProvider]] and [[Doer]] instances returned by [[getSharedDoerProvider]] and [[getSharedDoer]] respectively. */
	protected def getGenerators: GeneratorsForDoerTests[D] = sharedGenerators

	//// UTILITIES ////

	/** Thrown by [[break]] */
	class BreakException(cause: Throwable) extends RuntimeException(cause)

	/** Breaks the `promise` if it wasn't already completed. */
	protected def break[P](message: String)(using promise: Promise[P]): Nothing = {
		val error = new AssertionError(message)
		promise.tryFailure(error)
		throw new BreakException(error)
	}

	protected def gate[P](using promise: Promise[P]): Future[P] = {
		promise.future.map { result =>
			result
		}(using scala.concurrent.ExecutionContext.Implicits.global)
	}

	/** Waits the promise to complete or the specified duration, what happens first. In the second case the promise is broken with the specified message.
	 * @return the [[Future]] view of the provided [[Promise]]. */
	protected def breakAfterWaiting[P](duration: Int, message: String)(using promise: Promise[P]): Future[P] = {
		val latch = CountDownLatch(1)
		promise.future.andThen(_ => latch.countDown())(using scala.concurrent.ExecutionContext.Implicits.global)
		if !latch.await(duration, TimeUnit.MILLISECONDS) then break(message)
		gate
	}

	/** Executes the provided `supplier` observing the calls to the [[onUnhandledException]] method until its completion. */
	protected def observingUnhandledExceptionsDo[R, P](supplier: () => Future[R])(onUnhandledException: (Doer, Throwable) => Unit)(using promise: Promise[P]): Future[R] = {
		if unhandledExceptionObserver ne null then break("Nesting `observingAsyncUnhandledExceptionsDo` is not supported")
		observingSession += 1
		unhandledExceptionObserver = onUnhandledException
		try {
			supplier().andThen { tryR =>
				unhandledExceptionObserver = null
			}
		} catch {
			case e: Throwable =>
				unhandledExceptionObserver = null
				Future.failed(e)
		}
	}

	//// FACTORED-OUT SAMPLE GENERATORS AND EXCEPTION ASSERTION HELPERS ////

	/** Factored-out property sample generator for Task operand exception tests. */
	protected def forAllTaskOperandExceptions(
		testBody: (Doer#Task[Int], Throwable, Doer#Task[Int], Throwable) => Future[Unit]
	): PropF[Future] = {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			for {
				successfulTask <- genSuccessfulTask[Int]()
				failingTaskException <- throwableArbitrary.arbitrary
				failingTask <- genFailingTaskFrom(failingTaskException)
				expectedUnhandledException <- throwableArbitrary.arbitrary
			} yield (successfulTask, failingTaskException, failingTask, expectedUnhandledException)
		) { case (successfulTask, failingTaskException, failingTask, expectedUnhandledException) =>
			testBody(successfulTask, failingTaskException, failingTask, expectedUnhandledException)
		}
	}

	/** Factored-out property sample generator for subscribe MonoObserver exception tests. */
	protected def forAllSubscribeExceptions(
		testBody: (Doer#Task[Int], Doer#Task[Int], Throwable, Future[Int]) => Future[Unit]
	): PropF[Future] = {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (task1: Task[Int], task2: Task[Int], thrownException: Throwable, future: Future[Int]) =>
			testBody(task1, task2, thrownException, future)
		}
	}

	/** Helper for testing exception handling of function operands passed to Task operations. */
	protected def checkTaskOperandExceptionHandling[R](
		opName: String,
		operatedTask: Doer#Task[R],
		failingTaskException: Throwable,
		expectedUnhandledException: Throwable,
		shouldPropagateNonFatales: Boolean = false
	)(using promise: Promise[Unit]): Future[Unit] = {
		val sd = getSharedDoer
		observingUnhandledExceptionsDo { () =>
			operatedTask.asInstanceOf[sd.Task[R]].trigger()(new sd.MonoObserver[R] {
				override def onSuccess(operationResult: R): Unit = {
					break(s"`$opName`: Completed successfully despite an exception was thrown: $expectedUnhandledException")
				}

				override def onError(actualException: Throwable): Unit = {
					if actualException ==== failingTaskException then promise.trySuccess(())
					else if !NonFatal(expectedUnhandledException) then {
						scribe.error(actualException)
						break(s"`$opName` incorrectly caught the fatal exception [$expectedUnhandledException] (and propagated [$actualException])")
					}
					else if shouldPropagateNonFatales then {
						if (actualException ne expectedUnhandledException) && (actualException.getCause ne expectedUnhandledException) then break(s"`$opName` caught the non fatal exception [$expectedUnhandledException] but propagated another one: [$actualException]")
						else promise.trySuccess(())
					} else break(s"`$opName` incorrectly caught [$expectedUnhandledException] (and propagated [$actualException]).")
				}
			})

			breakAfterWaiting(999, s"`$opName`: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

		} { (doer, unCaughtException) =>
			if unCaughtException ne expectedUnhandledException then break(s"`$opName`: An unexpected exception was uncaught: $unCaughtException")
			else if shouldPropagateNonFatales && NonFatal(expectedUnhandledException) then break(s"`$opName` had not caught the non-fatal exception [$unCaughtException]")
			else promise.trySuccess(())
		}
	}

	/** Helper for testing MonoObserver exception propagation. */
	protected def checkMonoObserverExceptionNotCaught[R](
		opName: String,
		operatedTask: Doer#Task[R],
		thrownException: Throwable
	)(using promise: Promise[Unit]): Future[Unit] = {
		val sd = getSharedDoer
		observingUnhandledExceptionsDo { () =>
			operatedTask.asInstanceOf[sd.Task[R]].trigger(false)(new sd.MonoObserver[R] {
				override def onSuccess(value: R): Unit = throw thrownException

				override def onError(actualException: Throwable): Unit = throw thrownException
			})

			breakAfterWaiting(999, s"$opName: No notification of the exception until 999 milliseconds after applying the operation. Waiting aborted.")

		} { (d, t) =>
			if (d eq sd) && (t eq thrownException) then promise.trySuccess(())
		}(using promise)
	}
}
