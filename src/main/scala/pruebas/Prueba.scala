package readren.matrix
package pruebas

import doerproviders.{SharedQueueDoerProvider, SimpleDoerProvider, BalancedDoerProvider as TestedDoerProvider}
import rf.{RegularRf, SequentialMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object Prueba {

	private sealed trait Answer

	private case class Response(doer: MatrixDoer, producerIndex: Int, consumerIndex: Int, value: Int) extends Answer

	private case object End extends Answer

	private sealed trait Cmd

	private case class ProducerWasStopped(producerIndex: Int, producerDoer: MatrixDoer) extends Cmd

	private case class ConsumerWasStopped(consumerIndex: Int) extends Cmd, Answer

	//	case class Send(text: String)

	private case class SpawnResponse(endpoint: Endpoint[String])

	private case class Consumable(producerIndex: Int, value: Int)

	private inline val NUMBER_OF_PRODUCERS = 100
	private inline val NUMBER_OF_CONSUMERS = 100
	private inline val NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER = 100

	private inline val haveToCountAndCheck = true
	private inline val haveToShowFinalPhoto = false
	private inline val haveToShowPhotoEveryTime = false
	private inline val haveToRecordPhoto = haveToCountAndCheck || haveToShowFinalPhoto || haveToShowPhotoEveryTime
	private inline val usePercentages = false

	trait Closer[-S <: ShutdownAble] {
		def close(shutdownAble: S): Unit

		def diagnose(shutdownAble: S): StringBuilder
	}

	given Closer[ShutdownAble] = new Closer[ShutdownAble] {
		override def close(shutdownAble: ShutdownAble): Unit = {
			shutdownAble.shutdown()
			Try(shutdownAble.awaitTermination(1, TimeUnit.SECONDS)) match {
				case Success(true) => println("Shutdown completed normally")
				case Success(false) => println("Await termination timeout elapsed")
				case Failure(cause) => println(s"Shutdown is not completed after one seconds: $cause")
			}
		}

		override def diagnose(shutdownAble: ShutdownAble): StringBuilder = shutdownAble.diagnose(new StringBuilder())
	}

	private class Pixel(var value: Int, var updateSerial: Int)

	private class Durations(var simpleRegularRf: Long = 0L, var simpleSequentialMsgBufferRf: Long = 0L, var sharedQueueRegularRf: Long = 0L, var sharedQueueSequentialMsBufferRf: Long = 0L, var testedRegularRf: Long = 0L, var testedSequentialMsgBufferRf: Long = 0L)

	@main def runPrueba(): Unit = {
		given ExecutionContext = ExecutionContext.global

		object simpleAide extends Matrix.Aide[SimpleDoerProvider] {
			override def buildDoerProvider(owner: Matrix[SimpleDoerProvider]): SimpleDoerProvider =
				new SimpleDoerProvider(owner)

			override def buildLogger(owner: Matrix[SimpleDoerProvider]): Logger = new SimpleLogger(Logger.Level.info)
		}

		object sharedQueueAide extends Matrix.Aide[SharedQueueDoerProvider] {
			override def buildDoerProvider(owner: Matrix[SharedQueueDoerProvider]): SharedQueueDoerProvider =
				new SharedQueueDoerProvider(owner, Executors.defaultThreadFactory(), Runtime.getRuntime.availableProcessors(), _.printStackTrace())

			override def buildLogger(owner: Matrix[SharedQueueDoerProvider]): Logger = new SimpleLogger(Logger.Level.info)
		}

		object testedAide extends Matrix.Aide[TestedDoerProvider] {
			override def buildDoerProvider(owner: Matrix[TestedDoerProvider]): TestedDoerProvider =
				new TestedDoerProvider(owner, Executors.defaultThreadFactory(), Runtime.getRuntime.availableProcessors(), _.printStackTrace(), () => LinkedBlockingQueue[Runnable]())

			override def buildLogger(owner: Matrix[TestedDoerProvider]): Logger = new SimpleLogger(Logger.Level.info)
		}

		val numberOfWarmUpRepetitions = 4
		val numberOfMeasuredOfRepetitions = 6
		var totalFuture = Future.successful[Durations](Durations())
		for i <- 1 to (numberOfWarmUpRepetitions + numberOfMeasuredOfRepetitions) do {
			totalFuture = totalFuture.flatMap { durationAccumulator =>
				println(s"\n******* Loop #$i *******")
				for {
					sharedQueueRegularRfDuration <- run(sharedQueueAide, RegularRf, i)
					sharedQueueSequentialMsgBufferDuration <- run(sharedQueueAide, SequentialMsgBufferRf, i)
					testedRegularRfDuration <- run(testedAide, RegularRf, i)
					testedSequentialMsgBufferDuration <- run(testedAide, SequentialMsgBufferRf, i)
					simpleRegularRfDuration <- run(simpleAide, RegularRf, i)
					simpleSequentialMsgBufferDuration <- run(simpleAide, SequentialMsgBufferRf, i)
				} yield {
					if i > numberOfWarmUpRepetitions then {
						durationAccumulator.sharedQueueRegularRf += sharedQueueRegularRfDuration
						durationAccumulator.sharedQueueSequentialMsBufferRf += sharedQueueSequentialMsgBufferDuration
						durationAccumulator.testedRegularRf += testedRegularRfDuration
						durationAccumulator.testedSequentialMsgBufferRf += testedSequentialMsgBufferDuration
						durationAccumulator.simpleRegularRf += simpleRegularRfDuration
						durationAccumulator.simpleSequentialMsgBufferRf += simpleSequentialMsgBufferDuration
					}
					durationAccumulator
				}
			}
		}
		totalFuture.andThen {
			case Success(totalDuration) =>
				println(
					s"""All matrix were shutdown
					   |Average duration for regularRf: simple -> ${totalDuration.simpleRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}, sharedQueue -> ${totalDuration.sharedQueueRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}, tested -> ${totalDuration.testedRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}
					   |Average duration for sequentialMsgBuffer: simple -> ${totalDuration.simpleSequentialMsgBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}, sharedQueue-> ${totalDuration.sharedQueueSequentialMsBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}, tested -> ${totalDuration.testedSequentialMsgBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}
					   |Press <enter> to exit""".stripMargin
				)
			case Failure(cause) => cause.printStackTrace()
		}

		StdIn.readLine()
		println("Key caught. Main thread is ending.")
	}

	private def run[DP <: Matrix.DoerProvider & ShutdownAble](testingAide: Matrix.Aide[DP], reactantFactory: ReactantFactory, loopId: Int)(using closer: Closer[DP]): Future[Long] = {
		println(s"\nTest started:  loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", testingAide)
		println(s"Matrix created: doerProvider=${matrix.doerProvider.getClass.getSimpleName}")

		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val photo = Array.fill(NUMBER_OF_CONSUMERS, NUMBER_OF_PRODUCERS)(new Pixel(-1, 0))
		val consumerState = Array.fill(NUMBER_OF_CONSUMERS)(false)
		var previousTable: String = ""

		def showPhoto(): Unit = {
			val lastSerial = counter.get()
			val sb = new StringBuilder(99999)
			sb.append("<<< photo <<<\n")
			sb.append("consumer\\producer   ")
			for p <- 0 until NUMBER_OF_PRODUCERS do
				sb.append(f"| $p%4d  ")
			for c <- 0 until NUMBER_OF_CONSUMERS do {
				sb.append(f"\n  ${if consumerState(c) then "stopped" else "running"}%8s  $c%5d:   ")
				for p <- 0 until NUMBER_OF_PRODUCERS do {
					if usePercentages then sb.append(f"${(photo(c)(p).updateSerial * 1000 + 500) / lastSerial}%7d ")
					else sb.append(f"${photo(c)(p).updateSerial}%7d ")
				}
			}
			sb.append("\n>>> photo >>>")
			val table = sb.toString()
			if table != previousTable then {
				previousTable = table
				printer = printer.andThen { _ => println(table) }(ExecutionContext.global)
			}
		}

		var nanoAtEnd: Long = 0
		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Answer] {
			case response: Response =>
				response.doer.checkWithin()

				if haveToRecordPhoto then {
					val counterValue = counter.incrementAndGet()
					val entry = photo(response.consumerIndex)(response.producerIndex)
					assert(response.value == entry.value + 1)
					entry.value = response.value
					entry.updateSerial = counterValue

					if haveToShowPhotoEveryTime then showPhoto()
				}

			case ConsumerWasStopped(consumerIndex) =>
				consumerState(consumerIndex) = true

			case End =>
				nanoAtEnd = System.nanoTime()
				if haveToShowFinalPhoto then showPhoto()
		}

		val diagnosticScheduler = new Scheduler

		diagnosticScheduler.schedule(4000, TimeUnit.MILLISECONDS) { () =>

			try {
				val sb = new StringBuilder
				sb.append("\n<<< InspectorA <<<\n")
				matrix.doerProvider.diagnose(sb)
				sb.append(">>> InspectorA >>>\n")
				println(sb)
			} catch {
				case e: Throwable =>
					e.printStackTrace()
					throw e
			}
		}

		val result = Promise[Long]

		// println(matrix.doerProvider.diagnose(new StringBuilder("Pre parent creation:\n")))

		matrix.spawn[Cmd](reactantFactory) { parent =>
			// println("Parent initialization")
			parent.doer.checkWithin()

			parent.doer.Duty.sequenceToArray(
				for consumerIndex <- 0 until NUMBER_OF_CONSUMERS yield {
					parent.spawn[Consumable](reactantFactory) { consumer =>
						consumer.doer.checkWithin()
						var completedCounter = 0
						Behavior.factory { consumable =>
							if consumable.value >= 0 then {
								outEndpoint.tell(Response(consumer.doer, consumable.producerIndex, consumerIndex, consumable.value))
								// if consumable.value == 9 && (consumerIndex % 10) == 5 then throw new Exception("a ver que onda")
								Continue
							} else {
								completedCounter += 1
								if completedCounter == NUMBER_OF_PRODUCERS then Stop
								else Continue
							}
						}
					}.map { consumer =>
						parent.doer.checkWithin()
						parent.watch(consumer, ConsumerWasStopped(consumerIndex))
						consumer.endpointProvider.local[Consumable]
					}
				}
			).trigger(true) { consumersEndpoints =>
				parent.doer.checkWithin()
				for producerIndex <- 0 until NUMBER_OF_PRODUCERS do {
					parent.spawn[Started.type | Restarted.type](reactantFactory) { producer =>
						producer.doer.checkWithin()

						def producerBehavior(restartCount: Int): Behavior[Started.type | Restarted.type] = Behavior.factory {
							case Started | Restarted =>
								if restartCount < NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER then {
									for consumerEndpoint <- consumersEndpoints do
										consumerEndpoint.tell(Consumable(producerIndex, restartCount))
									RestartWith(producerBehavior(restartCount + 1))
								} else {
									for consumerEndpoint <- consumersEndpoints do
										consumerEndpoint.tell(Consumable(producerIndex, -1))
									Stop
								}
						}

						producerBehavior(0)
					}.trigger(true) { producer =>
						parent.doer.checkWithin()
						parent.watch(producer, ProducerWasStopped(producerIndex, producer.doer))
					}
				}
			}

			var activeConsumers = NUMBER_OF_CONSUMERS
			var activeProducers = NUMBER_OF_PRODUCERS

			Behavior.factory {
				case cws@ConsumerWasStopped(consumerIndex) =>
					parent.doer.checkWithin()
					outEndpoint.tell(cws)
					activeConsumers -= 1
					if activeProducers > 0 || activeConsumers > 0 then {
						// println(s"Consumer $consumerIndex stopped. Active children: ${parent.children.size}")
						Continue
					} else {
						outEndpoint.tell(End)
						Stop
					}
				case ProducerWasStopped(producerIndex, producerDoer) =>
					parent.doer.checkWithin()
					activeProducers -= 1
					if activeProducers > 0 || activeConsumers > 0 then {
						// println(s"Producer $producerIndex (#${producerDoer.id}) stopped. Active children: ${parent.children.size}")
						Continue
					} else {
						outEndpoint.tell(End)
						Stop
					}
			}
		}.trigger() { parent =>
			matrix.doer.checkWithin()
			// println("Parent created")

			diagnosticScheduler.schedule(4000, TimeUnit.MILLISECONDS) { () =>

				try {
					val sb = new StringBuilder
					sb.append("\n<<< InspectorB <<<\n")
					sb.append(
						s"""Parent's diagnostic: ${parent.staleDiagnose}""".stripMargin
					)
					sb.append("\n>>> InspectorB >>>\n")
					println(sb)
				} catch {
					case e: Throwable =>
						e.printStackTrace()
						throw e
				}
			}

			parent.stopDuty.trigger() { _ =>

				println(s"+++ Total number of non-negative numbers sent to children: ${counter.get()} +++")
				println(s"+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++")
				println(s"After successful completion diagnostic:\n${closer.diagnose(matrix.doerProvider)}")

				result.success(nanoAtEnd - nanoAtStart)
			}
		}
		result.future.andThen { tryDuration =>
			println(s"Before closing: duration=${tryDuration.map(_/1000000)}")
			closer.close(matrix.doerProvider)
			diagnosticScheduler.shutdown()
		}(ExecutionContext.global)
	}
}
