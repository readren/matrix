package readren.matrix
package pruebas

import behaviors.Inquisitive
import core.*
import core.Matrix.DoerProviderDescriptor

import readren.sequencer.providers.{CooperativeWorkersDp, CooperativeWorkersWithAsyncSchedulerDp, CooperativeWorkersTieredDp, RoundRobinDp}
import rf.{RegularRf, SequentialMsgBufferRf}
import utils.SimpleAide

import readren.sequencer.Doer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object Prueba {

	private type TestedDoerProvider = CooperativeWorkersWithAsyncSchedulerDp

	private sealed trait Report

	private case class ConsumedReport(doer: Doer, producerIndex: Int, consumerIndex: Int, value: Int) extends Report

	private case class ConsumerWasStopped(consumerIndex: Int) extends Report

	private case object End extends Report


	private case class ProducerWasStopped(producerIndex: Int, producerDoer: Doer)

	private case class Acknowledge(override val toQuestion: Inquisitive.QuestionId) extends Inquisitive.Answer

	private case class Consumable(producerIndex: Int, value: Int, questionId: Inquisitive.QuestionId = 0L, replyTo: Endpoint[Acknowledge] = null) extends Inquisitive.Question[Acknowledge]

	private inline val NUMBER_OF_PRODUCERS = 100
	private inline val NUMBER_OF_CONSUMERS = 100
	private inline val NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER = 100
	private inline val WATCH_DOG_DELAY_MILLIS = 4000

	private inline val haveToCountAndCheck = true
	private inline val haveToShowFinalPhoto = false
	private inline val haveToShowPhotoEveryTime = false
	private inline val haveToRecordPhoto = haveToCountAndCheck || haveToShowFinalPhoto || haveToShowPhotoEveryTime
	private inline val usePercentages = false

	private inline val useInquisitiveProducer = true

	private class Pixel(var value: Int, var updateSerial: Int)

	private class Durations(var roundRobinRegularRf: Long = 0L, var roundRobinSequentialMsgBufferRf: Long = 0L, var cooperativeRegularRf: Long = 0L, var cooperativeSequentialMsBufferRf: Long = 0L, var testedRegularRf: Long = 0L, var testedSequentialMsgBufferRf: Long = 0L)

	private class Iteration(val accumulated: Durations = new Durations(), val minimum: Durations = new Durations(Long.MaxValue, Long.MaxValue, Long.MaxValue, Long.MaxValue, Long.MaxValue, Long.MaxValue))

	private object roundRobinDpd extends DoerProviderDescriptor[Doer]("round-robin") {
		override def build(owner: Matrix.DoerProvidersManager): RoundRobinDp = new RoundRobinDp.Impl()
	}

	private object cooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade]("cooperative-fence-off") {
		override def build(owner: Matrix.DoerProvidersManager): CooperativeWorkersDp = new CooperativeWorkersDp.Impl(false)
	}

	private object testedDpd extends DoerProviderDescriptor[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade]("scheduling-fence-off") {
		override def build(owner: Matrix.DoerProvidersManager): TestedDoerProvider = new CooperativeWorkersWithAsyncSchedulerDp.Impl(false)
	}

	private def roundRobinAide = new SimpleAide(roundRobinDpd)

	private def cooperativeQueueAide = new SimpleAide(cooperativeWorkersDpd)

	private def testedAide = new SimpleAide(testedDpd)

	@main def runPrueba(): Unit = {
		given ExecutionContext = ExecutionContext.global

		val numberOfWarmUpRepetitions = 4
		val numberOfMeasuredOfRepetitions = 56
		var totalFuture = Future.successful[Iteration](Iteration())
		for i <- 1 to (numberOfWarmUpRepetitions + numberOfMeasuredOfRepetitions) do {
			totalFuture = totalFuture.flatMap { iteration =>
				println(s"\n******* Loop #$i *******")
				for {
					cooperativeRegularRfDuration <- run(cooperativeQueueAide, RegularRf, i)
					cooperativeSequentialMsgBufferDuration <- run(cooperativeQueueAide, SequentialMsgBufferRf, i)
					testedRegularRfDuration <- run(testedAide, RegularRf, i)
					testedSequentialMsgBufferDuration <- run(testedAide, SequentialMsgBufferRf, i)
					roundRobinRegularRfDuration <- run(roundRobinAide, RegularRf, i)
					roundRobinSequentialMsgBufferDuration <- run(roundRobinAide, SequentialMsgBufferRf, i)
				} yield {
					if i > numberOfWarmUpRepetitions then {
						iteration.accumulated.cooperativeRegularRf += cooperativeRegularRfDuration
						iteration.accumulated.cooperativeSequentialMsBufferRf += cooperativeSequentialMsgBufferDuration
						iteration.accumulated.testedRegularRf += testedRegularRfDuration
						iteration.accumulated.testedSequentialMsgBufferRf += testedSequentialMsgBufferDuration
						iteration.accumulated.roundRobinRegularRf += roundRobinRegularRfDuration
						iteration.accumulated.roundRobinSequentialMsgBufferRf += roundRobinSequentialMsgBufferDuration
					}

					if cooperativeRegularRfDuration < iteration.minimum.cooperativeRegularRf then iteration.minimum.cooperativeRegularRf = cooperativeRegularRfDuration
					if cooperativeSequentialMsgBufferDuration < iteration.minimum.cooperativeSequentialMsBufferRf then iteration.minimum.cooperativeSequentialMsBufferRf = cooperativeSequentialMsgBufferDuration
					if testedRegularRfDuration < iteration.minimum.testedRegularRf then iteration.minimum.testedRegularRf = testedRegularRfDuration
					if testedSequentialMsgBufferDuration < iteration.minimum.testedSequentialMsgBufferRf then iteration.minimum.testedSequentialMsgBufferRf = testedSequentialMsgBufferDuration
					if roundRobinRegularRfDuration < iteration.minimum.roundRobinRegularRf then iteration.minimum.roundRobinRegularRf = roundRobinRegularRfDuration
					if roundRobinSequentialMsgBufferDuration < iteration.minimum.roundRobinSequentialMsgBufferRf then iteration.minimum.roundRobinSequentialMsgBufferRf = roundRobinSequentialMsgBufferDuration

					iteration
				}
			}
		}
		totalFuture.andThen {
			case Success(totalDuration) =>
				println(
					s"""All matrix were shutdown
					   |Average duration for regularRf: roundRobin -> ${totalDuration.accumulated.roundRobinRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}, cooperative -> ${totalDuration.accumulated.cooperativeRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}, tested -> ${totalDuration.accumulated.testedRegularRf / (numberOfMeasuredOfRepetitions * 1000000)}
					   |Average duration for sequentialMsgBuffer: roundRobin -> ${totalDuration.accumulated.roundRobinSequentialMsgBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}, cooperative-> ${totalDuration.accumulated.cooperativeSequentialMsBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}, tested -> ${totalDuration.accumulated.testedSequentialMsgBufferRf / (numberOfMeasuredOfRepetitions * 1000000)}
					   |Minimum duration for regularRf: roundRobin -> ${totalDuration.minimum.roundRobinRegularRf / 1000000}, cooperative -> ${totalDuration.minimum.cooperativeRegularRf / 1000000}, tested -> ${totalDuration.minimum.testedRegularRf / 1000000}
					   |Minimum duration for sequentialMsgBuffer: roundRobin -> ${totalDuration.minimum.roundRobinSequentialMsgBufferRf / 1000000}, cooperative-> ${totalDuration.minimum.cooperativeSequentialMsBufferRf / 1000000}, tested -> ${totalDuration.minimum.testedSequentialMsgBufferRf / 1000000}
					   |Press <enter> to exit""".stripMargin
				)
			case Failure(cause) => cause.printStackTrace()
		}

		StdIn.readLine()
		println("Key caught. Main thread is ending.")
	}

	private def run[DefaultDoer <: Doer](testingAide: SimpleAide[DefaultDoer], reactantFactory: ReactantFactory, loopId: Int): Future[Long] = {
		println(s"\nTest started:  loop=$loopId, descriptor=${testingAide.defaultDoerProviderDescriptor.id}, factory=$reactantFactory")
		val matrix = new Matrix("myMatrix", testingAide)
		println(s"Matrix created: doerProvider=${matrix.doerProvidersManager.get(testingAide.defaultDoerProviderDescriptor).getClass.getSimpleName}")

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
				printer = printer.andThen { _ => println(table) }(using ExecutionContext.global)
			}
		}

		System.gc()
		var nanoAtEnd: Long = 0
		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Report] {
			case consumedReport: ConsumedReport =>
				consumedReport.doer.checkWithin()

				if haveToRecordPhoto then {
					val counterValue = counter.incrementAndGet()
					val entry = photo(consumedReport.consumerIndex)(consumedReport.producerIndex)
					assert(consumedReport.value == entry.value + 1)
					entry.value = consumedReport.value
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

		diagnosticScheduler.fixedRate(0, WATCH_DOG_DELAY_MILLIS, TimeUnit.MILLISECONDS) { () =>

			try {
				val sb = new StringBuilder
				sb.append("\n<<< InspectorA <<<\n")
				matrix.doerProvidersManager.diagnose(sb)
				sb.append(">>> InspectorA >>>\n")
				println(sb)
			} catch {
				case e: Throwable =>
					e.printStackTrace()
					throw e
			}
		}

		val result = Promise[Long]

		// println(matrix.doerProvidersManager.diagnose(new StringBuilder("Pre parent creation:\n")))

		matrix.spawns[ProducerWasStopped | ConsumerWasStopped](reactantFactory, matrix.provideDefaultDoer("parent")) { parent =>
			// println("Parent initialization")
			parent.doer.checkWithin()

			parent.doer.Duty_sequenceToArray(
				for consumerIndex <- 0 until NUMBER_OF_CONSUMERS yield {
					parent.spawns[Consumable](reactantFactory, matrix.provideDefaultDoer("consumer")) { consumer =>
						consumer.doer.checkWithin()
						var completedCounter = 0
						consumable =>
							if consumable.value >= 0 then {
								outEndpoint.tell(ConsumedReport(consumer.doer, consumable.producerIndex, consumerIndex, consumable.value))
								// if consumable.value == 9 && (consumerIndex % 10) == 5 then throw new Exception("a ver que onda")
								if useInquisitiveProducer then consumable.replyTo.tell(Acknowledge(consumable.questionId))
								Continue
							} else {
								completedCounter += 1
								if completedCounter == NUMBER_OF_PRODUCERS then Stop
								else Continue
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

					/** Creates a Duty that builds a producer with operates as follows:
					 * - Sends a Consumable to each consumer and then again NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER times.
					 * - The Consumables are sent one after the other without waiting any response.
					 * */
					def buildsRegularProducer: parent.doer.Duty[ReactantRelay[Initialization]] = {
						parent.spawns[Initialization](reactantFactory, matrix.provideDefaultDoer("regular-producer")) { producer =>
							producer.doer.checkWithin()

							def loop(restartCount: Int): Behavior[Initialization] = {
								_ =>
									if restartCount < NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER then {
										for consumerEndpoint <- consumersEndpoints do
											consumerEndpoint.tell(Consumable(producerIndex, restartCount))
										RestartWith(loop(restartCount + 1))
									} else {
										for consumerEndpoint <- consumersEndpoints do
											consumerEndpoint.tell(Consumable(producerIndex, -1))
										Stop
									}
							}

							loop(0)
						}
					}

					/**
					 * Creates a Duty that builds a producer which operates as follows:
					 * - For each consumer, the following actions are performed sequentially, repeated NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER times:
					 *   - A Consumable is sent to the consumer.
					 *   - The producer waits for an Acknowledge from the consumer before sending the next Consumable.
					 * - These actions are performed concurrently across all consumers.
					 */
					def buildsInquisitiveProducer: parent.doer.Duty[ReactantRelay[Started.type | Acknowledge]] = {
						parent.spawns[Started.type | Acknowledge](reactantFactory, matrix.provideDefaultDoer("inquisitive-producer")) { producer =>
							producer.doer.checkWithin()
							val selfEndpoint = producer.endpointProvider.local[Acknowledge]

							behaviors.inquisitiveNest(producer) { (started: Started.type) =>
								import Inquisitive.*
								var completedConsumersCounter = 0
								// for each consumer simultaneously do: send a Consumable to it and wait for the Acknowledge before sending the next Consumable to it, repeating this cycle NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER times.
								for (consumerEndpoint, consumerIndex) <- consumersEndpoints.zipWithIndex do {
									def loop(numberOfMessagesAlreadySentToConsumer: Int): Unit = {

										if numberOfMessagesAlreadySentToConsumer < NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER then {
											consumerEndpoint.ask(questionId => Consumable(producerIndex, numberOfMessagesAlreadySentToConsumer, questionId, selfEndpoint))
												.andThen(_ => loop(numberOfMessagesAlreadySentToConsumer + 1))
												.triggerAndForget(true)
										} else {
											consumerEndpoint.tell(Consumable(producerIndex, -1))
											completedConsumersCounter += 1
											if completedConsumersCounter == NUMBER_OF_CONSUMERS then producer.stop()
										}
									}

									loop(0)
								}
								Continue
							}()
						}
					}

					val buildsProducer: parent.doer.Duty[ReactantRelay[?]] =
						if useInquisitiveProducer then buildsInquisitiveProducer
						else buildsRegularProducer
					buildsProducer.trigger(true) { producer =>
						parent.doer.checkWithin()
						parent.watch(producer, ProducerWasStopped(producerIndex, producer.doer))
					}
				}
			}

			var activeConsumers = NUMBER_OF_CONSUMERS
			var activeProducers = NUMBER_OF_PRODUCERS

			{
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

			diagnosticScheduler.fixedRate(0, WATCH_DOG_DELAY_MILLIS, TimeUnit.MILLISECONDS) { () =>

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
				println(s"+++ Descriptor: ${testingAide.defaultDoerProviderDescriptor.id} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++")
				// println(s"After successful completion diagnostic:\n${matrix.doerProvidersManager.diagnose(new StringBuilder())}")

				result.success(nanoAtEnd - nanoAtStart)
			}
		}
		result.future.andThen { tryDuration =>
			// println(s"Before closing: duration=${tryDuration.map(_ / 1000000)}")
			matrix.doerProvidersManager.shutdown()
			Try(matrix.doerProvidersManager.awaitTermination(1, TimeUnit.SECONDS)) match {
				case Success(true) => println("Shutdown completed normally")
				case Success(false) => println("Await termination timeout elapsed")
				case Failure(cause) => println(s"Shutdown is not completed after one seconds: $cause")
			}

			diagnosticScheduler.shutdown()
		}(using ExecutionContext.global)
	}
}
