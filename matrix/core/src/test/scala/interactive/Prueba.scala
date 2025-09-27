package readren.matrix
package interactive

import behaviors.Inquisitive
import core.*
import core.Matrix.DoerProviderDescriptor
import rf.{RegularRf, SequentialMsgBufferRf}
import utils.SimpleAide

import readren.sequencer.Doer
import readren.sequencer.providers.{CooperativeWorkersDp, CooperativeWorkersWithAsyncSchedulerDp, CooperativeWorkersWithSyncSchedulerDp, RoundRobinDp}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object Prueba {

	private inline val A_KILO = 1024

	private type TestedDoerProvider = CooperativeWorkersWithAsyncSchedulerDp

	private sealed trait Report

	private case class ConsumedReport(doer: Doer, producerIndex: Int, consumerIndex: Int, value: Int) extends Report

	private case class ConsumerWasStopped(consumerIndex: Int) extends Report

	private case object End extends Report


	private case class ProducerWasStopped(producerIndex: Int, producerDoer: Doer)

	private case class Acknowledge(override val toQuestion: Inquisitive.QuestionId) extends Inquisitive.Answer

	private case class Consumable(producerIndex: Int, value: Int, questionId: Inquisitive.QuestionId = 0L, replyTo: Endpoint[Acknowledge] = null) extends Inquisitive.Question[Acknowledge]

	private val NUMBER_OF_WARM_UP_REPETITIONS = 1
	private val NUMBER_OF_MEASURE_REPETITIONS = 10

	private inline val NUMBER_OF_PRODUCERS = 50
	private inline val NUMBER_OF_CONSUMERS = 50
	private inline val NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER = 50
	private inline val WATCH_DOG_DELAY_MILLIS = 4000

	private inline val haveToCountAndCheck = true
	private inline val haveToShowFinalPhoto = false
	private inline val haveToShowPhotoEveryTime = false
	private inline val haveToRecordPhoto = haveToCountAndCheck || haveToShowFinalPhoto || haveToShowPhotoEveryTime
	private inline val usePercentages = false

	private inline val useInquisitiveProducer = true

	private class Pixel(var value: Int, var updateSerial: Int)

	private object roundRobinDpd extends DoerProviderDescriptor[Doer]("round-robin") {
		override def build(owner: Matrix.DoerProvidersManager): RoundRobinDp = new RoundRobinDp.Impl()
	}

	private object cooperativeWorkersDpd extends DoerProviderDescriptor[CooperativeWorkersDp.DoerFacade]("cooperative-fence-off") {
		override def build(owner: Matrix.DoerProvidersManager): CooperativeWorkersDp = new CooperativeWorkersDp.Impl(false)
	}

	private object cooperativeWorkersWithAsyncSchedulerDpd extends DoerProviderDescriptor[CooperativeWorkersWithAsyncSchedulerDp.SchedulingDoerFacade]("cooperative-with-async-scheduler-fence-off") {
		override def build(owner: Matrix.DoerProvidersManager): CooperativeWorkersWithAsyncSchedulerDp = new CooperativeWorkersWithAsyncSchedulerDp.Impl(false)
	}

	private object cooperativeWorkersWithSyncSchedulerDpd extends DoerProviderDescriptor[CooperativeWorkersWithSyncSchedulerDp.SchedulingDoerFacade]("cooperative-with-sync-scheduler-fence-off") {
		override def build(owner: Matrix.DoerProvidersManager): CooperativeWorkersWithSyncSchedulerDp = new CooperativeWorkersWithSyncSchedulerDp.Impl(false)
	}

	private val probes: Seq[Probe[?]] = List(
		Probe("RoundRobin and RegularRf", roundRobinDpd, RegularRf),
		Probe("CooperativeWorkers and RegularRf", cooperativeWorkersDpd, RegularRf),
		Probe("CooperativeWorkersWithAsyncScheduler and RegularRf", cooperativeWorkersWithAsyncSchedulerDpd, RegularRf),
		Probe("CooperativeWorkersWithSyncScheduler and RegularRf", cooperativeWorkersWithSyncSchedulerDpd, RegularRf),
		Probe("RoundRobin and SequentialRf", roundRobinDpd, SequentialMsgBufferRf),
		Probe("CooperativeWorkers and SequentialRf", cooperativeWorkersDpd, SequentialMsgBufferRf),
		Probe("CooperativeWorkersWithAsyncScheduler and SequentialRf", cooperativeWorkersWithAsyncSchedulerDpd, SequentialMsgBufferRf),
		Probe("CooperativeWorkersWithSyncScheduler and SequentialRf", cooperativeWorkersWithSyncSchedulerDpd, SequentialMsgBufferRf),
	)

	private class Probe[D <: Doer](name: String, descriptor: DoerProviderDescriptor[D], factory: ReactantFactory) {

		var accumulatedDuration: Long = 0
		var minimumDuration: Long = Long.MaxValue
		var maximumDuration: Long = 0
		var accumulatedConsumption: Long = 0
		var minimumConsumption: Long = Long.MaxValue
		var maximumConsumption: Long = Long.MinValue

		def measure(loopId: Int)(using ec: ExecutionContext): Future[Unit] = {
			val aide = SimpleAide(descriptor)
			run(aide, factory, loopId)
				.map { case (duration, consumption) =>
					if loopId > NUMBER_OF_WARM_UP_REPETITIONS then {
						accumulatedDuration += duration
						if duration < minimumDuration then minimumDuration = duration
						if duration > maximumDuration then maximumDuration = duration
						accumulatedConsumption += consumption
						if consumption < minimumConsumption then minimumConsumption = consumption
						if consumption > maximumConsumption then maximumConsumption = consumption
					}
				}
		}

		def showResult(): Unit = {
			println(f"Duration: max=${maximumDuration / 1_000_000}%5d, min=${minimumDuration / 1_000_000}%5d, average=${accumulatedDuration / (NUMBER_OF_MEASURE_REPETITIONS * 1_000_000)}%5d, Consumption: max=${maximumConsumption / A_KILO}%5d, min=${minimumConsumption / A_KILO}%5d, average=${accumulatedConsumption / (NUMBER_OF_MEASURE_REPETITIONS * A_KILO)}%5d, $name")
		}
	}

	@main def runProbes(): Unit = {
		given ExecutionContext = ExecutionContext.global

		var totalFuture = Future.successful[Unit](())

		def iterationLoop(loopIndex: Int): Future[Unit] = {
			if loopIndex > NUMBER_OF_WARM_UP_REPETITIONS + NUMBER_OF_MEASURE_REPETITIONS then Future.successful(())
			else {
				println(s"\n******* Loop #$loopIndex *******")
				probes.foldLeft(Future.successful[Unit](())) { (accumulator, probe) =>
					accumulator.flatMap(_ => probe.measure(loopIndex))
				}.flatMap(_ => iterationLoop(loopIndex + 1))
			}
		}

		iterationLoop(1).andThen {
			case Success(totalDuration) =>
				println(s"""All matrix were shutdown""")
				for probe <- probes do {
					probe.showResult()
				}
				println("Press <enter> to exit")

			case Failure(cause) =>
				cause.printStackTrace()
		}

		StdIn.readLine()
		println("Key caught. Main thread is ending.")
	}

	private def run[DefaultDoer <: Doer](testingAide: SimpleAide[DefaultDoer], reactantFactory: ReactantFactory, loopId: Int): Future[(Long, Long)] = {
		println(s"\nTest started:  loop=$loopId, descriptor=${testingAide.defaultDoerProviderDescriptor.id}, factory=${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", testingAide)
		println(s"Matrix created: doerProvider=${matrix.doerProvidersManager.get(testingAide.defaultDoerProviderDescriptor).getClass.getName}")

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
		val memoryBefore = ObjectCounterAgent.getApproximateObjectCount

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

		val result = Promise[(Long, Long)]

		// println(matrix.doerProvidersManager.diagnose(new StringBuilder("Pre parent creation:\n")))

		matrix.spawns[ProducerWasStopped | ConsumerWasStopped](reactantFactory, matrix.provideDefaultDoer("parent")) { parent =>
			// println("Parent initialization")
			parent.doer.checkWithin()

			parent.doer.Duty_sequenceToArray(
				for consumerIndex <- 0 until NUMBER_OF_CONSUMERS yield {
					parent.spawns[Consumable](reactantFactory, matrix.provideDefaultDoer(s"consumer#$consumerIndex")) { consumer =>
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
						parent.spawns[Initialization](reactantFactory, matrix.provideDefaultDoer(s"regular-producer#$producerIndex")) { producer =>
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
						parent.spawns[Started.type | Acknowledge](reactantFactory, matrix.provideDefaultDoer(s"inquisitive-producer#$producerIndex")) { producer =>
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
				val consumption = ObjectCounterAgent.getApproximateObjectCount - memoryBefore

				println(s"+++ Total number of non-negative numbers sent to children: ${counter.get()} +++")
				println(s"+++ Descriptor: ${testingAide.defaultDoerProviderDescriptor.id} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++, Consumption: ${consumption / A_KILO}M")
				// println(s"After successful completion diagnostic:\n${matrix.doerProvidersManager.diagnose(new StringBuilder())}")

				result.success((nanoAtEnd - nanoAtStart, consumption))
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
