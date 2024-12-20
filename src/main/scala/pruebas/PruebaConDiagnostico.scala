package readren.matrix
package pruebas

import rf.{RegularRf, SequentialMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.{Success, Failure}

object PruebaConDiagnostico {

	sealed trait Answer

	case class Response(doer: MatrixDoer, producerIndex: Int, consumerIndex: Int, value: Int) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case object ProducerWasStopped extends Cmd

	case class ConsumerWasStopped(childIndex: Int) extends Cmd, Answer

	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	case class Consumable(producerIndex: Int, value: Int)

	private inline val NUMBER_OF_PRODUCERS = 32
	private inline val NUMBER_OF_CONSUMERS = 32
	private inline val NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER = 16

	private inline val haveToShowFinalPhoto = true
	private inline val haveToShowPhotoEveryTime = false
	private inline val haveToRecordPhoto = haveToShowFinalPhoto || haveToShowPhotoEveryTime
	private inline val usePercentages = true

	private class Pixel(var value: Int, var updateSerial: Int)

	@main def runPruebaConDiagnostico(): Unit = {
		given ExecutionContext = ExecutionContext.global

		val numberOfWarmUpRepetitions = 1
		val numberOfMeasuredOfRepetitions = 3
		var totalFuture = Future.successful[(Long, Long)]((0L, 0L))
		for i <- 1 to (numberOfWarmUpRepetitions + numberOfMeasuredOfRepetitions) do {
			totalFuture = totalFuture.flatMap { durationAccumulator =>
				println(s"\n******* Loop #$i *******")
				for {
					regularRfDuration <- run(RegularRf, i)
					sequentialMsgBufferDuration <- run(SequentialMsgBufferRf, i)
				} yield {
					if i <= numberOfWarmUpRepetitions then durationAccumulator
					else (durationAccumulator._1 + regularRfDuration, durationAccumulator._2 + sequentialMsgBufferDuration)
				}
			}
		}
		totalFuture.andThen {
			case Success(totalDuration) =>
				println(
					s"""All matrix were shutdown
					   |Average duration: regularRf-> ${totalDuration._1 / (numberOfMeasuredOfRepetitions * 1000000)}, sequentialMsgBufferRfTotalDuration-> ${totalDuration._2 / (numberOfMeasuredOfRepetitions * 1000000)}
					   |Press <enter> to exit""".stripMargin
				)
			case Failure(cause) => cause.printStackTrace()	
		}

		StdIn.readLine()
		println("Key caught. Main thread is ending.")
	}

	def run(reactantFactory: ReactantFactory, loopId: Int): Future[Long] = {

		val testingAide = new Shared.TestingAide(true, s"Executors diagnostic corresponding to: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", testingAide)
		println(s"Matrix created: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val photo = Array.fill(NUMBER_OF_CONSUMERS, NUMBER_OF_PRODUCERS)(new Pixel(-1, 0))
		val consumerState = Array.fill(NUMBER_OF_CONSUMERS)(false)

		def showPhoto(): Unit = {
			val lastSerial = counter.get()
			val lsb = new StringBuilder(99999)
			lsb.append(">>> photo >>>>>\n")
			lsb.append("consumer\\producer")
			for p <- 0 until NUMBER_OF_PRODUCERS do
				lsb.append(f"| $p%4d  ")
			for c <- 0 until NUMBER_OF_CONSUMERS do {
				lsb.append(f"\n  ${consumerState(c)}%5b  $c%5d:   ")
				for p <- 0 until NUMBER_OF_CONSUMERS do {
					if usePercentages then lsb.append(f"${(photo(c)(p).updateSerial * 1000 + 500) / lastSerial}%7d ")
					else lsb.append(f"${photo(c)(p).updateSerial}%7d ")
				}
			}
			lsb.append("\n<<<<<<<<<<<<<")
			printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
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

		val result = Promise[Long]

		matrix.spawn[Cmd](reactantFactory) { parent =>
			parent.doer.checkWithin()

			parent.doer.Duty.sequenceToArray(
				for consumerIndex <- 0 until NUMBER_OF_CONSUMERS yield {
					parent.spawn[Consumable](reactantFactory) { consumer =>
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
						parent.watch(producer, ProducerWasStopped)
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
						// println(s"Consumer $consumerIndex stopped. Active consumers: ${parent.children.size}")
						Continue
					} else {
						outEndpoint.tell(End)
						Stop
					}
				case ProducerWasStopped =>
					parent.doer.checkWithin()
					activeProducers -= 1
					if activeProducers > 0 || activeConsumers > 0 then {
						// println(s"Consumer $consumerIndex stopped. Active consumers: ${parent.children.size}")
						Continue
					} else {
						outEndpoint.tell(End)
						Stop
					}
			}
		}.trigger() { parent =>
			matrix.doer.checkWithin()

			if true then {
				matrix.doerProvider.addMonitor(() => {
					parent.doer.Duty.mineFlat { () =>
						val childrenDiagnosticsDuties = parent.children.values.map(child => {
							child.diagnose.map(d => s"child ${child.serial}: $d").onBehalfOf(parent.doer)
						})
						val childrenDiagnostics: parent.doer.Duty[Array[String]] = parent.doer.Duty.sequenceToArray(childrenDiagnosticsDuties)
						for {
							parentDiagnostic <- parent.diagnose
							childrenDiagnostic <- childrenDiagnostics
						} yield
							s"""Parent's diagnostic: $parentDiagnostic
							   |Children's diagnostics:\n${childrenDiagnostic.mkString("\n")}\n>>>>""".stripMargin
					}.trigger()(println)
				})
			}

			parent.stopDuty.trigger() { _ =>
				println(s"+++ Total number of non-negative numbers sent to children: ${counter.get()} +++")
				println(s"+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++")

				// println(s"After successful completion diagnostic: ${matrixAide.diagnose()}")
				matrix.doerProvider.shutdown()
				println("Shutdown completed normally")
				result.success(nanoAtEnd - nanoAtStart)
			}
		}
		result.future
	}
}
