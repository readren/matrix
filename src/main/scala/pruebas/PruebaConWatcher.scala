package readren.matrix
package pruebas

import rf.{RegularRf, SequentialMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.Success

object PruebaConWatcher {

	sealed trait Answer

	case class Response(admin: MatrixAdmin, producerIndex: Int, consumerIndex: Int, text: String) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case object ProducerWasStopped extends Cmd
	case class ConsumerWasStopped(childIndex: Int) extends Cmd, Answer

	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	case class Consumable(producerIndex: Int, value: Int)

	private val NUMBER_OF_PRODUCERS = 1000
	private val NUMBER_OF_CONSUMERS = 1000
	private val NUMBER_OF_MESSAGES_TO_CONSUMER_PER_PRODUCER = 10

	@main def runPruebaConWatcher(): Unit = {
		given ExecutionContext = ExecutionContext.global

		val numberOfWarmUpRepetitions = 4
		val numberOfMeasuredOfRepetitions = 16
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
		totalFuture.andThen { case Success(totalDuration) =>
			println(
				s"""All matrix were shutdown
				   |Average duration: regularRf-> ${totalDuration._1 / (numberOfMeasuredOfRepetitions * 1000000)}, sequentialMsgBufferRfTotalDuration-> ${totalDuration._2 / (numberOfMeasuredOfRepetitions * 1000000)}
				   |Press <enter> to exit""".stripMargin
			)
		}

		StdIn.readLine()
	}

	def run(reactantFactory: ReactantFactory, loopId: Int): Future[Long] = {

		val matrixAide = new Shared.MatrixAide(true, s"Executors diagnostic corresponding to: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", matrixAide)
		println(s"Matrix created: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val sbs: Array[StringBuilder] = Array.fill(NUMBER_OF_CONSUMERS)(new StringBuilder(99999))

		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Answer] {
			case response: Response =>
				response.admin.checkWithin()
				val counterValue = counter.incrementAndGet()

				if false then {
					val consumerSb = sbs(response.consumerIndex)
					consumerSb.append(f"($counterValue%6d)${response.text}; ")

					if false then {
						println(f"${response.consumerIndex}%3d: $consumerSb")
					}
					if false then {
						val lsb = new StringBuilder(99999)
						lsb.append("\n>>>>>>>>>>>>>")
						for i <- 0 until NUMBER_OF_CONSUMERS if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
						lsb.append("\n<<<<<<<<<<<<<")
						printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
					}
				}

			case ConsumerWasStopped(consumerIndex) =>
				val consumerSb = sbs(consumerIndex)
				consumerSb.append(f"(${counter.get()}%6d) <| Stopped; ")
				if false then {
					println(f"$consumerIndex%3d: $consumerSb)")
				}

			case End =>
				if false then {
					val lsb = new StringBuilder(1024)
					for i <- 0 until NUMBER_OF_CONSUMERS do {
						lsb.append(f"$i%4d: ${sbs(i)}\n")
					}
					println(lsb)
				}
		}

		val result = Promise[Long]

		matrix.spawn[Cmd](reactantFactory) { parent =>
			parent.admin.checkWithin()

			parent.admin.Duty.sequenceToArray(
				for consumerIndex <- 0 until NUMBER_OF_CONSUMERS yield {
					parent.spawn[Consumable](reactantFactory) { consumer =>
						var completedCounter = 0
						Behavior.factory { consumable =>
							if consumable.value >= 0 then {
								outEndpoint.tell(Response(consumer.admin, consumable.producerIndex, consumerIndex, f"${consumable.value}%4d <-${consumable.producerIndex}%4d"))
								// if consumable.value == 9 && (consumerIndex % 10) == 5 then throw new Exception("a ver que onda")
								Continue
							} else {
								completedCounter += 1
								if completedCounter == NUMBER_OF_PRODUCERS then Stop
								else Continue
							}
						}
					}.map { consumer =>
						parent.admin.checkWithin()
						parent.watch(consumer, ConsumerWasStopped(consumerIndex))
						consumer.endpointProvider.local[Consumable]
					}
				}
			).trigger(true) { consumersEndpoints =>
				parent.admin.checkWithin()
				for producerIndex <- 0 until NUMBER_OF_PRODUCERS do {
					parent.spawn[Started.type | Restarted.type](reactantFactory) { producer =>
						producer.admin.checkWithin()
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
						parent.admin.checkWithin()
						parent.watch(producer, ProducerWasStopped)
					}
				}
			}

			var activeConsumers = NUMBER_OF_CONSUMERS
			var activeProducers = NUMBER_OF_PRODUCERS

			Behavior.factory {
				case cws@ConsumerWasStopped(consumerIndex) =>
					parent.admin.checkWithin()
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
					parent.admin.checkWithin()
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
			matrix.admin.checkWithin()

			if true then {
				matrixAide.addMonitor(() => {
					parent.admin.Duty.mineFlat { () =>
						val childrenDiagnosticsDuties = parent.children.values.map(child => {
							child.diagnose.map(d => s"child ${child.serial}: $d").onBehalfOf(parent.admin)
						})
						val childrenDiagnostics: parent.admin.Duty[Array[String]] = parent.admin.Duty.sequenceToArray(childrenDiagnosticsDuties)
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
				val nanoAtEnd = System.nanoTime()
				println(s"+++ Total number of non-negative numbers sent to children: ${counter.get()} +++")
				println(s"+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++")

				// println(s"After successful completion diagnostic: ${matrixAide.diagnose()}")
				matrixAide.shutdown().thenRun { () =>
					println("Shutdown completed normally")
					result.success(nanoAtEnd - nanoAtStart)
				}
			}
		}
		result.future
	}
}
