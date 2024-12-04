package readren.matrix
package pruebas

import rf.{RegularRf, SequentialMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn
import scala.util.Success

object Prueba {

	sealed trait Answer

	case class Response(doer: MatrixDoer, childIndex: Int, text: String) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case class ChildWasStopped(childIndex: Int) extends Cmd, Answer

	case class Spawn(childEndPointReceiver: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	val numberOfChildren = 10000
	val numberOfMessagesPerChild = 100

	@main def runPrueba(): Unit = {
		given ExecutionContext = ExecutionContext.global

		val numberOfWarmUpRepetitions = 10
		val numberOfMeasuredOfRepetitions = 40
		var totalFuture = Future.successful[(Long, Long)]((0L, 0L))
		for i <- 1 to (numberOfWarmUpRepetitions + numberOfMeasuredOfRepetitions) do {
			totalFuture = totalFuture.flatMap { durationAccumulator =>
				println(s"loop #$i")
				for {
					regularRfDuration <- runPrueba(RegularRf, i)
					sequentialMsgBufferDuration <- runPrueba(SequentialMsgBufferRf, i)
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
		println("Key caught. Main thread is ending.")
	}

	def runPrueba(reactantFactory: ReactantFactory, loopId: Int): Future[Long] = {

		val matrixAide = new Shared.MatrixAide(true, s"<<<< Executors diagnostic corresponding to: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", matrixAide)
		println(s"Matrix created: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}\n")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val numberOfStringBuilders = numberOfChildren
		val sbs: Array[StringBuilder] = Array.fill(numberOfStringBuilders)(new StringBuilder(99999))

		var nanoAtEnd: Long = 0
		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Answer] {
			case response: Response =>
				response.doer.checkWithin()
				val counterValue = counter.incrementAndGet()
				val childSb = sbs(response.childIndex)
				childSb.append(f"($counterValue%6d)-${response.text}; ")

				if false then {
					println(f"${response.childIndex}%3d: $childSb)")
				}
				if false then {
					val lsb = new StringBuilder(99999)
					lsb.append("\n>>>>>>>>>>>>>")
					for i <- 0 until numberOfStringBuilders if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
					lsb.append("\n<<<<<<<<<<<<<")
					printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
				}

			case ChildWasStopped(childIndex) =>
				val childSb = sbs(childIndex)
				childSb.append(f"(${counter.get()}%6d) <| Stopped; ")
				if false then {
					println(f"$childIndex%3d: $childSb)")
				}

			case End =>
				nanoAtEnd = System.nanoTime()
				val lsb = new StringBuilder(1024)
				lsb.append(s"\n+++ Total number of non-negative numbers sent to children: ${counter.get()} +++\n")
				lsb.append(s"\n+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++\n")
				if false then {
					for i <- 0 until numberOfStringBuilders do {
						lsb.append(f"$i%4d: ${sbs(i)}\n")
						sbs(i).setLength(0)
					}
				}
				println(lsb)
		}

		val result = Promise[Long]

		matrix.spawn[Cmd](reactantFactory) { parent =>
			parent.doer.checkWithin()
			val parentEndpointForChild = parent.endpointProvider.local[ChildWasStopped]
			var childrenCount = 0
			val childrenIndexSeq = new AtomicInteger(0)
			Behavior.factory {
				case spawn@Spawn(childEndPointReceiver, replyTo) =>
					parent.doer.checkWithin()
					val childIndex = childrenIndexSeq.getAndIncrement()
					parent.spawn[Int](reactantFactory) { child =>
						child.doer.checkWithin()

						Behavior.factory { (n: Int) =>
							child.doer.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.doer, childIndex, f"$childIndex%4d <=$n%4d"))
								Continue
							} else {
								parentEndpointForChild.tell(ChildWasStopped(childIndex))
								Stop
							}
						}
					}.map { child =>
						parent.doer.checkWithin()
						childrenCount += 1
						// println(s"Child ${child.serial} spawned. Active children: ${parent.children.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(childEndPointReceiver)
					Continue

				case cws@ChildWasStopped(childIndex) =>
					parent.doer.checkWithin()
					childrenCount -= 1
					outEndpoint.tell(cws)
					if childrenCount == 0 then {
						outEndpoint.tell(End)
						Stop
					} else {
						// println(s"Child $childIndex stopped. Active children: $childrenCount")
						Continue
					}

			}
		}.trigger() { parent =>
			matrix.doer.checkWithin()
			val parentEndpoint = parent.endpointProvider.local[Spawn]

			val csb: StringBuffer = new StringBuffer(9999)
			csb.append("Parent started\n")
			val futures = for j <- 0 until numberOfChildren yield Future {
				csb.append(s"Future $j begin: ")
				parentEndpoint.tell(Spawn(
					childEndpoint => Future {
						for i <- 0 until numberOfMessagesPerChild do childEndpoint.tell(i)
						childEndpoint.tell(-1)
					}(ExecutionContext.global),
					outEndpoint
				))
				csb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			if false then {
				Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
					println(s"\nFutures completed: csb=[${csb.toString}]")
				}(ExecutionContext.global)
			}
			if true then {
				matrixAide.addMonitor(() => {
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
				println(s"After successful completion diagnostic: ${matrixAide.diagnose()}")
				matrixAide.shutdown().thenRun { () =>
					println("Shutdown completed normally")
					result.success(nanoAtEnd - nanoAtStart)
				}
			}
		}
		result.future
	}
}
