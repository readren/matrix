package readren.matrix
package pruebas

import rf.{RegularRf, SequentialMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn

object PruebaConWatcher {

	sealed trait Answer

	case class Response(adminId: Int, childSerial: Int, text: String) extends Answer

	case object End extends Answer

	case class ChildWasStopped(childSerial: Int) extends Answer

	sealed trait Cmd

	case class Spawn(childEndPointReceiver: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	val numberOfChildren = 10000
	val numberOfMessagesPerChild = 100

	@main def runPruebaConWatcher(): Unit = {
		given ExecutionContext = ExecutionContext.global


		var totalFuture = Future.successful(())
		for i <- 1 to 4 do {
			totalFuture = totalFuture.flatMap { _ =>
				println(s"loop #$i")
				for {
					_ <- run(RegularRf, i)
					_ <- run(SequentialMsgBufferRf, i)
				} yield ()
			}
		}
		totalFuture.andThen(x => println(s"All matrix were shutdown returning: $x\nPress <enter> to exit"))

		StdIn.readLine()
	}

	def run(reactantFactory: ReactantFactory, loopId: Int): Future[Unit] = {

		val matrixAide = new Shared.MatrixAide(true, s"loop: $loopId, factory: ${reactantFactory.getClass.getSimpleName}")
		val matrix = new Matrix("myMatrix", matrixAide)
		println(s"Matrix created: loop=$loopId, factory=${reactantFactory.getClass.getSimpleName}\n")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val numberOfStringBuilders = numberOfChildren + 3
		val sbs: Array[StringBuilder] = Array.fill(numberOfStringBuilders)(new StringBuilder(9999))

		// SynchronousMsgBufferRf
		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Answer] {
			case response: Response =>
				val counterValue = counter.incrementAndGet()
				val childSb = sbs(response.childSerial)
				childSb.append(f"($counterValue%6d)-${response.text}; ")

				if false then {
					println(f"${response.childSerial}%3d: $childSb)")
				}
				if false then {
					val lsb = new StringBuilder(99999)
					lsb.append("\n>>>>>>>>>>>>>")
					for i <- 0 until numberOfStringBuilders if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
					lsb.append("\n<<<<<<<<<<<<<")
					printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
				}

			case ChildWasStopped(childSerial) =>
				sbs(childSerial).append(f"(${counter.get()}%6d) <| Stopped; ")

			case End =>
				val nanoAtEnd = System.nanoTime()
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

		val result = Promise[Unit]

		matrix.spawn[Cmd | ChildStopped](reactantFactory) { parent =>
			parent.admin.checkWithin()
			Behavior.factory {
				case spawn@Spawn(childEndPointReceiver, replyTo) =>
					parent.admin.checkWithin()
					parent.spawn[Int](reactantFactory) { child =>
						child.admin.checkWithin()
						Behavior.superviseNest(Behavior.factory { (n: Int) =>
							child.admin.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.admin.id, child.serial, f"${child.serial}%3d <=$n%3d"))

								if n == 9 && (child.serial % 10) == 5 then throw new Exception("a ver que onda")

								Continue
							} else {
								Stop
							}
						})
					}.map { child =>
						parent.admin.checkWithin()
						parent.watch(child.serial)
						// println(s"Child ${child.serial} spawned. Active children: ${parent.children.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(childEndPointReceiver)
					Continue

				case ChildStopped(childSerial) =>
					parent.admin.checkWithin()
					outEndpoint.tell(ChildWasStopped(childSerial))
					if parent.children.isEmpty then {
						outEndpoint.tell(End)
						Stop
					} else {
						// println(s"Child $childSerial stopped. Active children: ${parent.children.size}")
						Continue
					}
				case s: Signal =>
					println(s"Received signal: $s")
					Continue
			}
		}.trigger() { parent =>
			matrix.admin.checkWithin()
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
					parent.admin.Duty.mineFlat { () =>
						val childrenDiagnosticsDuties = parent.children.values.map(child => {
							child.diagnose.map(d => s"child ${child.serial}: $d").onBehalfOf(parent.admin)
						})
						val childrenDiagnostics: parent.admin.Duty[Array[String]] = parent.admin.Duty.sequenceToArray(childrenDiagnosticsDuties)
						for {
							parentDiagnostic <- parent.diagnose
							childrenDiagnostic <- childrenDiagnostics
						} yield
							s"""
							   |Parent's diagnostic: $parentDiagnostic
							   |Children's diagnostics:\n${childrenDiagnostic.mkString("\n")}""".stripMargin
					}.trigger()(println)
				})
			}

			parent.stopDuty.trigger() { _ =>
				matrixAide.shutdown().thenRun { () =>
					println("Shutdown completed normally")
					result.success(())
				}
			}
		}
		result.future
	}
}
