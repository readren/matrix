package readren.matrix
package pruebas

import rf.{RegularRf, SynchronousMsgBufferRf}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn

object PruebaConWatcher {

	sealed trait Answer

	case class Response(adminId: Int, childSerial: Int, text: String) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case class ChildWasStopped(childSerial: Int) extends Cmd, Answer

	case class Spawn(childEndPointReceiver: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	val numberOfChildren = 100
	val numberOfMessagesPerChild = 100

	@main def runPruebaConWatcher(): Unit = {
		given ExecutionContext = ExecutionContext.global


		var totalFuture = Future.successful(())
		for i <- 1 to 4 do {
			totalFuture = totalFuture.flatMap { _ =>
				println(s"loop #$i")
				for {
					_ <- run(RegularRf)
					_ <- run(SynchronousMsgBufferRf)
				} yield ()
			}
		}
		totalFuture.andThen(x => println(s"All matrix were shutdown returning: $x\nPress <enter> to exit"))

		StdIn.readLine()
	}

	def run(reactantFactory: ReactantFactory): Future[Unit] = {
		val csb: StringBuffer = new StringBuffer(2048)

		val matrixAide = new Shared.MatrixAide
		val matrix = new Matrix("myMatrix", matrixAide)
		csb.append("Matrix created\n")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val numberOfStringBuilders = numberOfChildren + 3
		val sbs: Array[StringBuilder] = Array.fill(numberOfStringBuilders)(new StringBuilder(9999))

		// SynchronousMsgBufferRf
		val nanoAtStart = System.nanoTime()
		val outEndpoint = matrix.buildEndpoint[Answer] {
			case message: Response =>
				val counterValue = counter.incrementAndGet()
				sbs(message.childSerial).append(f"($counterValue%6d)-${message.text}; ")

				if false then {
					val lsb = new StringBuilder(99999)
					lsb.append("\n>>>>>>>>>>>>>")
					for i <- 0 until numberOfStringBuilders if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
					lsb.append("\n------------")
					printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
				}

			case ChildWasStopped(childSerial) =>
				sbs(childSerial).append(f"(${counter.get()}%6d) <| Stopped; ")

			case End =>
				val nanoAtEnd = System.nanoTime()
				val lsb = new StringBuilder(1024)
				lsb.append(s"\n+++ Total number of non-negative numbers sent to children: ${counter.get()} +++\n")
				lsb.append(s"\n+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++\n")
				if true then {
					for i <- 0 until numberOfStringBuilders do {
						lsb.append(f"$i%3d: ${sbs(i)}\n")
						sbs(i).setLength(0)
					}
				}
				println(lsb)
		}

		val result = Promise[Unit]

		matrix.spawn[Cmd](reactantFactory) { parent =>
			parent.admin.checkWithin()
			val parentEndpointForChild = parent.endpointProvider.local[ChildWasStopped]
			Behavior.factory {
				case spawn@Spawn(childEndPointReceiver, replyTo) =>
					parent.admin.checkWithin()
					parent.spawn[Int](reactantFactory) { child =>
						child.admin.checkWithin()
						Behavior.superviseNest(Behavior.factory { (n: Int) =>
							child.admin.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.admin.id, child.serial, f"${child.serial}%3d <=$n%3d"))

								if n == 9 && (child.serial % 10) == 1 then throw new Exception("a ver que onda")

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
			val parentEndpoint = parent.endpointProvider.local[Spawn]
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
