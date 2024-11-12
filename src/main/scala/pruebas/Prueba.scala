package readren.matrix
package pruebas

import rf.RegularRf

import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, TimeUnit}
import readren.taskflow.Doer

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object Prueba {

	class MatrixAide extends Matrix.Aide { thisMatrixAide =>
		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()

		private val executors: ArrayBuffer[ExecutorService] = ArrayBuffer.empty

		override def buildDoerAssistantForAdmin(adminId: Int): Doer.Assistant = new Doer.Assistant {

			private val doSiThEx = {
				val newExecutor = Executors.newSingleThreadExecutor()
				executors.addOne(newExecutor)
				newExecutor
			}

			override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

			override def reportFailure(cause: Throwable): Unit = thisMatrixAide.reportFailure(cause)
		}

		def shutdown(): Unit = {
			CompletableFuture.runAsync(
				() => executors.foreach(_.shutdown()),
				CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS)
				)
		}
	}

	private val matrixAide = new MatrixAide

	sealed trait Answer

	case class Response(adminId: Int, childSerial: Int, text: String) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case class ChildWasStopped(serial: Int) extends Cmd, Answer

	case class Spawn(childEndPointReceiver: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	val numberOfChildren = 100

	@main def runPrueba(): Unit = {
		val csb: StringBuffer = new StringBuffer(99999)
		val matrix = new Matrix("myMatrix", matrixAide)
		csb.append("Matrix created\n")
		val numberOfStringBuilders = numberOfChildren + 3

		var printer: Future[Unit] = Future.successful(())

		val counter: AtomicInteger = new AtomicInteger(0)
		val sbs: Array[StringBuilder] = Array.fill(numberOfStringBuilders)(new StringBuilder(9999))
		val outReceiver = new Receiver[Answer] {
			override def submit(answer: Answer): Unit = {
				val lsb = new StringBuilder(99999)
				answer match {
					case message: Response =>
						val counterValue = counter.incrementAndGet()
						sbs(message.childSerial).append(f"($counterValue%6d)-${message.text}; ")

						if false && (counterValue % 1) == 0 then {
							lsb.append("\n>>>>>>>>>>>>>")
							for i <- 1 until numberOfStringBuilders if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
							lsb.append("\n------------")
							printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
							lsb.setLength(0)
						}

					case ChildWasStopped(childSerial) =>
						sbs(childSerial).append(f"(${counter.get()}%6d) <| Stopped; ")

					case End =>
						lsb.append(s"\n+++++++++ ${counter.get()}  +++++++++++++\n")
						for i <- 0 until numberOfStringBuilders do {
							lsb.append(f"$i%3d: ${sbs(i)}\n")
							sbs(i).setLength(0)
						}
						println(lsb)
						matrixAide.shutdown()
				}
			}

			override def uri: URI = ???
		}
		val outEndpoint = LocalEndpoint(outReceiver)

		csb.append("Matrix created\n")
		matrix.spawn[Cmd, Spawn](RegularRf) { parent =>
			parent.admin.checkWithin()
			val parentEndpointForChild = parent.endpointProvider.local[ChildWasStopped]
			var childrenCount = 0
			Behavior.messageAndSignal {

				case spawn@Spawn(childEndPointReceiver, replyTo) =>
					parent.admin.checkWithin()
					parent.spawn[Int](RegularRf) { child =>
						child.admin.checkWithin()
						Behavior.ignore.withMsgBehavior { n =>
							child.admin.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.admin.id, child.serial, f"${child.serial}%3d <=$n%3d"))
								Continue
							} else {
								parentEndpointForChild.tell(ChildWasStopped(child.serial))
								Stop
							}
						}
					}.map { child =>
						parent.watch(child.serial)
						childrenCount += 1
						println(s"Child ${child.serial} spawned. Active children: ${parent.children.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(childEndPointReceiver)
					Continue

				case cws@ChildWasStopped(childSerial) =>
					parent.admin.checkWithin()
					childrenCount -= 1
					outEndpoint.tell(cws)
					if childrenCount == 0 then {
						outEndpoint.tell(End)
						Stop
					} else {
						println(s"Child $childSerial stopped. Active children: $childrenCount")
						Continue
					}

			}(Behavior.handleSignal { s =>
				parent.admin.checkWithin()
				println(s"Parent has received the signal: $s")
				Continue
			})
		}.trigger() { parentEndpoint =>
			csb.append("Parent started\n")
			val futures = for j <- 0 until numberOfChildren yield Future {
				csb.append(s"Future $j begin: ")
				parentEndpoint.tell(Spawn(
					childEndpoint => Future {
						for i <- 0 to 99 do childEndpoint.tell(i)
						childEndpoint.tell(-1)
					}(ExecutionContext.global),
					outEndpoint
					))
				csb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
				println(s"\nFutures completed: sb=[${csb.toString}]")
			}(ExecutionContext.global)
		}
	}
}
