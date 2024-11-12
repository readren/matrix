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

	case class Response(adminId: Int, text: String)

	sealed trait Cmd

	case class ChildStopped(serial: Int, adminId: Int) extends Cmd

	case class Spawn(onChildEndPoint: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	@main def run(): Unit = {
		val csb: StringBuffer = new StringBuffer(99999)
		val matrix = new Matrix("myMatrix", matrixAide)
		csb.append("Matrix created\n")
		val numOfAdmins: Int = matrix.matrixAdmins.length

		val counter: AtomicInteger = new AtomicInteger(0)
		val sbs: Array[StringBuilder] = Array.fill(numOfAdmins + 1)(new StringBuilder(9999))
		val outReceiver = new Receiver[Response] {
			override def submit(message: Response): Unit = {
				val counterValue = counter.incrementAndGet()
				sbs(message.adminId).append(f"($counterValue%6d)-${message.text}; ")
				if false && (counterValue % 100) == 0 then {
					val lsb = new StringBuilder(99999)
					for i <- 1 to numOfAdmins do lsb.append(f"\n$i%2d: ${sbs(i).toString}")
					lsb.append("\n------------")
					println(lsb.toString())
				}
				if message.text == "End" then {
					for i <- 0 to numOfAdmins do {
						println(f"$i%2d: ${sbs(i)}")
						sbs(i).setLength(0)
					}
					sbs(0).append("\n----- More than one End message received.")
					matrixAide.shutdown()
				}
			}

			override def uri: URI = ???
		}
		val outEndpoint = LocalEndpoint(outReceiver)

		csb.append("Matrix created\n")
		matrix.spawn[Cmd, Spawn](RegularRf(false)) { parent =>
			parent.admin.checkWithin()
			val parentEndpointForChild = parent.endpointProvider.local[ChildStopped]
			Behavior.messageAndSignal {

				case spawn@Spawn(onChildEndPoint, replyTo) =>
					parent.admin.checkWithin()
					parent.spawn[Int](RegularRf(false)) { child =>
						child.admin.checkWithin()
						Behavior.ignore.withMsgBehavior { n =>
							child.admin.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.admin.id, f"${child.serial}%3d <= $n%6d"))
								Continue
							} else {
								parentEndpointForChild.tell(ChildStopped(child.serial, child.admin.id))
								Stop
							}
						}
					}.map { child =>
						parent.watch(child.serial)
						println(s"Child ${child.serial} spawned. Active children: ${parent.getChildren.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(onChildEndPoint)
					Continue

				case ChildStopped(childSerial, adminId) =>
					parent.admin.checkWithin()
					outEndpoint.tell(Response(adminId, f"$childSerial%3d | Stopped"))
					if parent.getChildren.isEmpty then {
						outEndpoint.tell(Response(0, "End"))
						println("\nParent was stopped --------")
						Stop
					} else {
						println(s"Child $childSerial stopped. Active children: ${parent.getChildren.size}")
						Continue
					}

			}(Behavior.handleSignal { s =>
				parent.admin.checkWithin()
				println(s"Parent has received the signal: $s")
				Continue
			})
		}.trigger() { parentEndpoint =>
			csb.append("Parent started\n")
			val futures = for j <- 0 to 99 yield Future {
				csb.append(s"Future $j begin: ")
				parentEndpoint.tell(Spawn(
					childEndpoint => Future {
						for i <- 0 to 99 do childEndpoint.tell(j * 1000 + i)
						childEndpoint.tell(-1)
					}(ExecutionContext.global),
					outEndpoint
					))
				csb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
				println(s"\nFutures completed: csb=[${csb.toString}]")
			}(ExecutionContext.global)
		}
	}
}
