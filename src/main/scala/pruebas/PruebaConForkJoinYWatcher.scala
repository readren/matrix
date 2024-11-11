package readren.matrix
package pruebas

import rf.RegularRf

import readren.taskflow.Doer

import java.net.URI
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object PruebaConForkJoinYWatcher {

	val threadLocal: ThreadLocal[Int] = new ThreadLocal[Int]()

	def checkAdmin(admin: MatrixAdmin): Unit = {
//		assert(admin.id == threadLocal.get(), s"admin.id=${admin.id}, threadLocal.get=${threadLocal.get()}")
	}

	private val matrixAide = new Matrix.Aide { thisMatrixAide =>
		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()


		override def buildDoerAssistantForAdmin(adminId: Int): Doer.Assistant = new Doer.Assistant {

			private val doSiThEx = Executors.newSingleThreadExecutor()

			override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute { () =>
				threadLocal.set(adminId)
				runnable.run()
			}

			override def reportFailure(cause: Throwable): Unit = thisMatrixAide.reportFailure(cause)
		}
	}

	sealed trait Cmd

//	case class ChildStopped(id: Int) extends Cmd

	case class Spawn(onChildEndPoint: Endpoint[Int] => Unit, replyTo: Endpoint[String]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	@main def run4(): Unit = {

		val counter: AtomicInteger = new AtomicInteger(0)
		val sb = new StringBuffer(999999)
		val outReceiver = new Receiver[String] {
			override def submit(message: String): Unit = {
				sb.append(s"(${counter.incrementAndGet()})-$message; ")

				if message == "End" then {
					println(sb.toString)
					sb.setLength(0)
					sb.append("\n----- More than one End message received.")
				}
			}

			override def uri: URI = ???
		}
		val outEndpoint = LocalEndpoint(outReceiver)

		val matrix = new Matrix("myMatrix", matrixAide)
		sb.append("Matrix created\n")
		matrix.spawn[Cmd, Spawn](RegularRf(false)) { parent =>
			checkAdmin(parent.admin)
//			val parentEndpointForChild = parent.endpointProvider.local[ChildStopped]
			Behavior.messageAndSignal {

				case spawn@Spawn(onChildEndPoint, replyTo) =>
					checkAdmin(parent.admin)
					parent.spawn[Int](RegularRf(true)) { child =>
						checkAdmin(child.admin)
						Behavior.ignore.withMsgBehavior { n =>
							checkAdmin(child.admin)
							if n >= 0 then {
								replyTo.tell(s"${child.admin.id} <= $n")
								Continue
							} else {
//								parentEndpointForChild.tell(ChildStopped(child.serial))
								Stop
							}
						}
					}.map { child =>
//						parent.watch(child.serial)
						println(s"Child ${child.serial} spawned. Active children: ${parent.getChildren.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(onChildEndPoint)
					Continue

//				case ChildStopped(childSerial) =>
//					checkAdmin(parent.admin)
//					if parent.getChildren.isEmpty then {
//						outEndpoint.tell("End")
//						Stop
//					} else {
//						println(s"Child $childSerial stopped. Active children: ${parent.getChildren.size}")
//						Continue
//					}

			}(Behavior.handleSignal {
				case ChildStopped(childSerial) =>
					checkAdmin(parent.admin)
					if parent.getChildren.isEmpty then {
						outEndpoint.tell("End")
						Stop
					} else {
						println(s"Child $childSerial stopped. Active children: ${parent.getChildren.size}")
						Continue
					}
				case s =>
					println(s"Received signal: $s")
					Continue
			})
		}.trigger() { parentEndpoint =>
			sb.append("Parent started\n")
			val futures = for j <- 0 to 99 yield Future {
				sb.append(s"Future $j begin: ")
				parentEndpoint.tell(Spawn(
					childEndpoint => Future {
						for i <- 0 to 99 do childEndpoint.tell(j * 1000 + i)
						childEndpoint.tell(-1)
					}(ExecutionContext.global),
					outEndpoint
					))
				sb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
				println(s"\nFutures completed: sb=[${sb.toString}]")
			}(ExecutionContext.global)
		}
	}
}
