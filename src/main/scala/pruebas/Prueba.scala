package readren.matrix
package pruebas

import rf.RegularRf

import readren.taskflow.Doer

import java.net.URI
import java.util.concurrent.Executors
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

object Prueba {

	val threadLocal: ThreadLocal[Int] = new ThreadLocal[Int]()
	def checkAdmin(admin: MatrixAdmin): Unit = {
		assert(admin.id == threadLocal.get(), s"admin.id=${admin.id}, threadLocal.get=${threadLocal.get()}")
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
	case class ChildStopped(id: Int) extends Cmd
	case class Spawn(onChildEndPoint: Endpoint[Int] => Unit)(val replyTo: Endpoint[String]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	@main def run(): Unit = {

		var counter: Int = 0
		val sb = new StringBuffer(9999999)
		val outReceiver = new Receiver[String] {
			override def submit(message: String): Unit = {
				counter += 1
				sb.append(s"($counter)-$message; ")
				if message=="End" then println(sb.toString)
			}
			override def uri: URI = ???
		}
		val outEndpoint = LocalEndpoint(outReceiver)

		val matrix = new Matrix("myMatrix", matrixAide)
		sb.append("Begin:")
		matrix.spawn[Cmd, Spawn](RegularRf) { parent =>
			checkAdmin(parent.admin)
			val parentEndpointForChild = parent.endpointProvider.local[ChildStopped]
			var childrenCounter = 0;
			Behavior.messageAndSignal {

				case spawn@Spawn(onChildEndPoint) =>
					childrenCounter += 1
					checkAdmin(parent.admin)
					parent.spawn[Int](RegularRf) { child =>
						val childId = childrenCounter
						checkAdmin(child.admin)
						Behavior.ignore.withMsgBehavior { n =>
							checkAdmin(child.admin)
							if n > 0 then {
								spawn.replyTo.tell(s"${child.admin.id} have received $n")
								Continue
							} else {
								parentEndpointForChild.tell(ChildStopped(childId))
								Stop
							}
						}
					}.map(_.endpointProvider.local[Int])
						.andThen { _ =>
						println(s"Child spawned. Active children: ${parent.getChildren.size}")
					}.trigger(true)(onChildEndPoint)
					Continue

				case ChildStopped(childId) =>
					checkAdmin(parent.admin)
					if parent.getChildren.isEmpty then {
						outEndpoint.tell("End")
						Stop
					} else {
						println(s"Child $childId stopped. Active children: ${parent.getChildren.size}")
						Continue
					}

			}(Behavior.ignore)
		}.trigger() { parentEndpoint =>
			sb.append("Start: ")
			val futures = for j <- 0 to 99 yield Future {
				sb.append(s"Future $j begin: ")
				parentEndpoint.tell(
					Spawn { childEndpoint =>
						Future {
							for i <- 1 to 100 do childEndpoint.tell(j*1000+i)
							childEndpoint.tell(0)
						}(ExecutionContext.global)
					}(outEndpoint))
				sb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
				println(s"\nFutures completed: sb=[${sb.toString}]")
			}(ExecutionContext.global)
		}
	}
}
