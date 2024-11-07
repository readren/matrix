package readren.matrix
package pruebas

import rf.RegularRf

import readren.taskflow.Doer

import java.util.concurrent.Executors

object Prueba {

	val matrixAide = new Matrix.Aide { thisMatrixAide =>
		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()

		override def buildDoerAssistantForAdmin(): Doer.Assistant = new Doer.Assistant {
			val doSiThEx = Executors.newSingleThreadExecutor()

			override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

			override def reportFailure(cause: Throwable): Unit = thisMatrixAide.reportFailure(cause)
		}
	}

	sealed trait Cmd

	case class Spawn(onChildEndPoint: Endpoint[Int] => Unit)(val onResponse: String => Unit) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	@main def run(): Unit = {
		val matrix = new Matrix("myMatrix", matrixAide)

		matrix.spawn[Cmd, Spawn](RegularRf) { parent =>
			//		val parentEndpointForChild = parent.endpointProvider.local[Send]
			new Behavior[Cmd] {
				override def handleMessage(message: Cmd): HandleMsgResult[Cmd] = message match {
					case spawn@Spawn(onChildEndPoint) =>
						parent.spawn[Int, Int](RegularRf) { child =>
							//					onChildEndPoint(child.endpointProvider.local)
							new Behavior[Int] {
								override def handleMessage(n: Int): HandleMsgResult[Int] = {
									spawn.onResponse(s"Have received $n")
									Continue
								}
							}
						}.trigger(true)(onChildEndPoint)
						Continue
				}
			}
		}.trigger(false) { parentEndpoint =>
			parentEndpoint.tell(
				Spawn { childEndpoint =>
					for i <- 1 to 100 do childEndpoint.tell(i)
				} { response =>
					print(s"$response, ")
				}
				)
		}
	}
}
