package readren.matrix
package pruebas

import behaviors.CheckedBehavior
import core.Matrix.DoerProviderDescriptor
import core.{Continue, Endpoint, Matrix, MatrixDoer}
import providers.doer.SharedQueueDoerProvider
import rf.RegularRf

object PruebaChecked {

	private case class Response(text: String)

	private trait Cmd

	private case class DoWork(integer: Int, replyTo: Endpoint[Response]) extends Cmd

	private case class Relax(replyTo: Endpoint[Response]) extends Cmd
	
	private class MyException extends Exception
	private class NeverException extends MyException

	private object sharedQueueDpd extends DoerProviderDescriptor[MatrixDoer, SharedQueueDoerProvider]("sharedQueue") {
		override def build(owner: Matrix.DoerProvidersManager): SharedQueueDoerProvider = new SharedQueueDoerProvider(false)
	}

	@main def runPruebaChecked(): Unit = {

		val matrixAide = new AideImpl(sharedQueueDpd)

		val matrix = new Matrix("testChecked", matrixAide)

		matrix.spawn[Cmd](RegularRf) { parent =>
			val cb: CheckedBehavior[Cmd, MyException] =
				CheckedBehavior.factory[Cmd, MyException, NeverException] {
					case cmd: DoWork =>
						if (cmd.integer % 5) >= 3 then throw new MyException
						cmd.replyTo.tell(Response(cmd.integer.toString))
						Continue
					case Relax(replyTo) =>
						replyTo.tell(Response(null))
						Continue
				}
			CheckedBehavior.makeSafe(cb) { (m: MyException) => cb }
		}.trigger() { parent =>
			val parentEndpoint = parent.endpointProvider.local
			val outEndpoint = matrix.buildEndpoint[Response] { response =>
				if response.text == null then matrix.doerProvidersManager.shutdown()
				else println(response)
			}
			for i <- 0 to 20 do {
				parentEndpoint.tell(DoWork(i, outEndpoint))
			}
			parentEndpoint.tell(Relax(outEndpoint))
		}

	}
}
