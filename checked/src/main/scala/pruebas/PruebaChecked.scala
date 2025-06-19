package readren.matrix
package pruebas

import behaviors.CheckedBehavior
import core.{Continue, Endpoint, Matrix}
import providers.descriptor.DefaultCooperativeWorkersDpd
import rf.RegularRf
import utils.SimpleAide

object PruebaChecked {

	private case class Response(text: String)

	private trait Cmd

	private case class DoWork(integer: Int, replyTo: Endpoint[Response]) extends Cmd

	private case class Relax(replyTo: Endpoint[Response]) extends Cmd
	
	private class MyException extends Exception
	private class NeverException extends MyException

	@main def runPruebaChecked(): Unit = {

		val matrixAide = SimpleAide(DefaultCooperativeWorkersDpd)

		val matrix = new Matrix("testChecked", matrixAide)

		matrix.spawns[Cmd](RegularRf) { parent =>
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
				if response.text eq null then matrix.doerProvidersManager.shutdown()
				else println(response)
			}
			for i <- 0 to 20 do {
				parentEndpoint.tell(DoWork(i, outEndpoint))
			}
			parentEndpoint.tell(Relax(outEndpoint))
		}

	}
}
