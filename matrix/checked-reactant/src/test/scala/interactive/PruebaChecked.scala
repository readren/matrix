package readren.matrix
package interactive

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

	@main def runPruebaChecked(): Unit = {

		val matrixAide = SimpleAide(DefaultCooperativeWorkersDpd)

		val matrix = new Matrix("testChecked", matrixAide)

		matrix.spawns[Cmd](RegularRf, matrix.provideDefaultDoer("parent")) { parent =>
			CheckedBehavior.factory[Cmd, MyException] {
				case cmd: DoWork =>
					if (cmd.integer % 5) >= 3 then throw new MyException
					cmd.replyTo.tell(Response(cmd.integer.toString))
					Continue
				case Relax(replyTo) =>
					replyTo.tell(Response(null))
					Continue
			}.recover { (m: MyException) =>
				println(s"Recovering from $m")
				Continue
			}
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
