package readren.matrix
package interactive

import behaviors.CheckedBehavior
import core.{Continue, Endpoint, Matrix}
import rf.RegularRf

import readren.sequencer.manager.ShutdownAbleDpm
import readren.sequencer.manager.descriptors.DefaultCooperativeWorkersDpd

import java.net.URI

object PruebaChecked {

	private case class Response(text: String)

	private trait Cmd

	private case class DoWork(integer: Int, replyTo: Endpoint[Response]) extends Cmd

	private case class Relax(replyTo: Endpoint[Response]) extends Cmd
	
	private class MyException extends Exception

	@main def runPruebaChecked(): Unit = {

		val uri = new URI(null, "localhost", null, null)
		val manager = new ShutdownAbleDpm
		val rootDoer = manager.provideDoer(DefaultCooperativeWorkersDpd, "root")
		val matrix = new Matrix(uri, rootDoer, manager)

		val parentDoer = matrix.provideDoer(DefaultCooperativeWorkersDpd, "parent")
		matrix.spawns[Cmd, parentDoer.type](RegularRf, parentDoer) { parent =>
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
				if response.text eq null then manager.shutdown()
				else println(response)
			}
			for i <- 0 to 20 do {
				parentEndpoint.tell(DoWork(i, outEndpoint))
			}
			parentEndpoint.tell(Relax(outEndpoint))
		}

	}
}
