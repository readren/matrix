package readren.nexus
package interactive

import behaviors.CheckedBehavior
import core.{Continue, Endpoint, NexusTyped}
import rf.RegularSf

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
		val nexus = new NexusTyped(uri, rootDoer, manager)

		val parentDoer = nexus.provideDoer(DefaultCooperativeWorkersDpd, "parent")
		nexus.spawns[Cmd, parentDoer.type](RegularSf, parentDoer) { parent =>
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
			val outEndpoint = nexus.buildEndpoint[Response] { response =>
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
