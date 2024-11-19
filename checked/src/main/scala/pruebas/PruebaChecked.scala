package readren.matrix
package pruebas

import rf.RegularRf

object PruebaChecked {

	case class Response(text: String)

	trait Cmd

	case class DoWork(integer: Int, replyTo: Endpoint[Response]) extends Cmd

	case class Relax(replyTo: Endpoint[Response]) extends Cmd
	
	class MyException extends Exception
	class NeverException extends MyException

	@main def runPruebaChecked(): Unit = {

		val matrixAide = new Shared.MatrixAide
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
				if response.text == null then matrixAide.shutdown()
				else println(response)
			}
			for i <- 0 to 20 do {
				parentEndpoint.tell(DoWork(i, outEndpoint))
			}
			parentEndpoint.tell(Relax(outEndpoint))
		}

	}
}
