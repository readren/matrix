package readren.matrix
package pruebas

import core.Matrix.DoerProviderDescriptor
import core.{Continue, Matrix, Started}
import providers.doer.TimedDoerProvider
import rf.RegularRf
import timed.MatrixTimedDoer

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object PruebaTimed {


	@main def runPruebaTimed(): Unit = {

		case object Tick

		val timedDpd = new DoerProviderDescriptor[MatrixTimedDoer, TimedDoerProvider]("timed") {
			override def build(owner: Matrix.DoerProvidersManager): TimedDoerProvider = new TimedDoerProvider(false)
		}
		val aide = new AideImpl(timedDpd)

		val matrix = new Matrix("timed", aide)

		val timedDoer = matrix.provideDoer(timedDpd)
		matrix.spawns[Started.type | Tick.type](RegularRf, timedDoer) { reactant =>
			val selfEndpoint = reactant.endpointProvider.local[Tick.type]
			val interval = FiniteDuration(1, TimeUnit.SECONDS)
			{
				case m@(Started | Tick) =>
					println(s"A $m was received")
					import timedDoer.*
					timedDoer.Duty.delay(interval)(() => selfEndpoint.tell(Tick)).triggerAndForget()
					Continue
			}
		}.triggerAndForget()
	}
}
