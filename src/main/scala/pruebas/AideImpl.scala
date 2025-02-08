package readren.matrix
package pruebas

import core.{Logger, Matrix, MatrixDoer}
import logger.SimpleLogger
import providers.ShutdownAbleDpd

class AideImpl[MD <: MatrixDoer](
	defaultDpd: Matrix.DoerProviderDescriptor[MD]
) extends Matrix.Aide[AideImpl[MD]] {
	override type DPsManager = ShutdownAbleDpd
	override type DefaultDoer = MD
//	override type DefaultDoerProvider = DP
	
	override val defaultDoerProviderDescriptor: Matrix.DoerProviderDescriptor[MD] = defaultDpd

	override val doerProvidersManager: DPsManager = new ShutdownAbleDpd

	override def buildLogger(owner: Matrix[AideImpl[MD]]): Logger = new SimpleLogger(Logger.Level.info)
}
