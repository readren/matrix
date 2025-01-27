package readren.matrix
package pruebas

import core.{Logger, Matrix, MatrixDoer}
import providers.CloseableDoerProvidersManager
import logger.SimpleLogger

import readren.taskflow.Doer

class AideImpl[D <: MatrixDoer, DP <: Matrix.DoerProvider[D]](
	defaultDpd: Matrix.DoerProviderDescriptor[D, DP]
) extends Matrix.Aide[AideImpl[D, DP]] {
	override type DPsManager = CloseableDoerProvidersManager
	override type DefaultDoer = D
	override type DefaultDoerProvider = DP
	
	override val defaultDoerProviderDescriptor: Matrix.DoerProviderDescriptor[D, DP] = defaultDpd

	override def buildDoerProviderManager(owner: Matrix[AideImpl[D, DP]]): DPsManager = new CloseableDoerProvidersManager

	override def buildLogger(owner: Matrix[AideImpl[D, DP]]): Logger = new SimpleLogger(Logger.Level.debug)
}
