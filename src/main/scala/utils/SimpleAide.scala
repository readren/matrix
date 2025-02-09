package readren.matrix
package utils

import core.{Logger, Matrix, MatrixDoer}
import logger.SimpleLogger
import providers.ShutdownAbleDpm

class SimpleAide[MD <: MatrixDoer](
	override val defaultDoerProviderDescriptor: Matrix.DoerProviderDescriptor[MD],
) extends Matrix.Aide[SimpleAide[MD]] {
	override type DPsManager = ShutdownAbleDpm
	override type DefaultDoer = MD
	override val doerProvidersManager: DPsManager = new ShutdownAbleDpm

	override def buildLogger(owner: Matrix[SimpleAide[MD]]): Logger = new SimpleLogger(Logger.Level.info)
}
