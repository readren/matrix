package readren.matrix
package pruebas

import core.{Logger, Matrix}
import dap.CloseableDoerAssistantProvidersManager
import logger.SimpleLogger

import readren.taskflow.Doer

class AideImpl[Dap <: Matrix.DoerAssistantProvider](
	override val defaultDapKind: Matrix.DapKind[Dap]
) extends Matrix.Aide[AideImpl[Dap]] {
	override type DapMan = CloseableDoerAssistantProvidersManager
	override type DefaultDap = Dap

	override def buildDoerAssistantProviderManager(owner: Matrix[AideImpl[Dap]]): DapMan = new CloseableDoerAssistantProvidersManager

	override def buildLogger(owner: Matrix[AideImpl[Dap]]): Logger = new SimpleLogger(Logger.Level.debug)
}
