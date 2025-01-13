package readren.matrix
package pruebas

import core.{Logger, Matrix}
import dap.CloseableDoerAssistantProvidersManager
import logger.SimpleLogger

class AideImpl[DefaultDAP <: Matrix.DoerAssistantProvider](override val defaultDapRef: Matrix.DoerAssistantProviderRef[DefaultDAP]) extends Matrix.Aide[AideImpl[DefaultDAP]] {
	override type DapMan = CloseableDoerAssistantProvidersManager
	override type DefaultDap = DefaultDAP

	override def buildDoerAssistantProviderManager(owner: Matrix[AideImpl[DefaultDAP]]): DapMan = new CloseableDoerAssistantProvidersManager

	override def buildLogger(owner: Matrix[AideImpl[DefaultDAP]]): Logger = new SimpleLogger(Logger.Level.debug)
}
