package readren.matrix
package pruebas

import dap.CloseableDoerAssistantProvidersManager

class AideImpl[DefaultDAP <: Matrix.DoerAssistantProvider](override val defaultDapRef: Matrix.DoerAssistantProviderRef[DefaultDAP]) extends Matrix.Aide[AideImpl[DefaultDAP]] {
	override type DapMan = CloseableDoerAssistantProvidersManager
	override type DefaultDap = DefaultDAP

	override def buildDoerAssistantProviderManager(owner: Matrix[AideImpl[DefaultDAP]]): DapMan = new CloseableDoerAssistantProvidersManager

	override def buildLogger(owner: Matrix[AideImpl[DefaultDAP]]): _root_.readren.matrix.Logger = new SimpleLogger(Logger.Level.info)
}
