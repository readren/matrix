package readren.matrix
package utils

import core.{Logger, Matrix}
import logger.SimpleLogger
import providers.ShutdownAbleDpm

import readren.sequencer.Doer

open class SimpleAide[D <: Doer](
	override val defaultDoerProviderDescriptor: Matrix.DoerProviderDescriptor[D],
) extends Matrix.Aide[SimpleAide[D]] {
	override type DPsManager = ShutdownAbleDpm
	override type DefaultDoer = D
	override val doerProvidersManager: DPsManager = new ShutdownAbleDpm

	override def buildLogger(owner: Matrix[SimpleAide[D]]): Logger = new SimpleLogger(Logger.Level.info)

	override def uriScheme: String = "http"

	override def uriHost: String = "localhost"

	override def uriPort: Int = 8080

}
