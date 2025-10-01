package readren.matrix
package core

import logger.SimpleLogger

import readren.sequencer.Doer
import readren.sequencer.manager.{DoerProviderDescriptor, DoerProvidersManager}

import java.net.URI


class Matrix[MD <: Doer](
	override val uri: URI,
	override val doer: MD,
	val doerProvidersManager: DoerProvidersManager,
	override val logger: Logger = SimpleLogger(Logger.Level.debug)
) extends AbstractMatrix(uri.getHost) { thisMatrix =>
	override type MatrixDoerType = MD

	override protected val spawner: Spawner[doer.type] =
		new Spawner(this, doer, 0)

	override def provideDoer[D <: Doer](descriptor: DoerProviderDescriptor[D], tag: descriptor.Tag): D =
		doerProvidersManager.provideDoer(descriptor, tag)

	override def provideDoer[D <: Doer](text: String, descriptor: DoerProviderDescriptor[D]): D = {
		val provider = doerProvidersManager.get(descriptor)
		provider.provide(provider.tagFromText(text))
	}
}
