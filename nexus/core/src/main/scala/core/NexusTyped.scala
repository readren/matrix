package readren.nexus
package core

import logger.SimpleLogger

import readren.sequencer.Doer
import readren.sequencer.manager.{DoerProviderDescriptor, DoerProvidersManager}

import java.net.URI

/**
 * A nexus of [[Actant]] instances.
 *
 * Serves as the central hub for autonomous Actant components.
 * While the Actant instances manage their own internal behavior, the Nexus orchestrates their creation, registration, and overall coordination, using the execution services provided by the [[DoerProvidersManager]].
 */
class NexusTyped[+MD <: Doer](
	override val uri: URI,
	override val doer: MD,
	val doerProvidersManager: DoerProvidersManager,
	override val logger: Logger = SimpleLogger(Logger.Level.debug)
) extends Nexus(uri.getHost) { thisNexus =>
	override protected val spawner: Spawner[doer.type] =
		new Spawner(this, doer, 0)

	override def provideDoer[D <: Doer](descriptor: DoerProviderDescriptor[D], tag: descriptor.Tag): D =
		doerProvidersManager.provideDoer(descriptor, tag)

	override def provideDoer[D <: Doer](text: String, descriptor: DoerProviderDescriptor[D]): D = {
		val provider = doerProvidersManager.get(descriptor)
		provider.provide(provider.tagFromText(text))
	}
}
