package readren.matrix
package core

import readren.sequencer.manager.{DoerProviderDescriptor, DoerProvidersManager}
import readren.sequencer.{Doer, DoerProvider}

import java.net.URI


object Matrix {

	/** Specifies the aide that a [[Matrix]] instance requires. */
	trait Aide[Self <: Aide[Self]] {
		/** The type of the [[doerProvidersManager]] field. */
		type DPsManager <: DoerProvidersManager

		/** Type of the doers provided by the default [[DoerProvider]] */
		type DefaultDoer <: Doer

		/** The [[DoerProviderDescriptor]] of the default [[DoerProvider]] */
		val defaultDoerProviderDescriptor: DoerProviderDescriptor[DefaultDoer]

		/** A [[DoerProvidersManager]] required to manage the [[Doer]] instances. */
		val doerProvidersManager: DPsManager

		def buildLogger(owner: Matrix[Self]): Logger

		def uriScheme: String

		def uriHost: String

		def uriPort: Int
	}
}

class Matrix[A <: Matrix.Aide[A]](name: String, val aide: A) extends AbstractMatrix(name) { thisMatrix =>
	override type DefaultDoer = aide.DefaultDoer

	val doerProvidersManager: aide.DPsManager = aide.doerProvidersManager

	override val doer: DefaultDoer =
		provideDefaultDoer(name)

	override protected val spawner: Spawner[doer.type] =
		new Spawner(this, doer, 0)

	override val logger: Logger =
		aide.buildLogger(this)

	override def provideDoer[D <: Doer](tag: DoerProvider.Tag, descriptor: DoerProviderDescriptor[D]): D = 
		doerProvidersManager.provideDoer(tag, descriptor)

	override def provideDefaultDoer(tag: DoerProvider.Tag): DefaultDoer =
		doerProvidersManager.get(aide.defaultDoerProviderDescriptor).provide(tag)

	override val uri: URI = new URI(aide.uriScheme, null, aide.uriHost, aide.uriPort, path, null, null)
		
}
