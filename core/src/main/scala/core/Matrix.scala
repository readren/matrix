package readren.matrix
package core

import core.Matrix.DoerProviderDescriptor
import readren.sequencer.providers.DoerProvider

import readren.common.Maybe
import readren.sequencer.Doer

import java.net.{InetAddress, URI}
import scala.compiletime.asMatchable


object Matrix {

	/**
	 * Identifies and builds an instance of [[DoerProvider]].
	 * Subtypes of this trait usually are singleton objects.
	 * @param id the identifier of this instance used for equality.
	 * @tparam D the subtype of [[Doer]] provided by the [[DoerProvider]] this instance identifies (and builds an instances of).
	 */
	trait DoerProviderDescriptor[+D <: Doer](val id: String) extends Equals {
		/**
		 * Builds a [[DoerProvider]] of the type this instance identifies.
		 * This method is called a single time per [[DoerProvidersManager]].
		 *
		 * @param owner The [[DoerProvidersManager]] that will manage the newly created [[DoerProvider]].
		 * @return The newly created [[DoerProvider]].
		 */
		def build(owner: DoerProvidersManager): DoerProvider[D]
		
		override def canEqual(that: Any): Boolean = that.isInstanceOf[DoerProviderDescriptor[?]]

		override def equals(that: Any): Boolean = that.asMatchable match {
			case that: DoerProviderDescriptor[?] => this.id == that.id
			case _ => false
		}

		override val hashCode: Int = id.hashCode()

		override def toString: String = id
	}

	/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DoerProviderDescriptor]]s and corresponding [[DoerProvider]]s that the [[Matrix]] instance uses. */
	trait DoerProvidersManager {

		/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created. */
		def get[D <: Doer](descriptor: DoerProviderDescriptor[D]): DoerProvider[D]

		inline def provideDoer[D <: Doer](tag: DoerProvider.Tag, descriptor: DoerProviderDescriptor[D]): D =
			get(descriptor).provide(tag)
	}

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
