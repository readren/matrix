package readren.matrix
package core

import core.Matrix.{DoerProvider, DoerProviderDescriptor}

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.atomic.AtomicLong


object Matrix {

	trait DoerProvider[+D <: MatrixDoer] {
		def provide(matrix: AbstractMatrix): D
	}

	/**
	 * Identifies, and builds instances of, the specified concrete subtype of [[DoerProvider]].
	 * Instances of this class usually are singleton objects.
	 * @param id the identifier of this instance used for equality.
	 * @tparam DP the concrete subtype of the [[DoerAssistantProvider]] that this instance identifies and builds instances of.
	 */
	trait DoerProviderDescriptor[+D <: MatrixDoer, +DP <: DoerProvider[D]](val id: String) extends Equals {
//		type ProvidedDoer = D
//		type DescribedProvider = DP
		/**
		 * Builds a [[DoerProvider]] of the type this instance identifies.
		 * This method is called a single time per [[DoerProvidersManager]].
		 *
		 * @param owner The [[DoerProvidersManager]] that will manage the newly created [[DoerAssistantProvider]].
		 * @return The newly created instance of [[DP]].
		 */
		def build(owner: DoerProvidersManager): DP
		
		override def canEqual(that: Any): Boolean = that.isInstanceOf[DoerProviderDescriptor[?, ?]]

		override def equals(that: Any): Boolean = that match {
			case that: DoerProviderDescriptor[?, ?] => this.id == that.id
			case _ => false
		}

		override val hashCode: Int = id.hashCode()

		override def toString: String = id
	}

	/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DoerProviderDescriptor]]s and corresponding [[DoerProvider]]s that the [[Matrix]] instance uses. */
	trait DoerProvidersManager {

		/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created. */
		def get[D <: MatrixDoer, DP <: DoerProvider[D]](descriptor: DoerProviderDescriptor[D, DP]): DP

		inline def provideDoer[D <: MatrixDoer](matrix: AbstractMatrix, descriptor: DoerProviderDescriptor[D, ?]): D =
			get(descriptor).provide(matrix)
	}

	/** Specifies the aide that a [[Matrix]] instance requires. */
	trait Aide[Self <: Aide[Self]] {
		/** The type of the [[DapManger]] that this aide builds. */
		type DPsManager <: DoerProvidersManager

		/** Type of the doers provided by the [[DefaultDoerProvider]] */
		type DefaultDoer <: MatrixDoer
		
		/** Type of the default [[DoerAssistantProvider]], which is the one used to provide the [[Doer.Assistant]] instance for the [[Matrix.doer]] member. */
		type DefaultDoerProvider <: DoerProvider[DefaultDoer]

		/** The [[DoerProviderDescriptor]] of the default [[DoerAssistantProvider]] */
		val defaultDoerProviderDescriptor: DoerProviderDescriptor[DefaultDoer, DefaultDoerProvider]

		/** @param owner the [[Matrix]] instance that will own the brand new [[DoerProvidersManager]] returned. */
		def buildDoerProviderManager(owner: Matrix[Self]): DPsManager

		def buildLogger(owner: Matrix[Self]): Logger
	}
}

class Matrix[A <: Matrix.Aide[A]](name: String, val aide: A) extends AbstractMatrix(name) { thisMatrix =>
	override type DefaultDoer = aide.DefaultDoer

	val doerProvidersManager: aide.DPsManager = aide.buildDoerProviderManager(thisMatrix)

	override val doer: DefaultDoer =
		provideDefaultDoer

	override protected val spawner: Spawner[doer.type] =
		new Spawner(Maybe.empty, doer, 0)

	override val logger: Logger =
		aide.buildLogger(this)

	override def provideDoer[D <: MatrixDoer](descriptor: DoerProviderDescriptor[D, ?]): D = 
		doerProvidersManager.provideDoer(thisMatrix, descriptor)

	override def provideDefaultDoer: DefaultDoer =
		doerProvidersManager.get(aide.defaultDoerProviderDescriptor).provide(thisMatrix)
		
}
