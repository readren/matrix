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
	 * Identifies and builds an instance of [[DoerProvider]].
	 * Concrete subtypes of this trait usually are singleton objects.
	 * @param id the identifier of this instance used for equality.
	 * @tparam MD the concrete subtype of the [[MatrixDoer]]s provided by the [[DoerProvider]] this instance identifies (and builds an instances of).
	 */
	trait DoerProviderDescriptor[+MD <: MatrixDoer](val id: String) extends Equals {
		/**
		 * Builds a [[DoerProvider]] of the type this instance identifies.
		 * This method is called a single time per [[DoerProvidersManager]].
		 *
		 * @param owner The [[DoerProvidersManager]] that will manage the newly created [[DoerAssistantProvider]].
		 * @return The newly created instance of [[DP]].
		 */
		def build(owner: DoerProvidersManager): DoerProvider[MD]
		
		override def canEqual(that: Any): Boolean = that.isInstanceOf[DoerProviderDescriptor[?]]

		override def equals(that: Any): Boolean = that match {
			case that: DoerProviderDescriptor[?] => this.id == that.id
			case _ => false
		}

		override val hashCode: Int = id.hashCode()

		override def toString: String = id
	}

	/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DoerProviderDescriptor]]s and corresponding [[DoerProvider]]s that the [[Matrix]] instance uses. */
	trait DoerProvidersManager {

		/** Gets the [[DoerProvider]] associated with the provided [[DoerProviderDescriptor]]. If none exists one is created. */
		def get[D <: MatrixDoer](descriptor: DoerProviderDescriptor[D]): DoerProvider[D]

		inline def provideDoer[D <: MatrixDoer](matrix: AbstractMatrix, descriptor: DoerProviderDescriptor[D]): D =
			get(descriptor).provide(matrix)
	}

	/** Specifies the aide that a [[Matrix]] instance requires. */
	trait Aide[Self <: Aide[Self]] {
		/** The type of the [[doerProvidersManager]] field. */
		type DPsManager <: DoerProvidersManager

		/** Type of the doers provided by the default [[DoerProvider]] */
		type DefaultDoer <: MatrixDoer
		
		/** The [[DoerProviderDescriptor]] of the default [[DoerProvider]] */
		val defaultDoerProviderDescriptor: DoerProviderDescriptor[DefaultDoer]

		/** A [[DoerProvidersManager]] required to manage the [[Doer]] instances. */
		val doerProvidersManager: DPsManager

		def buildLogger(owner: Matrix[Self]): Logger
	}
}

class Matrix[A <: Matrix.Aide[A]](name: String, val aide: A) extends AbstractMatrix(name) { thisMatrix =>
	override type DefaultDoer = aide.DefaultDoer

	val doerProvidersManager: aide.DPsManager = aide.doerProvidersManager

	override val doer: DefaultDoer =
		provideDefaultDoer

	override protected val spawner: Spawner[doer.type] =
		new Spawner(Maybe.empty, doer, 0)

	override val logger: Logger =
		aide.buildLogger(this)

	override def provideDoer[D <: MatrixDoer](descriptor: DoerProviderDescriptor[D]): D = 
		doerProvidersManager.provideDoer(thisMatrix, descriptor)

	override def provideDefaultDoer: DefaultDoer =
		doerProvidersManager.get(aide.defaultDoerProviderDescriptor).provide(thisMatrix)
		
}
