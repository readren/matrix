package readren.matrix
package core

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.atomic.AtomicLong


object Matrix {

	trait DoerAssistantProvider {

		type ProvidedAssistant <: Doer.Assistant

		/**
		 * Provides an [[Doer.Assistant]] intended to be used as a parameter for the constructor of a [[MatrixDoer]].
		 *
		 * @param serial the unique identifier of the [[MatrixDoer]] for which the returned [[Doer.Assistant]] is intended.
		 *               This identifier is guaranteed to be different for each call to this method. The implementation of
		 *               this method may use the `serial` parameter for purposes such as debugging or tracking,
		 *               but this is optional and not required for the functionality of the returned [[ProvidedAssistant]].
		 *               Neither the [[MatrixDoer]] nor other methods of this trait rely on any information the
		 *               implementation constructs based on the `serial` parameter.
		 *
		 * Note: The implementation of this method may return the same [[ProvidedAssistant]] instance for distinct calls,
		 * regardless of the provided `serial`.
		 */
		def provide(serial: MatrixDoer.Id): ProvidedAssistant
	}

	/**
	 * Identifies, and builds instances of, the specified concrete subtype of [[DoerAssistantProvider]].
	 * Instances of this class usually are singleton objects.
	 * @param id the identifier of this instance used for equality.
	 * @tparam Dap the concrete subtype of the [[DoerAssistantProvider]] that this instance identifies and builds instances of.
	 */
	trait DapKind[+Dap <: DoerAssistantProvider](val id: String) extends Equals {

		/**
		 * Builds a [[DoerAssistantProvider]] of the type this instance identifies.
		 * This method is called a single time per [[DapManager]].
		 *
		 * @param owner The [[DapManager]] that will manage the newly created [[DoerAssistantProvider]].
		 * @return The newly created instance of [[Dap]].
		 */
		def build(owner: DapManager): Dap

		override def canEqual(that: Any): Boolean = that.isInstanceOf[DapKind[?]]

		override def equals(that: Any): Boolean = that match {
			case that: DapKind[?] => this.id == that.id
			case _ => false
		}

		override val hashCode: Int = id.hashCode()

		override def toString: String = id
	}

	/** Specifies how a [[Matrix]] instance interacts with the object responsible for managing the [[DapKind]]s and corresponding [[DoerAssistantProvider]]s that the [[Matrix]] instance uses. */
	trait DapManager {

		/** Gets the [[DoerAssistantProvider]] associated with the provided [[DapKind]]. If none exists one is created. */
		def get[Dap <: DoerAssistantProvider](dapKind: DapKind[Dap]): Dap
	}

	/** Specifies the aide that a [[Matrix]] requires. */
	trait Aide[Self <: Aide[Self]] {
		/** The type of the [[DapManger]] that this aide builds. */
		type DapMan <: DapManager

		/** Type of the default [[DoerAssistantProvider]], which is the one used to provide the [[Doer.Assistant]] instance for the [[Matrix.doer]] member. */
		type DefaultDap <: DoerAssistantProvider

		/** The [[DapKind]] of the default [[DoerAssistantProvider]] */
		val defaultDapKind: DapKind[DefaultDap]

		/** @param owner the [[Matrix]] instance that will own the brand new [[DapManager]] returned. */
		def buildDoerAssistantProviderManager(owner: Matrix[Self]): DapMan

		def buildLogger(owner: Matrix[Self]): Logger
	}
}

class Matrix[A <: Matrix.Aide[A]](name: String, val aide: A) extends AbstractMatrix(name) { thisMatrix =>

	import Matrix.*

	private val matrixDoerIdSequencer = new AtomicLong(0)

	val dapManager: aide.DapMan = aide.buildDoerAssistantProviderManager(thisMatrix)
	override val defaultDapKind: aide.defaultDapKind.type = aide.defaultDapKind

	override def provideDoer[Dap <: DoerAssistantProvider](dapKind: DapKind[Dap]): MatrixDoer = {
		val serial = matrixDoerIdSequencer.getAndIncrement()
		new MatrixDoer(serial, dapManager.get(dapKind).provide(serial), thisMatrix)
	}

	override val doer: MatrixDoer = provideDoer(defaultDapKind)

	override protected val spawner: Spawner[doer.type] = new Spawner(Maybe.empty, doer, 0)

	override val logger: Logger = aide.buildLogger(this)

}
