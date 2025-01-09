package readren.matrix
package core

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.atomic.AtomicLong


object Matrix {

	trait DoerAssistantProvider {
		/**
		 * Provides an [[Doer.Assistant]] intended to be used as a parameter for the constructor of a [[MatrixDoer]].
		 *
		 * @param serial the unique identifier of the [[MatrixDoer]] for which the returned [[Doer.Assistant]] is intended.
		 *               This identifier is guaranteed to be different for each call to this method. The implementation of
		 *               this method may use the `serial` parameter for purposes such as debugging or tracking,
		 *               but this is optional and not required for the functionality of the returned [[Assistant]].
		 *               Neither the [[MatrixDoer]] nor other methods of this trait rely on any information the
		 *               implementation constructs based on the `serial` parameter.
		 *
		 * Note: The implementation of this method may return the same [[Assistant]] instance for distinct calls,
		 * regardless of the provided `serial`.
		 */
		def provide(serial: MatrixDoer.Id): Doer.Assistant
	}

	/**
	 * @tparam Dap the type of the [[DoerAssistantProvider]] that this reference refers to.
	 */
	trait DoerAssistantProviderRef[+Dap <: DoerAssistantProvider] extends Equals {

		override def canEqual(that: Any): Boolean = that.isInstanceOf[Matrix.DoerAssistantProvider]

		/**
		 * Creates the [[DoerAssistantProvider]] instance that this reference refers to.
		 *
		 * @param owner The [[DoerAssistantProviderManager]] that will manage the newly created [[DoerAssistantProvider]].
		 * @return The newly created instance of [[Dap]].
		 */
		def build(owner: DoerAssistantProviderManager): Dap
	}

	trait DoerAssistantProviderManager {

		/** Gets the [[DoerAssistantProvider]] associated with the provided [[DoerAssistantProviderRef]]. If none exists one is created. */
		def get[A <: DoerAssistantProvider](ref: DoerAssistantProviderRef[A]): A
	}

	trait Aide[Self <: Aide[Self]] {
		type DapMan <: DoerAssistantProviderManager

		/** Type of the default [[DoerAssistantProvider]]. */
		type DefaultDap <: DoerAssistantProvider

		/** The [[DoerAssistantProviderRef]] corresponding to the default [[DoerAssistantProvider]] */
		val defaultDapRef: DoerAssistantProviderRef[DefaultDap]

		/** @param owner the [[Matrix]] instance that will own the brand new [[DoerAssistantProviderManager]] returned. */
		def buildDoerAssistantProviderManager(owner: Matrix[Self]): DapMan

		def buildLogger(owner: Matrix[Self]): Logger
	}
}

class Matrix[A <: Matrix.Aide[A]](name: String, val aide: A) extends AbstractMatrix(name) { thisMatrix =>

	import Matrix.*

	private val matrixDoerIdSequencer = new AtomicLong(0)

	val doerAssistantProviderManager: aide.DapMan = aide.buildDoerAssistantProviderManager(thisMatrix)
	override val defaultDoerAssistantProviderRef: DoerAssistantProviderRef[aide.DefaultDap] = aide.defaultDapRef

	override def provideDoer[Dap<: Matrix.DoerAssistantProvider](ref: DoerAssistantProviderRef[Dap]): MatrixDoer = {
		val provider = doerAssistantProviderManager.get(ref)
		val serial = matrixDoerIdSequencer.getAndIncrement()
		new MatrixDoer(serial, provider.provide(serial), thisMatrix)
	}

	override val doer: MatrixDoer = provideDoer(defaultDoerAssistantProviderRef)

	override protected val spawner: Spawner[doer.type] = new Spawner(Maybe.empty, doer, 0)

	override val logger: Logger = aide.buildLogger(this)

}
