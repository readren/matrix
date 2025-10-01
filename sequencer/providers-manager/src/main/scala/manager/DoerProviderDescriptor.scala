package readren.sequencer
package manager

import scala.compiletime.asMatchable

/**
 * Identifies and builds an instance of [[DoerProvider]].
 * Subtypes of this trait usually are singleton objects.
 * @param id the identifier of this instance used for equality.
 * @tparam D the subtype of [[Doer]] provided by the [[DoerProvider]] instance that this descriptor instance identifies.
 */
trait DoerProviderDescriptor[D <: Doer](val id: String) extends Equals { thisDescriptor =>

	type Tag

	type DoerType = D

	/** The type of [[DoerProvider]] returned by the [[build]] method. */
	type DP <: DoerProvider[D] {type Tag = thisDescriptor.Tag}
	
	/**
	 * Builds the single [[DoerProvider]] instance that this descriptor identifies.
	 * This method is called a single time per [[DoerProvidersManager]].
	 * Intended to be called by the [[DoerProviderManager]] only.
	 *
	 * @param owner The [[DoerProvidersManager]] that will manage the newly created [[DoerProvider]].
	 * @return The newly created [[DoerProvider]].
	 */
	def build(owner: DoerProvidersManager): DP

	override def canEqual(that: Any): Boolean = that.isInstanceOf[DoerProviderDescriptor[?]]

	override def equals(that: Any): Boolean = that.asMatchable match {
		case that: DoerProviderDescriptor[?] => this.id == that.id
		case _ => false
	}

	override val hashCode: Int = id.hashCode()

	override def toString: String = id
}