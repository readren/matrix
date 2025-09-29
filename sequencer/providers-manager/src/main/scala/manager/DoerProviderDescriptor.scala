package readren.sequencer
package manager

import scala.compiletime.asMatchable

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