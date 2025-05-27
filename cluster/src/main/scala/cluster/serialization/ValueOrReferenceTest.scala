package readren.matrix
package cluster.serialization

sealed trait ValueOrReferenceTest[-T] {
	def isValueType: Boolean
}

object ValueOrReferenceTest {

	private val isValue: ValueOrReferenceTest[AnyVal] = new ValueOrReferenceTest[AnyVal] {
		override def isValueType: Boolean = true
	}
	private val isRef: ValueOrReferenceTest[AnyRef] = new ValueOrReferenceTest[AnyRef] {
		override def isValueType: Boolean = false
	}

	inline given whenReference: [R <: AnyRef] => ValueOrReferenceTest[R] = isRef

	inline given whenValue: [V <: AnyVal] => ValueOrReferenceTest[V] = isValue

}