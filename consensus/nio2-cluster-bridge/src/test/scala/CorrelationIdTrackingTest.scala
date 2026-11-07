package readren.consensus
package nio2

import munit.ScalaCheckSuite

class CorrelationIdTrackingTest extends ScalaCheckSuite {

	test("CorrelationId wraps monotonically across Short boundary") {
		var id = CorrelationId(32766.toShort)
		assertEquals(id.toShort, 32766.toShort)

		id = id.next
		assertEquals(id.toShort, 32767.toShort) // Short.MaxValue

		id = id.next
		assertEquals(id.toShort, (-32768).toShort) // Short.MinValue

		id = id.next
		assertEquals(id.toShort, (-32767).toShort)
	}

	test("CorrelationId.isLapsedRelativeTo detects window overflow (> 32,767 requests ahead)") {
		val initial = CorrelationId(0.toShort)

		// 1 to 32,767 requests ahead: NOT lapsed (current is within capacity window)
		assertEquals(initial.isLapsedRelativeTo(CorrelationId(1.toShort)), false)
		assertEquals(initial.isLapsedRelativeTo(CorrelationId(100.toShort)), false)
		assertEquals(initial.isLapsedRelativeTo(CorrelationId(32767.toShort)), false)

		// Identical ID: difference is 0 -> false
		assertEquals(initial.isLapsedRelativeTo(CorrelationId(0.toShort)), false)

		// > 32,768 steps ahead (e.g. 32,769 steps ahead = -32767): two's complement difference wraps to positive -> true
		assertEquals(initial.isLapsedRelativeTo(CorrelationId((-32767).toShort)), true)
		assertEquals(initial.isLapsedRelativeTo(CorrelationId((-32766).toShort)), true)
	}

	test("isStaleRelativeTo alias matches isLapsedRelativeTo") {
		val a = CorrelationId(10.toShort)
		val b = CorrelationId(20.toShort)
		val c = CorrelationId((-32760).toShort)

		assertEquals(a.isStaleRelativeTo(b), a.isLapsedRelativeTo(b))
		assertEquals(a.isStaleRelativeTo(c), a.isLapsedRelativeTo(c))
	}
}
