package readren.common

import munit.ScalaCheckEffectSuite

class MaybeTest extends ScalaCheckEffectSuite {
	test("A Maybe, when not empty, should behave as non-empty") {
		val oneMaybe: Maybe[1] = Maybe(1)
		assert(oneMaybe.isDefined)
	}
	test("A Maybe, when empty, should behave as empty") {
		val intMaybe: Maybe[Int] = Maybe.empty
		assert(intMaybe.isEmpty)
		assert(Maybe[String](null).isEmpty)
	}
	test("Maybe.some(x).get should return x") {
		assert(Maybe(7).get == 7)
		assert(Maybe("seven").get == "seven")
	}
}
