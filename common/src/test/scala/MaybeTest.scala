package readren.common

import munit.ScalaCheckEffectSuite

class MaybeTest extends ScalaCheckEffectSuite {
	test("A Maybe, when not empty, should behave as non-empty") {
		val one: 1 = 1
		val oneMaybe: Maybe[1] = Maybe(one)
		assert(oneMaybe.isDefined)

		val int: Int = 2
		assert(Maybe.apply(int).isDefined)
		assert(Maybe.some(int).isDefined)

		val string = "string"
		assert(Maybe.apply(string).isDefined)
		assert(Maybe.some(string).isDefined)

		val pf: PartialFunction[Int, String] = {
			case x if x > 0 => "isPositive"
		}

		val liftedPf = Maybe.liftPartialFunction(pf)
		assert(liftedPf(1).isDefined)

		val nullableString: String | Null = string
		assert(Maybe.apply(nullableString).isDefined)
	}

	test("A Maybe, when empty, should behave as empty") {
		val maybeEmpty: Maybe[Int] = Maybe.empty
		assert(maybeEmpty.isEmpty)

		val maybeNull = Maybe.apply[String](null)

		val maybeFrom = Maybe.apply[Int](null)
		assert(maybeFrom.isEmpty)
		val fromNullMaybe = Maybe.apply[String](null)
		assert(fromNullMaybe.isEmpty)

		val pepe = Maybe[String](null)
		assert(pepe.isEmpty)
	}

	test("Maybe.some(x).get should return x") {
		assert(Maybe.some(7).get == 7)
		assert(Maybe.some("seven").get == "seven")
	}
}
