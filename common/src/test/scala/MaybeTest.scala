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
		assert(maybeNull.isEmpty)

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

	test("Maybe.some wrapping null dynamically should throw IllegalArgumentException") {
		val nullStr: String = null.asInstanceOf[String]
		intercept[IllegalArgumentException] {
			Maybe.some(nullStr)
		}
	}

	test("Maybe.value should return underlying AnyRef or null") {
		val str = "test"
		val defined = Maybe.some(str)
		val empty = Maybe.empty
		assertEquals(defined.value, str)
		assert(empty.value eq null)
	}

	test("foreach should execute function only when defined") {
		var executed = false
		Maybe.some("hello").foreach { _ => executed = true }
		assert(executed)

		var emptyExecuted = false
		Maybe.empty.foreach { _ => emptyExecuted = true }
		assert(!emptyExecuted)
	}

	test("map should transform defined value and preserve empty state") {
		val defined = Maybe.some(5).map(_ * 2)
		assert(defined.isDefined)
		assertEquals(defined.get, 10)

		val empty = (Maybe.empty: Maybe[Int]).map(_ * 2)
		assert(empty.isEmpty)
	}

	test("flatMap should chain Maybe computations") {
		val definedToDefined = Maybe.some(5).flatMap(x => Maybe.some(x.toString))
		assert(definedToDefined.isDefined)
		assertEquals(definedToDefined.get, "5")

		val definedToEmpty = Maybe.some(5).flatMap(_ => Maybe.empty)
		assert(definedToEmpty.isEmpty)

		val emptyToDefined = (Maybe.empty: Maybe[Int]).flatMap(x => Maybe.some(x.toString))
		assert(emptyToDefined.isEmpty)
	}

	test("fold should return function result when defined and default when empty") {
		val definedRes = Maybe.some(3).fold("empty")(_.toString)
		assertEquals(definedRes, "3")

		val emptyRes = (Maybe.empty: Maybe[Int]).fold("empty")(_.toString)
		assertEquals(emptyRes, "empty")
	}

	test("orElse should return self when defined and alternative when empty") {
		val defined = Maybe.some("first")
		val alt = Maybe.some("alt")
		assertEquals(defined.orElse(alt).get, "first")

		val empty = Maybe.empty
		assertEquals(empty.orElse(alt).get, "alt")
	}

	test("getOrElse should return contained value when defined and default when empty") {
		assertEquals(Maybe.some(42).getOrElse(0), 42)
		assertEquals(Maybe.empty.getOrElse(0), 0)
	}

	test("exists should check predicate against contained value") {
		assert(Maybe.some(10).exists(_ > 5))
		assert(!Maybe.some(3).exists(_ > 5))
		assert(!(Maybe.empty: Maybe[Int]).exists(_ > 5))
	}

	test("is should check reference equality") {
		val obj = new AnyRef
		val maybeObj = Maybe.some(obj)
		assert(maybeObj.is(obj))
		assert(!maybeObj.is(new AnyRef))
		assert(!Maybe.empty.is(obj))
	}

	test("isEqualTo and == should handle equality correctly") {
		val a1 = Maybe.some("abc")
		val a2 = Maybe.some("abc")
		val b = Maybe.some("xyz")
		val empty1 = Maybe.empty
		val empty2 = Maybe.empty

		assert(a1.isEqualTo(a2))
		assert(!a1.isEqualTo(b))
		assert(!a1.isEqualTo(empty1))
		assert(empty1.isEqualTo(empty2))

		assert(a1 == a2)
		assert(!(a1 == b))
		assert(!(a1 == empty1))
		assert(empty1.isEqualTo(empty2))
	}

	test("contains and containsNonStrict should check contained value equality") {
		val defined = Maybe.some("hello")
		assert(defined.contains("hello"))
		assert(!defined.contains("world"))
		assert(!Maybe.empty.contains("hello"))

		assert(defined.containsNonStrict("hello"))
		assert(!defined.containsNonStrict("world"))
		assert(!Maybe.empty.containsNonStrict("hello"))
	}

	test("liftPartialFunction should return Maybe.empty when undefined") {
		val pf: PartialFunction[Int, String] = {
			case x if x > 0 => "positive"
		}
		val lifted = Maybe.liftPartialFunction(pf)
		assert(lifted(1).isDefined)
		assertEquals(lifted(1).get, "positive")
		assert(lifted(-1).isEmpty)
	}
}
