package readren.common

import munit.ScalaCheckEffectSuite

class EmptiableTest extends ScalaCheckEffectSuite {
	test("A Emptiable, when not empty, should behave as non-empty") {
		val one: 1 = 1
		val emptiableOne: Emptiable[1] = Emptiable(one)
		assert(emptiableOne.isDefined)

		val int: Int = 2
		val emptiableInt = Emptiable(int)
		assert(emptiableInt.isDefined)

		val string = "string"
		val emptiableString = Emptiable(string)
		assert(emptiableString.isDefined)

		val pf: PartialFunction[Int, Int | Null] = {
			case x if x > 0 => x
		}
		val liftedPf = Emptiable.liftPartialFunction(pf)
		assert(liftedPf(1).isDefined)

		val emptiableEmptiableOne = Emptiable(emptiableOne)
		assert(emptiableEmptiableOne.isDefined && emptiableEmptiableOne.get.get == 1)

		val emptiableEmpty = Emptiable(Emptiable.empty)
		assert(emptiableEmpty.isDefined && emptiableEmpty.get.isEmpty)
	}

	test("A Emptiable, when empty, should behave as empty") {
		val emptyInt: Emptiable[Int] = Emptiable.apply[Int](null)
		assert(emptyInt.isEmpty)
		val emptyString = Emptiable.apply[String](null)
		assert(emptyString.isEmpty)
		val pf: PartialFunction[Int, Int | Null] = {
			case x if x > 0 => x
		}
		val liftedPf = Emptiable.liftPartialFunction(pf)
		assert(liftedPf(0).isEmpty)
	}

	test("Emptiable.some(x).get should return x") {
		assert(Emptiable(7).get == 7)
		assert(Emptiable("seven").get == "seven")
	}
}
