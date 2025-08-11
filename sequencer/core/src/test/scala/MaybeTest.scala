package readren.sequencer

import org.scalatest.funspec.AnyFunSpec

class MaybeTest extends AnyFunSpec {
	describe("A Maybe") {
		describe("when not empty") {
			val oneMaybe: Maybe[1] = Maybe.some(1)
			it("should behave as non-empty") {
				assert(oneMaybe.isDefined)
			}
		}
		describe("when empty") {
			val intMaybe: Maybe[Int] = Maybe.empty
			it("should behave as empty") {
				assert(intMaybe.isEmpty)
				assert(Maybe[String](null).isEmpty)
			}
		}
		it("Maybe.some(x).get should return x") {
			assert(Maybe.some(7).get == 7)
			assert(Maybe.some("seven").get == "seven")
		}
	}

}
