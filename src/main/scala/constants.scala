package readren.matrix

import readren.taskflow.Maybe

import scala.util.Success

object constants {
	val SomeUnit: Maybe[Unit] = Maybe.some(())
	val SomeTrue: Maybe[true] = Maybe.some(true)
	val SomeFalse: Maybe[false] = Maybe.some(false)
	val SuccessUnit: Success[Unit] = Success(())
	val SuccessTrue: Success[true] = Success(true)
	val SuccessFalse: Success[false] = Success(false)
	val SuccessEmpty: Success[Maybe[Nothing]] = Success(Maybe.empty)
	val SomeSuccessUnit: Maybe[Success[Unit]] = Maybe.some(SuccessUnit)
	val SomeSuccessFalse: Maybe[Success[false]] = Maybe.some(SuccessFalse)
	val SomeSuccessTrue: Maybe[Success[true]] = Maybe.some(SuccessTrue)
	val SuccessSomeFalse: Success[Maybe[false]] = Success(SomeFalse)
}