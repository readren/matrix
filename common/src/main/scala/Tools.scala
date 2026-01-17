package readren.common

import scala.reflect.ClassTag
import scala.util.Failure

extension [A](failure: Failure[A]) {
	inline def castTo[B]: Failure[B] = failure.asInstanceOf[Failure[B]]
}


inline def deriveToString[A](a: A): String = {
	${ ToolsMacro.deriveToStringImpl[A]('a) }
}


extension [A](array: IArray[A]) {

	inline def mapWithIndex[B: ClassTag](inline f: (element: A, index: Int) => B): IArray[B] = {
		val length = array.length
		val result = new Array[B](length)
		var index = length
		while index > 0 do {
			index -= 1
			result(index) = f(array(index), index)
		}
		IArray.unsafeFromArray(result)
	}

	inline def foreachWithIndex(inline consumer: (element: A, index: Int) => Any): Unit = {
		var index = array.length
		while index > 0 do {
			index -= 1
			consumer(array(index), index)
		}
	}

	inline def countWithIndex(inline predicate: (element: A, index: Int) => Boolean): Int = {
		var index = array.length
		var counter: Int = 0
		while index > 0 do {
			index -= 1
			if predicate(array(index), index) then counter += 1
		}
		counter
	}

	inline def foldLeftWithIndex[B](initial: B)(inline f: (carry: B, elem: A, index: Int) => B): B = {
		val length = array.length
		var index = 0
		var carry = initial
		while index < length do {
			carry = f(carry, array(index), index)
			index += 1
		}
		carry
	}

	inline def forallWithIndex(inline predicate: (elem: A, index: Int) => Boolean): Boolean = {
		val length = array.length
		var index = 0
		while index < length && predicate(array(index), index) do index += 1
		index == length
	}

	inline def existsWithIndex(inline predicate: (elem: A, index: Int) => Boolean): Boolean = {
		var index = array.length
		var exists = false
		while index > 0 && !exists do {
			index -= 1
			exists = predicate(array(index), index)
		}
		exists
	}

}


