package readren.common

import scala.reflect.ClassTag

object IArrayWithIndexOps {
	inline def tabulate[A: ClassTag](length: Int)(inline f: Int => A): IArray[A] = {
		val array = new Array[A](length)
		var index = 0
		while index < length do {
			array(index) = f(index)
			index += 1
		}
		IArray.unsafeFromArray(array)
	}

	inline def copyFrom[A: ClassTag](origin: Array[A]): IArray[A] = {
		val array = new Array[A](origin.length)
		System.arraycopy(origin, 0, array, 0, array.length)
		IArray.unsafeFromArray(array)
	}
}

// 	TODO rename file to IArrayOps.scala
extension [A](array: IArray[A]) {

	inline def mapWithIndex[B: ClassTag](inline f: (element: A, index: Int) => B): IArray[B] = {
		val length = array.length
		val result = new Array[B](length)
		var index = 0
		while index < length do {
			result(index) = f(array(index), index)
			index += 1
		}
		IArray.unsafeFromArray(result)
	}

	inline def foreachWithIndex(inline consumer: (element: A, index: Int) => Any): Unit = {
		val length = array.length
		var index = 0
		while index < length do {
			consumer(array(index), index)
			index += 1
		}
	}

	inline def countWithIndex(inline predicate: (element: A, index: Int) => Boolean): Int = {
		val length = array.length
		var index = 0
		var counter = 0
		while index < length do {
			if predicate(array(index), index) then counter += 1
			index += 1
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
		val length = array.length
		var index = 0
		var exists = false
		while index < length && !exists do {
			exists = predicate(array(index), index)
			index += 1
		}
		exists
	}

	inline def collectWithIndex[B: ClassTag](inline pf: (element: A, index: Int) => Maybe[B]): IArray[B] = {
		val length = array.length
		val intermediateArray = new Array[B](length)
		var resultSize = 0
		var index = 0
		while index < length do {
			pf(array(index), index).foreach { b =>
				intermediateArray(resultSize) = b
				resultSize += 1
			}
			index += 1
		}
		IArray.unsafeFromArray(Array.copyOf(intermediateArray, resultSize))
	}

	inline def ++(other: IArray[A])(using ClassTag[A]): IArray[A] = {
		val thisLength = array.length
		if thisLength == 0 then other
		else if other.length == 0 then array
		else IArray.tabulate(thisLength + other.length) { i =>
			if i < thisLength then array(i) else other(i - thisLength)
		}
	}

}
