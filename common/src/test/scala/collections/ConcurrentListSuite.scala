package readren.common
package collections

import collections.ConcurrentList

import munit.ScalaCheckEffectSuite
import org.scalacheck.Gen
import org.scalacheck.effect.PropF

import java.util.concurrent.locks.LockSupport
import java.util.concurrent.{ConcurrentHashMap, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.*

class ConcurrentListSuite extends ScalaCheckEffectSuite {

	private val availableProcessors = Runtime.getRuntime.availableProcessors()
	given ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(availableProcessors))

	private val TOP = 100
	private val NODES_TO_ADD_BY_EACH_THREAD = 10000
	private val NODES_REMOVED_BY_EACH_THREAD = 9000
	private val PARALLELISM = availableProcessors / 3

	private case class TestNode(id: Int, addingThreadIndex: Int = -1) extends ConcurrentList.Node {
		type Self = TestNode

		override def toString: String = s"Node($id)"
	}

	test("Concurrent addition and traversal") {
		val list = new ConcurrentList[TestNode]

		val nodes = (1 to 100).map(TestNode(_)).toList

		val addFutures = Future.sequence(nodes.map(node => Future(list.add(node))))

		addFutures.map { _ =>
			val iterator = list.circularIterator
			val traversed = Iterator.continually(iterator.next())
				.take(nodes.size)
				.toSet

			assertEquals(traversed.size, nodes.size)
			assertEquals(traversed, nodes.toSet)
		}
	}

	test("Concurrent removal and traversal") {
		val list = new ConcurrentList[TestNode]

		val nodes = (1 to 100).map(TestNode(_)).toList
		nodes.foreach(list.add)

		val toRemove = nodes.take(50)

		val removeFutures = Future.sequence(toRemove.map(node => Future(node.remove())))

		removeFutures.map { _ =>
			val iterator = list.circularIterator
			val traversed = Iterator.continually(iterator.next())
				.take(50)
				.toSet

			assertEquals(traversed.size, 50)
			assertEquals(traversed, nodes.drop(50).toSet)
		}
	}


	test("Concurrent addition preserves all elements") {
		PropF.forAllF(Gen.listOf(Gen.posNum[Int])) { ids =>
			val list = new ConcurrentList[TestNode]
			val nodes = ids.distinct.map(TestNode(_))

			val addFutures = Future.sequence(nodes.map(node => Future(list.add(node))))

			addFutures.map { _ =>
				val iterator = list.circularIterator
				val traversed = Iterator.continually(iterator.next())
					.take(nodes.size)
					.toSet

				assertEquals(traversed.size, nodes.size)
				assertEquals(traversed.map(_.id), ids.distinct.toSet)
			}
		}
	}

	test("Concurrent addition, removal, and traversal ") {
		val testedList = new ConcurrentList[TestNode]
		val expectedNodesById = new ConcurrentHashMap[Int, TestNode]
		@volatile var stopped = false

		val addingFutures =
			for threadIndex <- 0 until PARALLELISM yield Future {
				for addedId <- threadIndex * NODES_TO_ADD_BY_EACH_THREAD until ((threadIndex + 1) * NODES_TO_ADD_BY_EACH_THREAD) do {
					val addedNode = TestNode(addedId, threadIndex)
					testedList.add(addedNode)
					expectedNodesById.put(addedId, addedNode)
					// println(s"Added $addedNode")
				}
			}
		val removingFutures =
			for threadIndex <- 0 until PARALLELISM yield Future {
				var remainingNodesToRemove = NODES_REMOVED_BY_EACH_THREAD
				while remainingNodesToRemove > 0 do {
					val iterator = expectedNodesById.entrySet().iterator()
					if iterator.hasNext then {
						val entryToRemove = iterator.next()
						entryToRemove.getValue.remove()
						iterator.remove()
						remainingNodesToRemove -= 1
					} else LockSupport.parkNanos(100_000)
				}
			}

		val traversingFutures =
			for threadIndex <- 0 until PARALLELISM yield Future {
				val circularIterator = testedList.circularIterator
				while !stopped do {
					var current = circularIterator.next()
					while current eq null do {
						LockSupport.parkNanos(100_000)
						current = circularIterator.next()
					}
					val greatestIdByThread = Array.fill(PARALLELISM)(Integer.MIN_VALUE)
					val leastIdByThread = Array.fill(PARALLELISM)(Integer.MAX_VALUE)
					var reachedAInitialNode = false
					while !reachedAInitialNode do {
						if current.id > greatestIdByThread(current.addingThreadIndex) then greatestIdByThread(current.addingThreadIndex) = current.id
						if current.id < leastIdByThread(current.addingThreadIndex) then leastIdByThread(current.addingThreadIndex) = current.id
						val next = circularIterator.next()
						if next.id >= greatestIdByThread(next.addingThreadIndex) then reachedAInitialNode = true
						else {
							assert(next.id < leastIdByThread(next.addingThreadIndex))
						}
						current = next
					}
				}

			}

		Future.sequence(addingFutures ++ removingFutures).onComplete {
			case Success(_) =>
				stopped = true
			case Failure(e) =>
				e.printStackTrace()
				stopped = true
				throw e
		}
		Future.sequence(traversingFutures).andThen {
			case Failure(e) =>
				e.printStackTrace()
				throw e
			case Success(_) =>
				var node = testedList.nextOf(null)
				var nodesCounter = 0
				while node ne null do {
					nodesCounter += 1
					assert(expectedNodesById.containsKey(node.id))
					node = testedList.nextOf(node)
				}
				assert(nodesCounter == expectedNodesById.size)
		}
	}
}

