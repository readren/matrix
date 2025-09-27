package readren.matrix
package pruebas

import java.lang.management.{ManagementFactory, MemoryType}

object ObjectCounterAgent {
	def getApproximateObjectCount: Long = {

		val pools = ManagementFactory.getMemoryPoolMXBeans
		pools.stream()
			.filter(_.getType == MemoryType.HEAP)
			.mapToLong(pool => pool.getUsage.getUsed)
			.sum()
	}

}