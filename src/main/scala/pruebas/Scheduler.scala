package readren.matrix
package pruebas

import java.util.concurrent.{Executors, TimeUnit}

class Scheduler {
	private val executor = Executors.newSingleThreadScheduledExecutor()
	
	def schedule(period: Long, unit: TimeUnit)(runnable: Runnable): Unit = {
		executor.scheduleAtFixedRate(runnable, period, period, unit)
	}
	
	def shutdown(): Unit = executor.shutdown()
}
