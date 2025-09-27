package readren.matrix
package pruebas

import java.util.concurrent.{Executors, TimeUnit}

class Scheduler {
	private val executor = Executors.newSingleThreadScheduledExecutor { r =>
		val thread = new Thread(r)
		thread.setDaemon(true)
		thread
	}

	def schedule(delay: Long, unit: TimeUnit)(runnable: Runnable): Unit = {
		executor.schedule(runnable, delay, unit)
	}	
	
	def fixedRate(initialDelay: Long, period: Long, unit: TimeUnit)(runnable: Runnable): Unit = {
		executor.scheduleAtFixedRate(runnable, period, period, unit)
	}
	
	def shutdown(): Unit = executor.shutdown()
}
