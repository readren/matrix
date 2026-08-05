package readren.sequencer

import java.util.concurrent.atomic.AtomicBoolean

object CpuSaturator {
	/**
	 * Starts an infinite loop in as many threads as processors.
	 * Returns a Runnable that when executed makes all the loops to exit.
	 */
	def startSaturation(): Runnable = {
		val running = new AtomicBoolean(true)
		val numProcessors = Runtime.getRuntime.availableProcessors()
		val threads = new Array[Thread](numProcessors)

		var i = 0
		while i < numProcessors do {
			val thread = new Thread(new Runnable {
				override def run(): Unit = {
					while running.get() do {
						// Busy loop to consume 100% CPU
					}
				}
			}, s"cpu-contention-thread-$i")
			thread.setDaemon(true)
			threads(i) = thread
			thread.start()
			i += 1
		}

		new Runnable {
			override def run(): Unit = {
				running.set(false)
				var j = 0
				while j < numProcessors do {
					threads(j).join()
					j += 1
				}
			}
		}
	}
}
