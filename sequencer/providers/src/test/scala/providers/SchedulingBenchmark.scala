package readren.sequencer
package providers

import readren.sequencer.*
import providers.*

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

object SchedulingBenchmark {

	def main(args: Array[String]): Unit = {
		println("======================================================================")
		println("STARTING SCHEDULING DOERPROVIDER BENCHMARK (CPU-Saturating Workloads)")
		println("======================================================================")

		val poolSize = 4

		// Instantiate the 5 providers
		val flatProvider = new CooperativeFlatPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)
		val hierarchicalProvider = new CooperativeHierarchicalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)
		val threadDrivenProvider = new CooperativeThreadDrivenSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)
		val shardedProvider = new CooperativeShardedPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)
		val localProvider = new CooperativeLocalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize)

		// Warm-up to trigger JIT compilation
		println("\nPerforming JVM warm-up...")
		warmUp(flatProvider, poolSize)
		warmUp(hierarchicalProvider, poolSize)
		warmUp(threadDrivenProvider, poolSize)
		warmUp(shardedProvider, poolSize)
		warmUp(localProvider, poolSize)
		println("Warm-up complete.")

		// Run Scenario 1
		println("Running Scenario 1...")
		val s1Flat = runScenario1(flatProvider, poolSize)
		val s1Hier = runScenario1(hierarchicalProvider, poolSize)
		val s1Thread = runScenario1(threadDrivenProvider, poolSize)
		val s1Shard = runScenario1(shardedProvider, poolSize)
		val s1Local = runScenario1(localProvider, poolSize)

		// Run Scenario 2
		println("Running Scenario 2...")
		val s2Flat = runScenario2(flatProvider, poolSize)
		val s2Hier = runScenario2(hierarchicalProvider, poolSize)
		val s2Thread = runScenario2(threadDrivenProvider, poolSize)
		val s2Shard = runScenario2(shardedProvider, poolSize)
		val s2Local = runScenario2(localProvider, poolSize)

		// Run Scenario 3
		println("Running Scenario 3...")
		val s3Flat = runScenario3(flatProvider, poolSize)
		val s3Hier = runScenario3(hierarchicalProvider, poolSize)
		val s3Thread = runScenario3(threadDrivenProvider, poolSize)
		val s3Shard = runScenario3(shardedProvider, poolSize)
		val s3Local = runScenario3(localProvider, poolSize)

		// Run Scenario 4
		println("Running Scenario 4...")
		val s4Flat = runScenario4(flatProvider, poolSize)
		val s4Hier = runScenario4(hierarchicalProvider, poolSize)
		val s4Thread = runScenario4(threadDrivenProvider, poolSize)
		val s4Shard = runScenario4(shardedProvider, poolSize)
		val s4Local = runScenario4(localProvider, poolSize)

		// Run Scenario 5 (High Timer Density / External Threads)
		println("Running Scenario 5...")
		val s5Flat = runScenario5(flatProvider, poolSize)
		val s5Hier = runScenario5(hierarchicalProvider, poolSize)
		val s5Thread = runScenario5(threadDrivenProvider, poolSize)
		val s5Shard = runScenario5(shardedProvider, poolSize)
		val s5Local = runScenario5(localProvider, poolSize)

		// Run Scenario 6 (Flat Polling Winner)
		println("Running Scenario 6...")
		val s6Flat = runScenario6(flatProvider, poolSize)
		val s6Hier = runScenario6(hierarchicalProvider, poolSize)
		val s6Thread = runScenario6(threadDrivenProvider, poolSize)
		val s6Shard = runScenario6(shardedProvider, poolSize)
		val s6Local = runScenario6(localProvider, poolSize)

		// Run Scenario 7 (Sharded Polling Winner)
		println("Running Scenario 7...")
		val s7Flat = runScenario7(flatProvider, poolSize)
		val s7Hier = runScenario7(hierarchicalProvider, poolSize)
		val s7Thread = runScenario7(threadDrivenProvider, poolSize)
		val s7Shard = runScenario7(shardedProvider, poolSize)
		val s7Local = runScenario7(localProvider, poolSize)

		// Run Scenario 8 (Thread-Driven Winner / Worker Isolation)
		println("Running Scenario 8...")
		val s8Flat = runScenario8(flatProvider, poolSize)
		val s8Hier = runScenario8(hierarchicalProvider, poolSize)
		val s8Thread = runScenario8(threadDrivenProvider, poolSize)
		val s8Shard = runScenario8(shardedProvider, poolSize)
		val s8Local = runScenario8(localProvider, poolSize)

		// Shutdown all providers
		flatProvider.shutdown()
		hierarchicalProvider.shutdown()
		threadDrivenProvider.shutdown()
		shardedProvider.shutdown()
		localProvider.shutdown()

		// Print Results
		val separator = "-" * 241
		println("\n\n" + separator)
		println(" " * 105 + "BENCHMARK RESULTS SUMMARY")
		println(separator)
		printRow("Scenario", "Scenario 1", "Scenario 2", "Scenario 3", "Scenario 4", "Scenario 5", "Scenario 6", "Scenario 7", "Scenario 8")
		printRow("Description", "High-Freq Loop", "Lock Contention", "Expiry Burst", "Local Winner", "Hier Winner", "Flat Winner", "Shard Winner", "Thread Winner")
		printRow("Total Doers", "2,000", "2,000", "2,000", "2,000", "2,000", "5,000", "2,000", "2,000")
		printRow("Active Schedules", "N/A", "1 / doer", "100,000", "N/A", "50 / doer", "Sparse (<50)", "1 / doer", "N/A")
		printRow("Client Threads", "None (Workers)", "16", "8", "16 (via Workers)", "16", "16", "16", "16")
		printRow("Scheduling Origin", "Internal", "External", "External", "Internal", "External", "External", "External", "External")
		printRow("Cancellation Type", "None", "cancel", "None", "cancel", "cancel", "cancelAll", "cancel", "cancel")
		println(separator)
		printRow("DoerProvider", "", "", "", "", "", "", "", "")
		println(separator)
		printRow("Flat Polling", "%,.0f ops/s".format(s1Flat), "%,.0f ops/s".format(s2Flat), "%,.1f ms".format(s3Flat), "%,.0f ops/s".format(s4Flat), "%,.0f ops/s".format(s5Flat), "%,.0f ops/s".format(s6Flat), "%,.0f ops/s".format(s7Flat), "%,.0f ops/s".format(s8Flat))
		printRow("Hierarchical Polling", "%,.0f ops/s".format(s1Hier), "%,.0f ops/s".format(s2Hier), "%,.1f ms".format(s3Hier), "%,.0f ops/s".format(s4Hier), "%,.0f ops/s".format(s5Hier), "%,.0f ops/s".format(s6Hier), "%,.0f ops/s".format(s7Hier), "%,.0f ops/s".format(s8Hier))
		printRow("Thread-Driven", "%,.0f ops/s".format(s1Thread), "%,.0f ops/s".format(s2Thread), "%,.1f ms".format(s3Thread), "%,.0f ops/s".format(s4Thread), "%,.0f ops/s".format(s5Thread), "%,.0f ops/s".format(s6Thread), "%,.0f ops/s".format(s7Thread), "%,.0f ops/s".format(s8Thread))
		printRow("Sharded Polling", "%,.0f ops/s".format(s1Shard), "%,.0f ops/s".format(s2Shard), "%,.1f ms".format(s3Shard), "%,.0f ops/s".format(s4Shard), "%,.0f ops/s".format(s5Shard), "%,.0f ops/s".format(s6Shard), "%,.0f ops/s".format(s7Shard), "%,.0f ops/s".format(s8Shard))
		printRow("Local Polling", "%,.0f ops/s".format(s1Local), "%,.0f ops/s".format(s2Local), "%,.1f ms".format(s3Local), "%,.0f ops/s".format(s4Local), "%,.0f ops/s".format(s5Local), "%,.0f ops/s".format(s6Local), "%,.0f ops/s".format(s7Local), "%,.0f ops/s".format(s8Local))
		println(separator)
	}

	private def printRow(c1: String, c2: String, c3: String, c4: String, c5: String, c6: String, c7: String, c8: String, c9: String): Unit = {
		val p1 = if c1.length >= 22 then c1 else c1 + " " * (22 - c1.length)
		val p2 = if c2.length >= 24 then c2 else " " * (24 - c2.length) + c2
		val p3 = if c3.length >= 24 then c3 else " " * (24 - c3.length) + c3
		val p4 = if c4.length >= 24 then c4 else " " * (24 - c4.length) + c4
		val p5 = if c5.length >= 24 then c5 else " " * (24 - c5.length) + c5
		val p6 = if c6.length >= 24 then c6 else " " * (24 - c6.length) + c6
		val p7 = if c7.length >= 24 then c7 else " " * (24 - c7.length) + c7
		val p8 = if c8.length >= 24 then c8 else " " * (24 - c8.length) + c8
		val p9 = if c9.length >= 24 then c9 else " " * (24 - c9.length) + c9
		println(s"| $p1 | $p2 | $p3 | $p4 | $p5 | $p6 | $p7 | $p8 | $p9 |")
	}

	private def warmUp(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Unit = {
		val doers = Array.tabulate(poolSize)(i => dp.provide(s"warmup-doer-$i"))
		val latch = new CountDownLatch(1000)
		for (i <- 0 until 1000) {
			val doer = doers(i % poolSize)
			doer.executeSequentially(new Runnable {
				override def run(): Unit = {
					val sched: SchedulingExtension = doer
					val schedule = sched.newDelaySchedule(-1)
					sched.scheduleSequentially(schedule, _ => latch.countDown())
				}
			})
		}
		latch.await(2, TimeUnit.SECONDS)
	}

	private def runScenario1(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s1-doer-$i"))
		val taskCount = new AtomicLong(0)
		val startLatch = new CountDownLatch(1)
		val stopLatch = new CountDownLatch(doerCount)
		@volatile var running = true

		for (doer <- doers) {
			doer.executeSequentially(new Runnable {
				override def run(): Unit = {
					def scheduleNext(): Unit = {
						if running then {
							val sched: SchedulingExtension = doer
							val schedule = sched.newDelaySchedule(-1)
							sched.scheduleSequentially(schedule, _ => {
								taskCount.incrementAndGet()
								scheduleNext()
							})
						} else {
							stopLatch.countDown()
						}
					}

					startLatch.await()
					scheduleNext()
				}
			})
		}

		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		stopLatch.await(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		taskCount.get().toDouble / durationSeconds
	}

	private def runScenario2(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s2-doer-$i"))
		val opCount = new AtomicLong(0)
		val clientPool = Executors.newFixedThreadPool(16)
		val startLatch = new CountDownLatch(1)
		@volatile var running = true

		for (t <- 0 until 16) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					var i = 0
					while running do {
						val doer = doers(i % doerCount)
						val sched: SchedulingExtension = doer
						val schedule = sched.newDelaySchedule(100)
						sched.scheduleSequentially(schedule, _ => ())
						sched.cancel(schedule)
						opCount.incrementAndGet()
						i += 1
					}
				}
			})
		}

		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		clientPool.shutdown()
		clientPool.awaitTermination(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		opCount.get().toDouble / durationSeconds
	}

	private def runScenario3(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val scheduleCount = 100000
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s3-doer-$i"))
		val latch = new CountDownLatch(scheduleCount)
		val targetTime = System.currentTimeMillis() + 4000

		val clientPool = Executors.newFixedThreadPool(8)
		val startLatch = new CountDownLatch(1)
		for (i <- 0 until scheduleCount) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					val doer = doers(i % doerCount)
					val sched: SchedulingExtension = doer
					val delay = targetTime - System.currentTimeMillis()
					val schedule = sched.newDelaySchedule(if delay > 0 then delay else 1)
					sched.scheduleSequentially(schedule, _ => latch.countDown())
				}
			})
		}

		val startEnqueueTime = System.currentTimeMillis()
		startLatch.countDown()
		clientPool.shutdown()
		clientPool.awaitTermination(10, TimeUnit.SECONDS)

		val targetTimeNano = System.nanoTime() + ((targetTime - System.currentTimeMillis()) * 1000000L)
		latch.await(15, TimeUnit.SECONDS)
		val endMeasurementTime = System.nanoTime()

		val durationMs = (endMeasurementTime - targetTimeNano).toDouble / 1e6
		durationMs
	}

	private def runScenario4(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s4-doer-$i"))
		val opCount = new AtomicLong(0)
		val startLatch = new CountDownLatch(1)
		val stopLatch = new CountDownLatch(doerCount)
		@volatile var running = true

		for (doer <- doers) {
			doer.executeSequentially(new Runnable {
				override def run(): Unit = {
					def loop(): Unit = {
						if running then {
							val sched: SchedulingExtension = doer
							val schedule = sched.newDelaySchedule(50)
							sched.scheduleSequentially(schedule, _ => ())
							sched.cancel(schedule)
							opCount.incrementAndGet()

							// Schedule next iteration via zero-delay timer to avoid trapping the worker
							val nextLoop = sched.newDelaySchedule(0)
							sched.scheduleSequentially(nextLoop, _ => loop())
						} else {
							stopLatch.countDown()
						}
					}

					startLatch.await()
					loop()
				}
			})
		}
		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		stopLatch.await(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		opCount.get().toDouble / durationSeconds
	}

	private def runScenario5(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val initialSchedulesPerDoer = 50
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s5-doer-$i"))

		// Pre-populate each doer with 50 active schedules in the far future
		for (doer <- doers) {
			val sched: SchedulingExtension = doer
			for (j <- 0 until initialSchedulesPerDoer) {
				val schedule = sched.newDelaySchedule(10000 + j * 10)
				sched.scheduleSequentially(schedule, _ => ())
			}
		}

		val opCount = new AtomicLong(0)
		val clientPool = Executors.newFixedThreadPool(16)
		val startLatch = new CountDownLatch(1)
		@volatile var running = true

		for (t <- 0 until 16) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					var i = 0
					while running do {
						val doer = doers((t * 100 + i) % doerCount)
						val sched: SchedulingExtension = doer
						val schedule = sched.newDelaySchedule(15000)
						sched.scheduleSequentially(schedule, _ => ())
						sched.cancel(schedule)
						opCount.incrementAndGet()
						i += 1
					}
				}
			})
		}
		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		clientPool.shutdown()
		clientPool.awaitTermination(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		// Clean up to prevent leaks
		for (doer <- doers) {
			val sched: SchedulingExtension = doer
			sched.cancelAll()
		}

		val durationSeconds = (endTime - startTime) / 1e9
		opCount.get().toDouble / durationSeconds
	}

	private def runScenario6(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 5000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s6-doer-$i"))
		val opCount = new AtomicLong(0)
		val clientPool = Executors.newFixedThreadPool(16)
		val startLatch = new CountDownLatch(1)
		@volatile var running = true

		for (t <- 0 until 16) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					var i = 0
					while running do {
						val doer = doers((t * 100 + i) % doerCount)
						val sched: SchedulingExtension = doer
						val schedule = sched.newDelaySchedule(100000)
						sched.scheduleSequentially(schedule, _ => ())
						sched.cancelAll()
						opCount.incrementAndGet()
						i += 1
					}
				}
			})
		}
		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		clientPool.shutdown()
		clientPool.awaitTermination(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		opCount.get().toDouble / durationSeconds
	}

	private def runScenario7(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s7-doer-$i"))
		val opCount = new AtomicLong(0)
		val clientPool = Executors.newFixedThreadPool(16)
		val startLatch = new CountDownLatch(1)
		@volatile var running = true

		for (t <- 0 until 16) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					var i = 0
					while running do {
						val doer = doers((t * 100 + i) % doerCount)
						val sched: SchedulingExtension = doer
						val schedule = sched.newDelaySchedule(100000)
						sched.scheduleSequentially(schedule, _ => ())
						sched.cancel(schedule)
						opCount.incrementAndGet()
						i += 1
					}
				}
			})
		}
		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(5000)
		running = false
		clientPool.shutdown()
		clientPool.awaitTermination(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		opCount.get().toDouble / durationSeconds
	}

	private def runScenario8(dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String}, poolSize: Int): Double = {
		val doerCount = 2000
		val doers = Array.tabulate(doerCount)(i => dp.provide(s"s8-doer-$i"))
		val clientPool = Executors.newFixedThreadPool(16)
		val startLatch = new CountDownLatch(1)
		@volatile var running = true
		val normalTaskCount = new AtomicLong(0)

		// Start background normal tasks on the worker pool
		for (doer <- doers) {
			doer.executeSequentially(new Runnable {
				override def run(): Unit = {
					def loop(): Unit = {
						if running then {
							normalTaskCount.incrementAndGet()
							doer.executeSequentially(() => loop())
						}
					}

					loop()
				}
			})
		}

		// Start client threads scheduling and canceling timers to create lock contention
		val clientThreads = 16
		for (t <- 0 until clientThreads) {
			clientPool.submit(new Runnable {
				override def run(): Unit = {
					startLatch.await()
					var i = 0
					while running do {
						val doer = doers((t * 100 + i) % doerCount)
						val sched: SchedulingExtension = doer
						val schedule = sched.newDelaySchedule(10000)
						sched.scheduleSequentially(schedule, _ => ())
						sched.cancel(schedule)
						i += 1
					}
				}
			})
		}

		val startTime = System.nanoTime()
		startLatch.countDown()
		Thread.sleep(4000)
		running = false
		clientPool.shutdown()
		clientPool.awaitTermination(5, TimeUnit.SECONDS)
		val endTime = System.nanoTime()

		val durationSeconds = (endTime - startTime) / 1e9
		normalTaskCount.get().toDouble / durationSeconds
	}
}
