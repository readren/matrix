package readren.sequencer
package providers

import readren.sequencer.*
import providers.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.*

object SelectionBenchmark {
	private val DefaultDoerCount = 10000
	private val InitialFrequency = 20.0 // 50ms delay
	private val EnableSleepTracking = true

	def main(args: Array[String]): Unit = {
		println("======================================================================")
		println("STARTING PROVIDER SELECTION BENCHMARK (MULTIDIMENSIONAL) v 9")
		println("======================================================================")
		println(s"Fixed Doers count: $DefaultDoerCount")
		println("GLOSSARY OF TERMS:")
		println("  S       : Target active schedules per doer (timer density)")
		println("  C       : Target proportion of canceled schedules (cancellation rate)")
		println("  R       : Target relation of scheduled to non-scheduled executions (ratio)")
		println("  ops/s   : Operations per second (sum of completed schedules, cancellations,")
		println("            and completed normal executions per second)")
		println("  Freq    : Calibrated scheduled feeding frequency per active doer (Hz)")
		println("  Retries : Calibration iterations taken to reach 100% CPU saturation")
		println("            and parameter accuracy within 5% tolerance")
		println("======================================================================")

		val onlyTable = args.contains("--only-table") || args.contains("table")

		val cachePath = Paths.get("selection_benchmark_cache.txt")
		val cache = scala.collection.mutable.Map[(Double, Double, Double, String), (Double, Double, Double, Double, Double)]()
		if (Files.exists(cachePath)) then {
			val lines = Files.readAllLines(cachePath).asScala
			for (line <- lines if !line.trim.isEmpty && !line.startsWith("#")) {
				val parts = line.split(",")
				if (parts.length >= 7) then {
					val s = parts(0).toDouble
					val c = parts(1).toDouble
					val r = parts(2).toDouble
					val name = parts(3)
					val freq = parts(4).toDouble
					val adjC = parts(5).toDouble
					val adjR = parts(6).toDouble
					val throughput = if (parts.length >= 8) then parts(7).toDouble else 0.0
					val utilization = if (parts.length >= 9) then parts(8).toDouble else 0.0
					cache((s, c, r, name)) = (freq, adjC, adjR, throughput, utilization)
				}
			}
			println(s"Loaded ${cache.size} calibrated settings from cache file.\n")
		}

		val activeSchedulesList = List(0.1, 1.0, 10.0)
		val cancelProportions = List(0.0, 0.1, 0.5, 0.9)
		val relations = List(0.1, 1.0, 10.0)

		val poolSize = 4

		def createProvider(name: String): DoerProvider[Doer & SchedulingExtension] {type Tag = String} & ShutdownAble = {
			name match {
				case "Flat" => new CooperativeFlatPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = EnableSleepTracking)
				case "Hier" => new CooperativeHierarchicalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = EnableSleepTracking)
				case "Thread" => new CooperativeThreadDrivenSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = EnableSleepTracking)
				case "Shard" => new CooperativeShardedPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = EnableSleepTracking)
				case "Local" => new CooperativeLocalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = EnableSleepTracking)
				case _ => throw new IllegalArgumentException(s"Unknown provider: $name")
			}
		}

		val providerNames = List("Flat", "Hier", "Thread", "Shard", "Local")

		val results = scala.collection.mutable.Map[(Double, Double, Double, String), Double]()
		val sleepMap = scala.collection.mutable.Map[(Double, Double, Double, String), Double]()

		if (onlyTable) then {
			println("Running in Table-Only mode. Drawing results from cache file...")
			for (((s, c, r, name), (_, _, _, throughput, utilization)) <- cache) {
				results((s, c, r, name)) = throughput
				sleepMap((s, c, r, name)) = 1.0 - utilization
			}
		} else {
			// 1. Warm-up
			println("Performing warm-up...")
			for (name <- providerNames) {
				val dp = createProvider(name)
				try {
					executeRun(dp, doerCount = 1000, activeSchedules = 1.0, cancelProp = 0.1, relation = 1.0, timerDelayMs = 50, tasksPerTick = 1, durationMs = 300)
				} finally {
					dp.shutdown()
				}
			}
			println("Warm-up complete.\n")

			// 2. Run multidimensional benchmark
			for (s <- activeSchedulesList) {
				for (c <- cancelProportions) {
					for (r <- relations) {
						println(s"\nBenchmarking configuration: Target S=$s | Target C=$c | Target R=$r")
						for (name <- providerNames) {
							val cachedSettings = cache.get((s, c, r, name)).map(x => (x._1, x._2, x._3))
							val dp = createProvider(name)
							try {
								val (opsPerSec, actualS, actualC, actualR, finalUtilization, retries, finalFreq, finalAdjC, finalAdjR) =
									calibrateAndRunExperiment(name, dp, targetS = s, targetC = c, targetR = r, cachedSettings)
								results((s, c, r, name)) = opsPerSec
								sleepMap((s, c, r, name)) = 1.0 - finalUtilization
								cache((s, c, r, name)) = (finalFreq, finalAdjC, finalAdjR, opsPerSec, finalUtilization)

								val sleepPct = (1.0 - finalUtilization) * 100.0
								printf("\r  %-7s -> Calibrated Freq=%-5.1f Hz | Actual S=%5.2f, C=%5.1f%%, R=%5.2f | Sleep=%5.1f%% | Retries=%d | Throughput=%,.0f ops/s\n",
									name, finalFreq, actualS, actualC * 100, actualR, sleepPct, retries, opsPerSec)
							} finally {
								dp.shutdown()
							}
							System.gc()
						}
					}
				}
			}
		}

		if (!onlyTable) then {
			// Save cache back to file
			val writer = Files.newBufferedWriter(cachePath)
			try {
				writer.write("# S,C,R,ProviderName,Freq(Hz),adjC,adjR,throughput,utilization\n")
				for (((s, c, r, name), (freq, adjC, adjR, throughput, utilization)) <- cache.toSeq.sortBy(x => (x._1._1, x._1._2, x._1._3, x._1._4))) {
					writer.write(s"$s,$c,$r,$name,$freq,$adjC,$adjR,$throughput,$utilization\n")
				}
			} finally {
				writer.close()
			}
			println("\nCalibrated settings saved to selection_benchmark_cache.txt.")
		}

		// 3. Print the flattened multidimensional table
		val separator = "=" * 125
		println("\n" + separator)
		println(" " * 35 + "FLATTENED MULTIDIMENSIONAL SELECTION SUMMARY")
		println(separator)

		for (c <- cancelProportions) {
			for (s <- activeSchedulesList) {
				println(f"\n[ Schedules per Doer (S): $s%-4.1f | Cancellation Rate (C): ${c * 100}%3.0f%% ]")
				val cellSeparator = "-" * 115
				println(cellSeparator)
				printf("| %-12s | %-30s | %-30s | %-30s |\n",
					"Provider", "Scheduled/Normal Ratio = 0.1", "Scheduled/Normal Ratio = 1", "Scheduled/Normal Ratio = 10")
				println(cellSeparator)
				for (name <- providerNames) {
					val val1 = results.getOrElse((s, c, 0.1, name), 0.0)
					val slp1 = sleepMap.getOrElse((s, c, 0.1, name), 0.0) * 100.0
					val val2 = results.getOrElse((s, c, 1.0, name), 0.0)
					val slp2 = sleepMap.getOrElse((s, c, 1.0, name), 0.0) * 100.0
					val val3 = results.getOrElse((s, c, 10.0, name), 0.0)
					val slp3 = sleepMap.getOrElse((s, c, 10.0, name), 0.0) * 100.0

					val cell1 = f"${val1}%,.0f (${slp1}%2.0f%% sleep)"
					val cell2 = f"${val2}%,.0f (${slp2}%2.0f%% sleep)"
					val cell3 = f"${val3}%,.0f (${slp3}%2.0f%% sleep)"
					printf("| %-12s | %30s | %30s | %30s |\n", name, cell1, cell2, cell3)
				}
				println(cellSeparator)
			}
		}
	}

	private def calibrateAndRunExperiment(
		name: String,
		dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String},
		targetS: Double,
		targetC: Double,
		targetR: Double,
		cachedSettings: Option[(Double, Double, Double)]
	): (Double, Double, Double, Double, Double, Int, Double, Double, Double) = {
		val estimatedMaxThroughput = name match {
			case "Local" | "Thread" => 40_000_000.0
			case _ => 25_000_000.0
		}
		val activeCount = (DefaultDoerCount * targetS).max(1.0)
		val estimatedFreq = estimatedMaxThroughput / (activeCount * (1.0 + 1.0 / targetR))
		val initialEstimation = math.max(10.0, math.min(100000.0, estimatedFreq * 0.35))

		val maxFrequencyCap = name match {
			case "Local" | "Thread" => 300000.0
			case _ => 100000.0
		}

		var feedingFreq = cachedSettings.map(_._1).getOrElse(initialEstimation)
		var adjC = cachedSettings.map(_._2).getOrElse(targetC)
		var adjR = cachedSettings.map(_._3).getOrElse(targetR)

		var calibrated = false
		var finalThroughput = 0.0
		var finalActualS = 0.0
		var finalActualC = 0.0
		var finalActualR = 0.0
		var finalUtilization = 0.0
		var retries = 0

		// First try takes 1/10 of the target duration (80ms for pilot, 60ms for saturation test)
		var pilotMs = 80
		var saturationMs = 60

		while !calibrated do {
			// Calculate tasks per tick and integer delay to match timer precision
			val timerDelayMs = math.max(1, (1000.0 / feedingFreq).toInt)
			val tasksPerTick = if timerDelayMs == 1 then math.ceil(feedingFreq / 1000.0).toInt else 1

			// Run test (use 800ms if we think we are calibrated, to validate under target duration)
			val currentDuration = math.min(800, pilotMs)
			val (throughput, actualS, actualC, actualR, utilization) = executeRun(dp, DefaultDoerCount, targetS, adjC, adjR, timerDelayMs, tasksPerTick, durationMs = currentDuration)

			// Check parameter accuracy
			val sError = math.abs(actualS - targetS) / targetS
			val cError = if targetC == 0.0 then actualC else math.abs(actualC - targetC)
			val rError = if actualR.isInfinite then 1.0 else math.abs(actualR - targetR) / targetR

			val paramsAccurate = (sError < 0.05) && (cError < 0.05) && (rError < 0.05)

			// Check CPU saturation and growth: skip saturation run if utilization is low (not saturated)
			val (throughputHigher, utilizationHigher, growth) = if utilization >= 0.85 then {
				val higherFreq = feedingFreq * 1.5
				val higherTimerDelayMs = math.max(1, (1000.0 / higherFreq).toInt)
				val higherTasksPerTick = if higherTimerDelayMs == 1 then math.ceil(higherFreq / 1000.0).toInt else 1
				val satDuration = if currentDuration >= 800 then 600 else saturationMs
				val (tHigher, _, _, _, uHigher) = executeRun(dp, DefaultDoerCount, targetS, adjC, adjR, higherTimerDelayMs, higherTasksPerTick, durationMs = satDuration)
				val g = if (throughput == 0.0) then 0.0 else (tHigher - throughput) / throughput
				(tHigher, uHigher, g)
			} else {
				(0.0, 0.0, 1.0)
			}

			// Compute CPU load/saturation error based on pool sleep time (target is >= 98% busy)
			val cpuError = math.max(0.0, 0.98 - utilization)
			val utilizationThreshold = name match {
				case "Thread" => 0.70
				case "Hier" => 0.80
				case _ => 0.90
			}
			val cpuSaturated = utilization >= 0.96 || (utilization >= utilizationThreshold && growth <= 0.0)

			// Print progress status overwriting the same line
			val sErrPct = sError * 100.0
			val cErrPct = cError * 100.0
			val rErrPct = rError * 100.0
			val cpuErrPct = cpuError * 100.0
			print(f"\r  $name%-7s ... Calibrating (Errors: S=$sErrPct%5.1f%%, C=$cErrPct%5.1f%%, R=$rErrPct%5.1f%%, CPU=$cpuErrPct%5.1f%%) | Freq=$feedingFreq%6.1f Hz | Retry=$retries%-2d")
			System.out.flush()

			val atFreqCap = feedingFreq >= maxFrequencyCap
			if (paramsAccurate && cpuSaturated && currentDuration == 800) || atFreqCap || retries >= 45 then {
				calibrated = true
				finalThroughput = throughput
				finalActualS = actualS
				finalActualC = actualC
				finalActualR = actualR
				finalUtilization = utilization
			} else {
				if !paramsAccurate then {
					if actualC > 0.0 && targetC > 0.0 then adjC = math.max(0.0, math.min(0.99, adjC * (targetC / actualC)))
					if !actualR.isInfinite && actualR > 0.0 then adjR = adjR * (actualR / targetR)
				}
				if !cpuSaturated then {
					val scaleFactor =
						if growth < 0.0 then 0.9
						else if growth == 0.0 then 1.1
						else {
							val ratio = 0.95 / math.max(0.05, utilization)
							math.max(1.2, math.min(3.0, ratio))
						}
					feedingFreq = math.min(maxFrequencyCap, feedingFreq * scaleFactor)
				}

				// Update pilot and saturation durations for the next try based on discrepancy
				if paramsAccurate && cpuSaturated then {
					// We met calibration criteria for pilot, so elevate to full 800ms for final verification
					pilotMs = 800
				} else {
					val maxError = math.max(math.max(sError, cError), math.max(rError, cpuError))
					val fVal = 1.0 / (1.0 + 4.0 * maxError)
					pilotMs = math.max(80, (800 * fVal).toInt)
					saturationMs = math.max(60, (pilotMs * 0.75).toInt)
				}

				retries += 1
				System.gc()
			}
		}

		if finalThroughput == 0.0 then {
			val timerDelayMs = math.max(1, (1000.0 / feedingFreq).toInt)
			val tasksPerTick = if timerDelayMs == 1 then math.ceil(feedingFreq / 1000.0).toInt else 1
			val (finalT, fS, fC, fR, finalU) = executeRun(dp, DefaultDoerCount, targetS, adjC, adjR, timerDelayMs, tasksPerTick, durationMs = 800)
			finalThroughput = finalT
			finalActualS = fS
			finalActualC = fC
			finalActualR = fR
			finalUtilization = finalU
		}

		val finalTimerDelayMs = math.max(1, (1000.0 / feedingFreq).toInt)
		val finalTasksPerTick = if finalTimerDelayMs == 1 then math.ceil(feedingFreq / 1000.0).toInt else 1
		val actualFinalFreq = (1000.0 / finalTimerDelayMs) * finalTasksPerTick

		(finalThroughput, finalActualS, finalActualC, finalActualR, finalUtilization, retries, actualFinalFreq, adjC, adjR)
	}

	private def executeRun(
		dp: DoerProvider[Doer & SchedulingExtension] {type Tag = String},
		doerCount: Int,
		activeSchedules: Double,
		cancelProp: Double,
		relation: Double,
		timerDelayMs: Int,
		tasksPerTick: Int,
		durationMs: Int
	): (Double, Double, Double, Double, Double) = {
		val t0 = System.nanoTime()
		val workersDp = dp.asInstanceOf[CooperativeWorkersDp]

		val doers = Array.tabulate(doerCount)(i => dp.provide(s"sel-doer-$i"))
		val t1 = System.nanoTime()
		val completedSchedules = new AtomicLong(0)
		val canceledSchedules = new AtomicLong(0)
		val createdSchedules = new AtomicLong(0)
		val submittedNormalTasks = new AtomicLong(0)
		val completedNormalTasks = new AtomicLong(0)

		@volatile var running = true
		val startLatch = new CountDownLatch(1)

		val normalTask = new Runnable {
			override def run(): Unit = {
				completedNormalTasks.incrementAndGet()
			}
		}

		def timerCallback(sched: SchedulingExtension, doer: Doer): Unit = {
			if running then {
				val completedSched = completedSchedules.incrementAndGet()

				// Schedule replacement timer
				val replacement = sched.newDelaySchedule(timerDelayMs)
				sched.scheduleSequentially(replacement, _ => timerCallback(sched, doer))
				createdSchedules.incrementAndGet()

				// Simulate cancellations
				if cancelProp > 0.0 then {
					val targetCanceled = (cancelProp * completedSched / (1.0 - cancelProp)).toLong
					var cancelSpin = 0
					while canceledSchedules.get() < targetCanceled do {
						val temp = sched.newDelaySchedule(10000)
						sched.scheduleSequentially(temp, _ => ())
						sched.cancel(temp)
						createdSchedules.incrementAndGet()
						canceledSchedules.incrementAndGet()
						cancelSpin += 1
						if cancelSpin > 5000000 then {
							println(f"\n[WARN] cancel loop spinning on doer=${doer.tag}: cancelSpin=$cancelSpin, canceledSchedules=${canceledSchedules.get()}, targetCanceled=$targetCanceled, completedSched=$completedSched, cancelProp=$cancelProp%.6f")
							cancelSpin = 0
						}
					}
				}

				// Simulate normal tasks
				val targetNormal = (completedSched / relation).toLong
				var normalSpin = 0
				while submittedNormalTasks.get() < targetNormal do {
					doer.executeSequentially(normalTask)
					submittedNormalTasks.incrementAndGet()
					normalSpin += 1
					if normalSpin > 5000000 then {
						println(f"\n[WARN] normal loop spinning on doer=${doer.tag}: normalSpin=$normalSpin, submittedNormalTasks=${submittedNormalTasks.get()}, targetNormal=$targetNormal, completedSched=$completedSched, relation=$relation%.6f")
						normalSpin = 0
					}
				}
			}
		}

		// Pre-populate active schedules (physically multiplying active schedules in heap by tasksPerTick)
		val schedulesToCreate = if activeSchedules < 1.0 then {
			if activeSchedules > 0.0 then (doerCount * activeSchedules).toInt else 0
		} else activeSchedules.toInt

		for (i <- 0 until doerCount) {
			val doer = doers(i)
			val sched: SchedulingExtension = doer

			val activeCount = if activeSchedules < 1.0 then {
				if i < schedulesToCreate then 1 else 0
			} else activeSchedules.toInt

			val totalActiveCount = activeCount * tasksPerTick
			if totalActiveCount > 0 then {
				doer.executeSequentially(new Runnable {
					override def run(): Unit = {
						startLatch.await()
						for (j <- 0 until totalActiveCount) {
							val schedule = sched.newDelaySchedule(timerDelayMs + j * 5)
							sched.scheduleSequentially(schedule, _ => timerCallback(sched, doer))
							createdSchedules.incrementAndGet()
						}
					}
				})
			}
		}

		val t2 = System.nanoTime()
		val sleepStartTimes = if EnableSleepTracking then workersDp.workersSleepTimeNanos else Array.empty[Long]
		val schedulerSleepStart = if EnableSleepTracking then {
			dp match {
				case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
				case _ => -1L
			}
		} else -1L
		val startTime = t2
		startLatch.countDown()
		Thread.sleep(durationMs)
		running = false
		val t3 = System.nanoTime()
		val sleepEndTimes = if EnableSleepTracking then workersDp.workersSleepTimeNanos else Array.empty[Long]
		val schedulerSleepEnd = if EnableSleepTracking then {
			dp match {
				case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
				case _ => -1L
			}
		} else -1L
		val endTime = t3

		val activeDoerCount = if activeSchedules < 1.0 then schedulesToCreate else doerCount
		// Instead of O(N^2) cancelAll, we sleep to let remaining active timers drain naturally.
		val drainMs = math.max(60, (doerCount * 0.0003 * tasksPerTick).toInt)
		Thread.sleep(drainMs)
		val t4 = System.nanoTime()

		val doerCreationMs = (t1 - t0) / 1000000.0
		val schedulingMs = (t2 - t1) / 1000000.0
		val sleepMs = (t3 - t2) / 1000000.0
		val cleanupMs = (t4 - t3) / 1000000.0

		val totalDurationNanos = endTime - startTime
		val utilization = if EnableSleepTracking then {
			val poolSize = sleepStartTimes.length
			var totalSleepNanos = 0L
			var idx = 0
			while idx < poolSize do {
				totalSleepNanos += (sleepEndTimes(idx) - sleepStartTimes(idx))
				idx += 1
			}

			// The startup period (before the first timer expires) has no timer expirations, so workers sleep.
			val startupDelayNanos = timerDelayMs.toLong * 1000000L
			val activeDurationNanos = totalDurationNanos - startupDelayNanos

			val workersUtilization = if (activeDurationNanos <= 0) then 0.0 else {
				val inevitableSleep = poolSize * startupDelayNanos
				val activeSleep = math.max(0L, totalSleepNanos - inevitableSleep)
				1.0 - (activeSleep.toDouble / (activeDurationNanos.toDouble * poolSize))
			}

			val schedulerUtilization = if schedulerSleepStart >= 0L && activeDurationNanos > 0 then {
				val schedSleep = schedulerSleepEnd - schedulerSleepStart
				val activeSleep = math.max(0L, schedSleep - startupDelayNanos)
				1.0 - (activeSleep.toDouble / activeDurationNanos.toDouble)
			} else 0.0

			math.max(workersUtilization, schedulerUtilization)
		} else {
			1.0
		}

		val compS = completedSchedules.get()
		val cancS = canceledSchedules.get()
		val compN = completedNormalTasks.get()
		val createdS = createdSchedules.get()

		val actualS = (createdS - compS - cancS).toDouble / (doerCount.toDouble * tasksPerTick)
		val actualC = if (compS + cancS > 0) then cancS.toDouble / (compS + cancS).toDouble else 0.0
		val actualR = if (compN > 0) then compS.toDouble / compN.toDouble else Double.PositiveInfinity

		val totalOps = compS + cancS + compN
		val throughput = totalOps.toDouble / (durationMs.toDouble / 1000.0)

		if true then {
			println(f"\n[DEBUG] executeRun (doers=$doerCount, delay=${timerDelayMs}ms, tpt=$tasksPerTick): creation=${doerCreationMs}%.1fms, scheduling=${schedulingMs}%.1fms, sleep=${sleepMs}%.1fms, cleanup=${cleanupMs}%.1fms, completedS=$compS, createdS=$createdS, completedN=$compN, throughput=$throughput%.0f, utilization=${utilization * 100.0}%.1f%%")
		}

		if (doerCount >= 100000 && throughput == 0.0) then {
			println(s"\n=== THREAD DUMP FOR 0 THROUGHPUT (doerCount=$doerCount) ===")
			val threadMXBean = java.lang.management.ManagementFactory.getThreadMXBean()
			val threadInfos = threadMXBean.dumpAllThreads(true, true)
			for (info <- threadInfos) {
				println(info.toString)
			}
			println("====================================\n")
		}

		(throughput, actualS, actualC, actualR, utilization)
	}
}
