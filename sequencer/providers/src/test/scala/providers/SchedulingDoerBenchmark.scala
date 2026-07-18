package readren.sequencer
package providers

import readren.sequencer.*
import providers.*

import java.nio.file.{Files, Paths, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, ThreadLocalRandom}
import scala.jdk.CollectionConverters.*

/** Case classes to model execution workloads and results without using generic Tuples. */

/** Represents a target benchmark execution scenario configuration.
 *
 * @param targetActiveSchedulesPerDoer Target density of active schedules per doer.
 * @param targetCanceledSchedulesFraction Target fraction of canceled schedules.
 * @param targetScheduledVsRegularTasksRatio Target ratio of scheduled tasks to regular tasks. */
case class Scenario(
	targetActiveSchedulesPerDoer: Double,
	targetCanceledSchedulesFraction: Double,
	targetScheduledVsRegularTasksRatio: Double
)

/** Key identifying a specific provider running under a specific scenario.
 *
 * @param scenario The target configuration scenario.
 * @param providerName The name of the sequencer provider implementation. */
case class ProviderScenario(
	scenario: Scenario,
	providerName: String
)

/** Represents settings cached in the settings cache file.
 *
 * @param calibratedFrequency Calibrated feeding frequency (Hz).
 * @param adjustedCanceledSchedulesFraction Actual adjusted cancellation fraction.
 * @param adjustedScheduledVsRegularRatio Actual adjusted scheduled-to-regular task ratio.
 * @param throughput Measured execution throughput (ops/sec).
 * @param cpuUtilization Measured CPU utilization (fraction).
 * @param doerCount Total number of doers provisioned during execution. */
case class CachedSettings(
	calibratedFrequency: Double,
	adjustedCanceledSchedulesFraction: Double,
	adjustedScheduledVsRegularRatio: Double,
	throughput: Double,
	cpuUtilization: Double,
	doerCount: Int
)

/** Holds the final calibration and metric results of an experiment. */
case class CalibrationResult(
	throughput: Double,
	actualActiveSchedulesPerDoer: Double,
	actualCanceledSchedulesFraction: Double,
	actualScheduledVsRegularTaskRatio: Double,
	cpuUtilization: Double,
	retries: Int,
	calibratedFrequency: Double,
	adjustedCanceledFraction: Double,
	adjustedScheduledVsRegularRatio: Double
)

/** Holds the metric results returned from a single experiment execution run. */
case class ExperimentRunResult(
	throughput: Double,
	actualActiveSchedulesPerDoer: Double,
	actualCanceledSchedulesFraction: Double,
	actualScheduledVsRegularTaskRatio: Double,
	actualCpuUtilization: Double
)

/** A refactored and documented version of the selection benchmark.
 *
 * This class measures the performance of different [[DoerProvider]] implementations
 * under multidimensional execution workloads containing schedules, cancellations,
 * and regular tasks. */
object SchedulingDoerBenchmark {
	type Sequencer = Doer & SchedulingExtension
	type SequencerProvider = DoerProvider[Sequencer] {type Tag = String} & ShutdownAble

	private val INITIAL_TARGET_ACTIVATION_SCHEDULES = 20000
	private val ENABLE_SLEEP_TRACKING = true
	private val TARGET_EXPERIMENT_DURATION = 800 // milliseconds
	private val MAX_JITTER_MS = 16.0

	def main(args: Array[String]): Unit = {
		println("======================================================================")
		println("STARTING PROVIDER SELECTION BENCHMARK (REFACTORED)")
		println("======================================================================")
		println(s"Initial target active schedules in queue: $INITIAL_TARGET_ACTIVATION_SCHEDULES")
		println("======================================================================")

		val onlyTable = args.contains("--only-table") || args.contains("table")
		val cachePath = Paths.get("selection_benchmark_cache.txt")
		val cache = scala.collection.mutable.Map[ProviderScenario, CachedSettings]()
		val doerCounts = scala.collection.mutable.Map[Scenario, Int]()

		loadCache(cachePath, cache)

		val activeSchedulesPerDoerTargets = List(0.1, 1.0, 10.0)
		val cancelledSchedulesFractionTargets = List(0.0, 0.1, 0.5, 0.9)
		val scheduledVsRegularTasksRatioTargets = List(0.1, 1.0, 10.0)
		val poolSize = 4

		def createProvider(providerName: String): SequencerProvider = {
			providerName match {
				case "Flat" => new CooperativeFlatPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Hier" => new CooperativeHierarchicalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Thread" => new CooperativeThreadDrivenSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Shard" => new CooperativeShardedPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Local" => new CooperativeLocalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = poolSize, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case _ => throw new IllegalArgumentException(s"Unknown provider: $providerName")
			}
		}

		val providerNames = List("Local", "Shard", "Flat", "Hier", "Thread")
		val results = scala.collection.mutable.Map[ProviderScenario, Double]()
		val sleepMap = scala.collection.mutable.Map[ProviderScenario, Double]()

		if onlyTable then {
			println("Running in Table-Only mode. Drawing results from cache file...")
			for ((ProviderScenario(scenario, providerName), cachedSettings) <- cache) {
				results(ProviderScenario(scenario, providerName)) = cachedSettings.throughput
				sleepMap(ProviderScenario(scenario, providerName)) = 1.0 - cachedSettings.cpuUtilization
				doerCounts(scenario) = cachedSettings.doerCount
			}
		} else {
			println("Performing warm-up...")
			for (providerName <- providerNames) {
				val doerProvider = createProvider(providerName)
				try {
					val warmUpExperiment = new Experiment(
						doerProvider = doerProvider,
						doerCount = 1000,
						scenario = Scenario(
							targetActiveSchedulesPerDoer = 1.0,
							targetCanceledSchedulesFraction = 0.1,
							targetScheduledVsRegularTasksRatio = 1.0
						),
						timerDelayMs = 50,
						durationMs = 300
					)
					warmUpExperiment.run()
				} finally {
					doerProvider.shutdown()
				}
			}
			println("Warm-up complete.\n")

			// Flatten and sort configurations to execute those requiring highest frequency first
			val configs = for {
				targetActiveSchedulesPerDoer <- activeSchedulesPerDoerTargets
				targetCancelledSchedulesFraction <- cancelledSchedulesFractionTargets
				targetScheduledVsRegularTasksRatio <- scheduledVsRegularTasksRatioTargets
			} yield Scenario(targetActiveSchedulesPerDoer, targetCancelledSchedulesFraction, targetScheduledVsRegularTasksRatio)

			val sortedConfigs = configs.sortBy { scenario =>
				1.0 + (1.0 / scenario.targetScheduledVsRegularTasksRatio) + (scenario.targetCanceledSchedulesFraction / (1.0 - scenario.targetCanceledSchedulesFraction))
			}

			for (scenario <- sortedConfigs) {
				var targetActiveSchedules = INITIAL_TARGET_ACTIVATION_SCHEDULES.toDouble
				var completedConfig = false

				while !completedConfig do {
					val numberOfDoers = (targetActiveSchedules / scenario.targetActiveSchedulesPerDoer).round.toInt.max(1)
					println(s"\nBenchmarking configuration: active schedules/doer=${scenario.targetActiveSchedulesPerDoer} | cancels%=${scenario.targetCanceledSchedulesFraction * 100}%3.0f%% | scheduled/immediate=${scenario.targetScheduledVsRegularTasksRatio} | doers=$numberOfDoers | queueSize=$targetActiveSchedules")
					var needRedo = false
					val tempResults = scala.collection.mutable.Map[String, CalibrationResult]()

					// Run a pilot of all providers at 1ms delay to find their relative speeds under load
					val pilotThroughputs = scala.collection.mutable.Map[String, Double]()
					for (providerName <- providerNames) {
						val doerProvider = createProvider(providerName)
						try {
							val pilotExperiment = new Experiment(
								doerProvider = doerProvider,
								doerCount = numberOfDoers,
								scenario = scenario,
								timerDelayMs = 1,
								durationMs = 200
							)
							val runResult = pilotExperiment.run()
							pilotThroughputs(providerName) = runResult.throughput
						} finally {
							doerProvider.shutdown()
						}
					}

					// Sort providers: fastest (highest pilot throughput) first
					val orderedProviders = providerNames.sortBy(providerName => -pilotThroughputs(providerName))
					var providerIndex = 0

					while providerIndex < orderedProviders.length && !needRedo do {
						val providerName = orderedProviders(providerIndex)
						val key = ProviderScenario(scenario, providerName)
						val cachedSettings = cache.get(key)
						val doerProvider = createProvider(providerName)
						try {
							val calibrationResult = calibrateAndRunExperiment(
								providerName = providerName,
								doerProvider = doerProvider,
								numberOfDoers = numberOfDoers,
								scenario = scenario,
								cachedSettings = cachedSettings
							)
							val finalFreq = calibrationResult.calibratedFrequency
							val finalUtilization = calibrationResult.cpuUtilization

							val finalTimerDelayMs = math.max(1, (1000.0 / finalFreq).toInt)
							val utilizationThreshold = providerName match {
								case "Thread" => 0.70
								case "Hier" => 0.80
								case _ => 0.90
							}

							if finalTimerDelayMs == 1 && finalUtilization < utilizationThreshold then {
								// Double queue size and trigger redo
								targetActiveSchedules *= 2.0
								needRedo = true
								println(s"\n[WARNING] $providerName failed to saturate CPU at 1000Hz limit (utilization=${finalUtilization * 100}%2.1f%%). Doubling targetActiveSchedules to $targetActiveSchedules and restarting configuration...")
							} else {
								tempResults(providerName) = calibrationResult
							}
						} finally {
							doerProvider.shutdown()
						}
						System.gc()
						providerIndex += 1
					}

					if !needRedo then {
						completedConfig = true
						doerCounts(scenario) = numberOfDoers
						for (providerName <- orderedProviders) {
							val calibrationResult = tempResults(providerName)
							val key = ProviderScenario(scenario, providerName)
							results(key) = calibrationResult.throughput
							sleepMap(key) = 1.0 - calibrationResult.cpuUtilization
							cache(key) = CachedSettings(
								calibratedFrequency = calibrationResult.calibratedFrequency,
								adjustedCanceledSchedulesFraction = calibrationResult.adjustedCanceledFraction,
								adjustedScheduledVsRegularRatio = calibrationResult.adjustedScheduledVsRegularRatio,
								throughput = calibrationResult.throughput,
								cpuUtilization = calibrationResult.cpuUtilization,
								doerCount = numberOfDoers
							)

							val sleepPct = (1.0 - calibrationResult.cpuUtilization) * 100.0
							printf("  %-7s -> Calibrated Freq=%-5.1f Hz | Actual S=%5.2f, C=%5.1f%%, R=%5.2f | Sleep=%5.1f%% | Retries=%d | Throughput=%s\n",
								providerName, calibrationResult.calibratedFrequency, calibrationResult.actualActiveSchedulesPerDoer, calibrationResult.actualCanceledSchedulesFraction * 100, calibrationResult.actualScheduledVsRegularTaskRatio, sleepPct, calibrationResult.retries, formatThroughput(calibrationResult.throughput))
						}

						saveCache(cache, cachePath, onlyTable)
					}
				}
			}

			saveCache(cache, cachePath, onlyTable)
			println("\nCalibrated settings saved to selection_benchmark_cache.txt.")
		}

		val separator = "=" * 125
		println("\n" + separator)
		println(" " * 35 + "FLATTENED MULTIDIMENSIONAL SELECTION SUMMARY")
		println(separator)

		for (targetCanceledSchedulesFraction <- cancelledSchedulesFractionTargets) {
			for (targetActiveSchedulesPerDoer <- activeSchedulesPerDoerTargets) {
				println(f"\n[ Active schedules per Doer: $targetActiveSchedulesPerDoer%-4.1f | Canceled percentage: ${targetCanceledSchedulesFraction * 100}%3.0f%% ]")

				val colWidth = 30
				val headerFormatBuilder = new StringBuilder("| %-12s |")
				val headerArgs = scala.collection.mutable.ListBuffer[Any]("Provider")

				for (targetScheduledVsRegularTasksRatio <- scheduledVsRegularTasksRatioTargets) {
					headerFormatBuilder.append(s" %-${colWidth}s |")
					val scenario = Scenario(
						targetActiveSchedulesPerDoer = targetActiveSchedulesPerDoer,
						targetCanceledSchedulesFraction = targetCanceledSchedulesFraction,
						targetScheduledVsRegularTasksRatio = targetScheduledVsRegularTasksRatio
					)
					val doerCount = doerCounts.getOrElse(scenario, (INITIAL_TARGET_ACTIVATION_SCHEDULES / targetActiveSchedulesPerDoer).round.toInt.max(1))
					headerArgs.append(f"Sched/Regul= $targetScheduledVsRegularTasksRatio (${doerCount / 1000}%,dk doers)")
				}

				val cellSeparator = "-" * (12 + 4 + scheduledVsRegularTasksRatioTargets.length * (colWidth + 3) + 1)
				println(cellSeparator)
				printf(headerFormatBuilder.toString() + "\n", headerArgs.toSeq *)
				println(cellSeparator)

				for (providerName <- providerNames) {
					val rowArgs = scala.collection.mutable.ListBuffer[Any](providerName)
					for (targetScheduledVsRegularTasksRatio <- scheduledVsRegularTasksRatioTargets) {
						val key = ProviderScenario(
							Scenario(
								targetActiveSchedulesPerDoer = targetActiveSchedulesPerDoer,
								targetCanceledSchedulesFraction = targetCanceledSchedulesFraction,
								targetScheduledVsRegularTasksRatio = targetScheduledVsRegularTasksRatio
							),
							providerName
						)
						val throughput = results.getOrElse(key, 0.0)
						val sleepPercentage = sleepMap.getOrElse(key, 0.0) * 100.0
						rowArgs.append(f"${formatThroughput(throughput)} ($sleepPercentage%2.0f%% sleep)")
					}
					val rowFormatStr = "| %-12s |" + scheduledVsRegularTasksRatioTargets.map(_ => s" %${colWidth}s |").mkString
					printf(rowFormatStr + "\n", rowArgs.toSeq *)
				}
				println(cellSeparator)
			}
		}
	}

	def formatThroughput(opsPerSec: Double): String = {
		val megas = opsPerSec / 1_000_000.0
		if megas == 0.0 then "0.00M"
		else if megas >= 100.0 then f"$megas%.0fM"
		else if megas >= 10.0 then f"$megas%.1fM"
		else if megas >= 1.0 then f"$megas%.2fM"
		else if megas >= 0.1 then f"$megas%.3fM"
		else if megas >= 0.01 then f"$megas%.4fM"
		else f"$megas%.5fM"
	}

	def saveCache(
		cache: scala.collection.mutable.Map[ProviderScenario, CachedSettings],
		cachePath: Path,
		onlyTable: Boolean
	): Unit = {
		if !onlyTable then {
			val writer = Files.newBufferedWriter(cachePath)
			try {
				writer.write("# S,C,R,ProviderName,Freq(Hz),adjC,adjR,throughput,utilization,doers\n")
				val sortedCache = cache.toSeq.sortBy { case (providerScenario, cachedSettings) =>
					(
						providerScenario.scenario.targetActiveSchedulesPerDoer,
						providerScenario.scenario.targetCanceledSchedulesFraction,
						providerScenario.scenario.targetScheduledVsRegularTasksRatio,
						providerScenario.providerName
					)
				}
				for ((providerScenario, cachedSettings) <- sortedCache) {
					writer.write(s"${providerScenario.scenario.targetActiveSchedulesPerDoer},${providerScenario.scenario.targetCanceledSchedulesFraction},${providerScenario.scenario.targetScheduledVsRegularTasksRatio},${providerScenario.providerName},${cachedSettings.calibratedFrequency},${cachedSettings.adjustedCanceledSchedulesFraction},${cachedSettings.adjustedScheduledVsRegularRatio},${cachedSettings.throughput},${cachedSettings.cpuUtilization},${cachedSettings.doerCount}\n")
				}
			} finally {
				writer.close()
			}
		}
	}

	def loadCache(
		cachePath: Path,
		cache: scala.collection.mutable.Map[ProviderScenario, CachedSettings]
	): Unit = {
		if Files.exists(cachePath) then {
			val lines = Files.readAllLines(cachePath).asScala
			for (line <- lines if line.trim.nonEmpty && !line.startsWith("#")) {
				val parts = line.split(",")
				if parts.length >= 10 then {
					val targetActiveSchedulesPerDoer = parts(0).toDouble
					val targetCanceledSchedulesFraction = parts(1).toDouble
					val targetScheduledVsRegularTasksRatio = parts(2).toDouble
					val providerName = parts(3)
					val calibratedFrequency = parts(4).toDouble
					val adjustedCanceledFraction = parts(5).toDouble
					val adjustedScheduledVsRegularRatio = parts(6).toDouble
					val throughput = parts(7).toDouble
					val cpuUtilization = parts(8).toDouble
					val doerCount = parts(9).toInt

					val key = ProviderScenario(Scenario(targetActiveSchedulesPerDoer, targetCanceledSchedulesFraction, targetScheduledVsRegularTasksRatio), providerName)
					cache(key) = CachedSettings(calibratedFrequency, adjustedCanceledFraction, adjustedScheduledVsRegularRatio, throughput, cpuUtilization, doerCount)
				}
			}
			println(s"Loaded ${cache.size} calibrated settings from cache file.\n")
		}
	}

	/** Calibrates the feeding frequency to achieve CPU saturation and parameter accuracy.
	 *
	 * @param providerName The name of the DoerProvider being evaluated.
	 * @param doerProvider The DoerProvider instance.
	 * @param numberOfDoers the number of doers that should participante in the experiment.
	 * @param scenario The target execution scenario configuration.
	 * @param cachedSettings Cached settings if previously run.
	 * @return A CalibrationResult containing the calibrated metrics and execution stats. */
	private def calibrateAndRunExperiment(
		providerName: String,
		doerProvider: SequencerProvider,
		numberOfDoers: Int,
		scenario: Scenario,
		cachedSettings: Option[CachedSettings]
	): CalibrationResult = {
		val targetActiveSchedulesPerDoer = scenario.targetActiveSchedulesPerDoer
		val targetCanceledSchedulesFraction = scenario.targetCanceledSchedulesFraction
		val targetScheduledVsRegularTasksRatio = scenario.targetScheduledVsRegularTasksRatio

		val minimumFeedingFrequency = 1.0
		val maximumFeedingFrequency = 1000.0

		val initialEstimation = math.sqrt(minimumFeedingFrequency * maximumFeedingFrequency) // ~31.6 Hz
		val maxFrequencyCap = maximumFeedingFrequency

		var feedingFreq = cachedSettings.map(_.calibratedFrequency).getOrElse(initialEstimation)
		var feedingPeriodMillis = (1000.0 / feedingFreq).round.toInt.max(1)
		feedingFreq = 1000.0 / feedingPeriodMillis

		var calibrated = false
		var previousRunWasFullAndSaturated = false
		var finalThroughput = 0.0
		var finalActualActiveSchedulesPerDoer = 0.0
		var finalActualCanceledSchedulesFraction = 0.0
		var finalActualScheduledVsRegularTaskRatio = 0.0
		var finalUtilization = 0.0
		var retries = 0

		var pilotDurationMs = TARGET_EXPERIMENT_DURATION / 10

		while !calibrated do {
			val currentExperimentDuration = math.min(TARGET_EXPERIMENT_DURATION, pilotDurationMs)
			val experiment = new Experiment(doerProvider, numberOfDoers, scenario, feedingPeriodMillis, currentExperimentDuration)
			val runResult = experiment.run()

			val activeSchedulesPerDoerError = math.abs(runResult.actualActiveSchedulesPerDoer - targetActiveSchedulesPerDoer) / targetActiveSchedulesPerDoer
			val canceledFractionError = if targetCanceledSchedulesFraction == 0.0 then runResult.actualCanceledSchedulesFraction else math.abs(runResult.actualCanceledSchedulesFraction - targetCanceledSchedulesFraction)
			val ratioError = if runResult.actualScheduledVsRegularTaskRatio.isInfinite then 1.0 else math.abs(runResult.actualScheduledVsRegularTaskRatio - targetScheduledVsRegularTasksRatio) / targetScheduledVsRegularTasksRatio

			val parametersAccurate = (activeSchedulesPerDoerError < 0.05) && (canceledFractionError < 0.05) && (ratioError < 0.05)

			val cpuUtilizationError = math.max(0.0, 0.98 - runResult.actualCpuUtilization)
			val cpuSaturated = runResult.actualCpuUtilization >= 0.98

			val schedulesPerDoerErrorPercentage = activeSchedulesPerDoerError * 100.0
			val cancellationPercentageErrorPercentage = canceledFractionError * 100.0
			val scheduledVersusRegularTasksRatioErrorPercentage = ratioError * 100.0
			val cpuUtilizationErrorPercentage = cpuUtilizationError * 100.0
			println(f"  $providerName%-7s ... Calibrating (Errors: S=$schedulesPerDoerErrorPercentage%5.1f%%, C=$cancellationPercentageErrorPercentage%5.1f%%, R=$scheduledVersusRegularTasksRatioErrorPercentage%5.1f%%, CPU=$cpuUtilizationErrorPercentage%5.1f%%) | Freq=$feedingFreq%6.1f Hz | Retry=$retries%-2d")

			val repeatRun = previousRunWasFullAndSaturated && cpuSaturated && (currentExperimentDuration == TARGET_EXPERIMENT_DURATION)
			val atFreqCap = feedingFreq >= maxFrequencyCap
			if (parametersAccurate && cpuSaturated && currentExperimentDuration == TARGET_EXPERIMENT_DURATION) || repeatRun || atFreqCap || retries >= 45 then {
				calibrated = true
				finalThroughput = runResult.throughput
				finalActualActiveSchedulesPerDoer = runResult.actualActiveSchedulesPerDoer
				finalActualCanceledSchedulesFraction = runResult.actualCanceledSchedulesFraction
				finalActualScheduledVsRegularTaskRatio = runResult.actualScheduledVsRegularTaskRatio
				finalUtilization = runResult.actualCpuUtilization
			} else {
				if cpuSaturated && currentExperimentDuration == TARGET_EXPERIMENT_DURATION then {
					previousRunWasFullAndSaturated = true
				} else {
					previousRunWasFullAndSaturated = false
				}
				if !cpuSaturated then {
					val ratio = 0.98 / math.max(0.05, runResult.actualCpuUtilization)
					val scaleFactor = math.max(1.2, math.min(3.0, ratio))
					val dampedScaleFactor = 1.0 + 0.5 * (scaleFactor - 1.0)
					val proposedFreq = math.min(maxFrequencyCap, feedingFreq * dampedScaleFactor)
					var proposedDelay = (1000.0 / proposedFreq).round.toInt.max(1)

					if proposedDelay == feedingPeriodMillis then {
						if dampedScaleFactor > 1.0 then {
							proposedDelay = (feedingPeriodMillis - 1).max(1)
						} else if dampedScaleFactor < 1.0 then {
							proposedDelay = feedingPeriodMillis + 1
						}
					}

					feedingPeriodMillis = proposedDelay
					feedingFreq = 1000.0 / feedingPeriodMillis
				}

				if cpuSaturated then {
					pilotDurationMs = TARGET_EXPERIMENT_DURATION
				} else {
					val maxError = math.max(math.max(activeSchedulesPerDoerError, canceledFractionError), math.max(ratioError, cpuUtilizationError))
					val dampingScaleFactor = 1.0 / (1.0 + 4.0 * maxError)
					pilotDurationMs = math.max(TARGET_EXPERIMENT_DURATION / 10, (TARGET_EXPERIMENT_DURATION * dampingScaleFactor).toInt)
				}

				retries += 1
				System.gc()
			}
		}

		if finalThroughput == 0.0 then {
			val finalTimerDelayMs = (1000.0 / feedingFreq).round.toInt.max(1)
			val finalExperiment = new Experiment(doerProvider, numberOfDoers, scenario, finalTimerDelayMs, durationMs = TARGET_EXPERIMENT_DURATION)
			val runResult = finalExperiment.run()
			finalThroughput = runResult.throughput
			finalActualActiveSchedulesPerDoer = runResult.actualActiveSchedulesPerDoer
			finalActualCanceledSchedulesFraction = runResult.actualCanceledSchedulesFraction
			finalActualScheduledVsRegularTaskRatio = runResult.actualScheduledVsRegularTaskRatio
			finalUtilization = runResult.actualCpuUtilization
		}

		CalibrationResult(
			throughput = finalThroughput,
			actualActiveSchedulesPerDoer = finalActualActiveSchedulesPerDoer,
			actualCanceledSchedulesFraction = finalActualCanceledSchedulesFraction,
			actualScheduledVsRegularTaskRatio = finalActualScheduledVsRegularTaskRatio,
			cpuUtilization = finalUtilization,
			retries = retries,
			calibratedFrequency = feedingFreq,
			adjustedCanceledFraction = targetCanceledSchedulesFraction,
			adjustedScheduledVsRegularRatio = targetScheduledVsRegularTasksRatio
		)
	}

	/** Represents a single isolated benchmark execution run.
	 *
	 * Owns the lifecycle of the doers, tokens, and verification parameters.
	 *
	 * @param doerProvider The DoerProvider instance.
	 * @param doerCount Total number of doers to provision.
	 * @param scenario The execution target parameters (density, cancel fraction, relation ratio).
	 * @param timerDelayMs Delay in milliseconds for scheduled timer tasks.
	 * @param durationMs Run duration in milliseconds. */
	private class Experiment(
		private val doerProvider: SchedulingDoerBenchmark.SequencerProvider,
		private val doerCount: Int,
		private val scenario: Scenario,
		private val timerDelayMs: Int,
		private val durationMs: Int
	) {
		private val doers: Array[SchedulingDoerBenchmark.Sequencer] = Array.tabulate(doerCount)(i => doerProvider.provide(s"sel-doer-$i"))
		private val completedScheduledTasks = new AtomicLong(0)
		private val completedNormalTasks = new AtomicLong(0)

		@volatile private var running = true

		private val activeSchedulesTarget = (doerCount * scenario.targetActiveSchedulesPerDoer).round.toInt.max(1)

		// Flat arrays to store the final counts for each token slot
		private val scheduledTasksCreated = new Array[Long](activeSchedulesTarget)
		private val scheduledTasksCanceled = new Array[Long](activeSchedulesTarget)
		private val normalTasksCreated = new Array[Long](activeSchedulesTarget)

		/** Executes the experiment run and returns the calculated metrics. */
		def run(): ExperimentRunResult = {
			val workersDp = doerProvider match {
				case w: CooperativeWorkersDp => w
				case _ => throw new RuntimeException("Expected a worker-based doer provider")
			}

			val prepopulationLatch = new CountDownLatch(activeSchedulesTarget)

			// Pre-populate schedules to start token timer loops
			for (i <- 0 until activeSchedulesTarget) {
				val doer = doers(i % doerCount)

				doer.executeSequentially(new Runnable {
					override def run(): Unit = {
						doStep(tokenId = i, scheduledTasksCreatedCount = 0L, normalTaskCreatedCount = 0L, scheduledTasksCanceledCount = 0L)
						prepopulationLatch.countDown()
					}
				})
			}

			prepopulationLatch.await()
			val t2 = System.nanoTime()
			val sleepStartTimes = if ENABLE_SLEEP_TRACKING then workersDp.workersSleepTimeNanos else Array.empty[Long]
			val schedulerSleepStart = if ENABLE_SLEEP_TRACKING then {
				doerProvider match {
					case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
					case _ => -1L
				}
			} else -1L

			Thread.sleep(durationMs)
			running = false
			val t3 = System.nanoTime()

			val sleepEndTimes = if ENABLE_SLEEP_TRACKING then workersDp.workersSleepTimeNanos else Array.empty[Long]
			val schedulerSleepEnd = if ENABLE_SLEEP_TRACKING then {
				doerProvider match {
					case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
					case _ => -1L
				}
			} else -1L
			val endTime = t3

			var totalNormalTasksCreatedCount = 0L
			var idxSum = 0
			while idxSum < activeSchedulesTarget do {
				totalNormalTasksCreatedCount += normalTasksCreated(idxSum)
				idxSum += 1
			}

			// Drain phase: wait until all enqueued normal tasks have been completed
			val drainStart = System.currentTimeMillis()
			while completedNormalTasks.get() < totalNormalTasksCreatedCount && (System.currentTimeMillis() - drainStart) < 500 do {
				Thread.sleep(1)
			}
			val t4 = System.nanoTime()

			val totalDurationNanos = endTime - t2
			val utilization = if SchedulingDoerBenchmark.ENABLE_SLEEP_TRACKING then {
				val poolSize = sleepStartTimes.length
				var totalSleepNanos = 0L
				var idx = 0
				while idx < poolSize do {
					totalSleepNanos += (sleepEndTimes(idx) - sleepStartTimes(idx))
					idx += 1
				}

				val startupDelayNanos = timerDelayMs.toLong * 1000000L
				val activeDurationNanos = totalDurationNanos - startupDelayNanos

				val jitterMarginNanos = math.max(0.0, SchedulingDoerBenchmark.MAX_JITTER_MS * (1.0 - durationMs.toDouble / SchedulingDoerBenchmark.TARGET_EXPERIMENT_DURATION.toDouble)) * 1000000.0

				val workersUtilization = if activeDurationNanos <= 0 then 0.0 else {
					val inevitableSleep = poolSize * startupDelayNanos + jitterMarginNanos.toLong
					val activeSleep = math.max(0L, totalSleepNanos - inevitableSleep)
					1.0 - (activeSleep.toDouble / (activeDurationNanos.toDouble * poolSize))
				}

				val schedulerUtilization = if schedulerSleepStart >= 0L && activeDurationNanos > 0 then {
					val schedSleep = schedulerSleepEnd - schedulerSleepStart
					val inevitableSleep = startupDelayNanos + jitterMarginNanos.toLong
					val activeSleep = math.max(0L, schedSleep - inevitableSleep)
					1.0 - (activeSleep.toDouble / activeDurationNanos.toDouble)
				} else 0.0

				math.max(workersUtilization, schedulerUtilization)
			} else {
				1.0
			}

			val completedScheduledTasksCount = completedScheduledTasks.get()
			val completedNormalTasksCount = completedNormalTasks.get()
			var totalScheduledTasksCreatedCount = 0L
			var totalScheduledTasksCanceledCount = 0L
			var idx = 0
			while idx < activeSchedulesTarget do {
				totalScheduledTasksCreatedCount += scheduledTasksCreated(idx)
				totalScheduledTasksCanceledCount += scheduledTasksCanceled(idx)
				idx += 1
			}

			val actualActiveSchedulesPerDoer = (totalScheduledTasksCreatedCount - completedScheduledTasksCount - totalScheduledTasksCanceledCount).toDouble / doerCount.toDouble
			val actualCanceledSchedulesFraction = if totalScheduledTasksCreatedCount > 0 then totalScheduledTasksCanceledCount.toDouble / totalScheduledTasksCreatedCount.toDouble else 0.0
			val actualScheduledVsRegularTaskRatio = if completedNormalTasksCount > 0 then totalScheduledTasksCreatedCount.toDouble / completedNormalTasksCount.toDouble else Double.PositiveInfinity

			val totalOps = completedScheduledTasksCount + totalScheduledTasksCanceledCount + completedNormalTasksCount
			val throughput = totalOps.toDouble / (durationMs.toDouble / 1000.0)

			ExperimentRunResult(
				throughput = throughput,
				actualActiveSchedulesPerDoer = actualActiveSchedulesPerDoer,
				actualCanceledSchedulesFraction = actualCanceledSchedulesFraction,
				actualScheduledVsRegularTaskRatio = actualScheduledVsRegularTaskRatio,
				actualCpuUtilization = utilization
			)
		}

		private def doStep(
			tokenId: Int,
			scheduledTasksCreatedCount: Long,
			normalTaskCreatedCount: Long,
			scheduledTasksCanceledCount: Long
		): Unit = {
			val initialCreatedCount = scheduledTasksCreatedCount + 1

			// 1. Do cancellations first (eliminates lag)
			val completedScheduled = scheduledTasksCreatedCount - scheduledTasksCanceledCount
			val requiredCanceled = (scenario.targetCanceledSchedulesFraction * completedScheduled / (1.0 - scenario.targetCanceledSchedulesFraction)).round
			val toCancel = requiredCanceled - scheduledTasksCanceledCount
			val nextCanceled = scheduledTasksCanceledCount + toCancel
			val nextCreated = initialCreatedCount + toCancel

			// 2. Submit normal tasks second
			val requiredNormals = (nextCreated / scenario.targetScheduledVsRegularTasksRatio).round
			val toSubmit = requiredNormals - normalTaskCreatedCount
			val nextNormals = normalTaskCreatedCount + toSubmit

			var loop = 0
			while loop < toSubmit do {
				val randomDoer = doers(ThreadLocalRandom.current().nextInt(doerCount))
				randomDoer.executeSequentially(() => completedNormalTasks.incrementAndGet())
				loop += 1
			}

			loop = 0
			while loop < toCancel do {
				val randomDoer = doers(ThreadLocalRandom.current().nextInt(doerCount))
				val toCancelSchedule = randomDoer.newDelaySchedule(10000)
				randomDoer.scheduleSequentially(toCancelSchedule, _ => ())
				randomDoer.cancel(toCancelSchedule)
				loop += 1
			}

			// Store counts in flat arrays for final metric collection
			scheduledTasksCreated(tokenId) = nextCreated
			scheduledTasksCanceled(tokenId) = nextCanceled
			normalTasksCreated(tokenId) = nextNormals

			scheduleNextStep(tokenId, nextCreated, nextNormals, nextCanceled)
		}

		private def scheduleNextStep(
			tokenId: Int,
			scheduledTasksCreatedCount: Long,
			normalTaskCreatedCount: Long,
			scheduledTasksCanceledCount: Long
		): Unit = {
			val randomDoer = doers(ThreadLocalRandom.current().nextInt(doerCount))
			val schedule = randomDoer.newDelaySchedule(timerDelayMs)
			randomDoer.scheduleSequentially(
				schedule,
				_ => {
					if running then {
						completedScheduledTasks.incrementAndGet()
						doStep(tokenId, scheduledTasksCreatedCount, normalTaskCreatedCount, scheduledTasksCanceledCount)
					}
				}
			)
		}
	}
}