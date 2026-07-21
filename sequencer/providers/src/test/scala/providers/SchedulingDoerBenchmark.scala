package readren.sequencer
package providers

import readren.sequencer.*
import providers.*

import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, ThreadLocalRandom, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.collection.mutable

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
 * @param throughput Measured execution throughput (ops/sec).
 * @param cpuUtilization Measured CPU utilization (fraction).
 * @param doerCount Total number of doers provisioned during execution.
 * @param targetActiveSchedules The targeted number of active schedules. */
case class CachedSettings(
	calibratedFrequency: Double,
	throughput: Double,
	cpuUtilization: Double,
	doerCount: Int,
	targetActiveSchedules: Int
)

/** Holds the final calibration and metric results of an experiment. */
case class CalibrationResult(
	experimentResult: ExperimentRunResult,
	retries: Int,
	calibratedFrequency: Double
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
	type SequencerProvider = DoerProvider[Sequencer] {type Tag = String} & CooperativeWorkersDp

	private val INITIAL_TARGET_ACTIVE_SCHEDULES = 25_000
	private val ENABLE_SLEEP_TRACKING = true
	private val TARGET_EXPERIMENT_DURATION = 2_000 // milliseconds
	private val MAX_JITTER_MS = 16.0
	private val POOL_SIZE = 8
	private inline val CACHE_FILE_NAME = "selection_benchmark_cache.txt"
	private inline val MINIMUM_STEPS = 4

	private val activeSchedulesPerDoerTargets = List(0.1, 1.0, 10.0)
	private val cancelledSchedulesFractionTargets = List(0.0, 0.1, 0.5, 0.9)
	private val scheduledVsRegularTasksRatioTargets = List(0.1, 1.0, 10.0)
	private val providerNames = List("Local", "Contained", "Sharded", "Flat", "Hierarchical", "ThreadDriven")

	def main(args: Array[String]): Unit = {
		println("======================================================================")
		println("STARTING PROVIDER SELECTION BENCHMARK (REFACTORED)")
		println("======================================================================")
		println(s"Initial target active schedules in queue: $INITIAL_TARGET_ACTIVE_SCHEDULES")
		println("======================================================================")

		val onlyTable = args.contains("--only-table") || args.contains("table")
		val cachePath = Paths.get(CACHE_FILE_NAME)
		val cache = mutable.Map[ProviderScenario, CachedSettings]()

		loadCache(cachePath, cache)

		def createProvider(providerName: String): SequencerProvider = {
			providerName match {
				case "Flat" => new CooperativeFlatPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Hierarchical" => new CooperativeHierarchicalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "ThreadDriven" => new CooperativeThreadDrivenSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Sharded" => new CooperativeShardedPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Local" => new CooperativeLocalPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case "Contained" => new CooperativeContainedPollingSchedulerDp.Impl(applyMemoryFence = false, threadPoolSize = POOL_SIZE, trackSleepTime = ENABLE_SLEEP_TRACKING)
				case _ => throw new IllegalArgumentException(s"Unknown provider: $providerName")
			}
		}

		val results = mutable.Map[ProviderScenario, Double]()
		val sleepMap = mutable.Map[ProviderScenario, Double]()
		val doerCounts = scala.collection.mutable.Map[Scenario, Int]()

		if onlyTable then drawTable(cache)
		else {
			println("Performing warm-up...")
			for (providerName <- providerNames) {
				val doerProvider = createProvider(providerName)
				try {
					val warmUpExperiment = new Experiment(
						doerProvider = doerProvider,
						numberOfDoers = 1000,
						scenario = Scenario(
							targetActiveSchedulesPerDoer = 1.0,
							targetCanceledSchedulesFraction = 0.1,
							targetScheduledVsRegularTasksRatio = 1.0
						),
						feedingPeriodMillis = 50,
						experimentDurationMillis = 300
					)
					warmUpExperiment.run()
				} finally {
					doerProvider.shutdown()
					doerProvider.awaitTermination(1, TimeUnit.SECONDS)
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
				var targetActiveSchedules = cache.find(x => x._1.scenario == scenario).fold(INITIAL_TARGET_ACTIVE_SCHEDULES)(_._2.targetActiveSchedules)
				var completedConfig = false

				while !completedConfig do {
					val numberOfDoers = (targetActiveSchedules / scenario.targetActiveSchedulesPerDoer).round.toInt.max(1)
					println(s"\nBenchmarking configuration: active schedules/doer=${scenario.targetActiveSchedulesPerDoer} | canceledSchedules/total=${scenario.targetCanceledSchedulesFraction * 100}% | scheduled/immediate=${scenario.targetScheduledVsRegularTasksRatio} | doers=$numberOfDoers | queueSize=$targetActiveSchedules")
					var needRedo = false
					val tempResults = scala.collection.mutable.Map[String, CalibrationResult]()

					// Run a pilot of all providers at 1ms delay to find their relative speeds under load
					val pilotThroughputs = scala.collection.mutable.Map[String, Double]()
					for (providerName <- providerNames) {
						val doerProvider = createProvider(providerName)
						try {
							val pilotExperiment = new Experiment(
								doerProvider = doerProvider,
								numberOfDoers = numberOfDoers,
								scenario = scenario,
								feedingPeriodMillis = 1,
								experimentDurationMillis = 200
							)
							val runResult = pilotExperiment.run()
							pilotThroughputs(providerName) = runResult.throughput
						} finally {
							doerProvider.shutdown()
							doerProvider.awaitTermination(9, TimeUnit.SECONDS)
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
						val calibrationResult = try calibrateAndRunExperiment(
							providerName = providerName,
							doerProvider = doerProvider,
							numberOfDoers = numberOfDoers,
							scenario = scenario,
							cachedSettings = cachedSettings
						)
						finally {
							doerProvider.shutdown()
							doerProvider.awaitTermination(9, TimeUnit.SECONDS)
						}
						if calibrationResult eq null then {
							targetActiveSchedules *= 2
							needRedo = true
							println(s"\n[WARNING] $providerName failed to saturate CPU. Doubling targetActiveSchedules to $targetActiveSchedules and restarting configuration...")
						} else {
							tempResults(providerName) = calibrationResult
							providerIndex += 1
						}
					}

					if !needRedo then {
						completedConfig = true
						doerCounts(scenario) = numberOfDoers
						for (providerName <- orderedProviders) {
							val calibrationResult = tempResults(providerName)
							val key = ProviderScenario(scenario, providerName)
							results(key) = calibrationResult.experimentResult.throughput
							sleepMap(key) = 1.0 - calibrationResult.experimentResult.actualCpuUtilization
							cache(key) = CachedSettings(
								calibratedFrequency = calibrationResult.calibratedFrequency,
								throughput = calibrationResult.experimentResult.throughput,
								cpuUtilization = calibrationResult.experimentResult.actualCpuUtilization,
								doerCount = numberOfDoers,
								targetActiveSchedules = targetActiveSchedules
							)

							val sleepPct = (1.0 - calibrationResult.experimentResult.actualCpuUtilization) * 100.0
							printf("  %-7s -> Calibrated Freq=%-5.4f Hz | active schecules/doer=%5.2f, canceledSchedules=%5.1f%%, scheduled/regular=%5.2f | Sleep=%5.1f%% | Retries=%d | Throughput=%s\n",
								providerName, calibrationResult.calibratedFrequency, calibrationResult.experimentResult.actualActiveSchedulesPerDoer, calibrationResult.experimentResult.actualCanceledSchedulesFraction * 100, calibrationResult.experimentResult.actualScheduledVsRegularTaskRatio, sleepPct, calibrationResult.retries, formatThroughput(calibrationResult.experimentResult.throughput))
						}

						saveCache(cache, cachePath, onlyTable)
					}
				}
			}

			saveCache(cache, cachePath, onlyTable)
			println(s"""\nCalibrated settings saved to "$CACHE_FILE_NAME".""")
			drawTable(cache)
		}
	}

	def drawTable(cache: mutable.Map[ProviderScenario, CachedSettings]): Unit = {
		val separator = "=" * 125
		println("\n" + separator)
		println(" " * 35 + "FLATTENED MULTIDIMENSIONAL SELECTION SUMMARY")
		println(separator)

		val results = mutable.Map[ProviderScenario, Double]()
		val sleepMap = mutable.Map[ProviderScenario, Double]()
		val doerCounts = scala.collection.mutable.Map[Scenario, Int]()

		println("Running in Table-Only mode. Drawing results from cache file...")
		for ((ProviderScenario(scenario, providerName), cachedSettings) <- cache) {
			results(ProviderScenario(scenario, providerName)) = cachedSettings.throughput
			sleepMap(ProviderScenario(scenario, providerName)) = 1.0 - cachedSettings.cpuUtilization
			doerCounts(scenario) = cachedSettings.doerCount
		}

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
					val doerCount = doerCounts(scenario)
					headerArgs.append(f"Sched/Regul= $targetScheduledVsRegularTasksRatio (${doerCount / 1000.0}%4.1fk doers)")
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
				writer.write("# activeSchedulesPerDoer, canceledSchedulesFraction, scheduledVsRegularTasksRatio, ProviderName, Freq(Hz), throughput, utilization, doers, targetActiveSchedules\n")
				val sortedCache = cache.toSeq.sortBy { case (providerScenario, cachedSettings) =>
					(
						providerScenario.scenario.targetActiveSchedulesPerDoer,
						providerScenario.scenario.targetCanceledSchedulesFraction,
						providerScenario.scenario.targetScheduledVsRegularTasksRatio,
						providerScenario.providerName
					)
				}
				for ((providerScenario, cachedSettings) <- sortedCache) {
					writer.write(s"${providerScenario.scenario.targetActiveSchedulesPerDoer},${providerScenario.scenario.targetCanceledSchedulesFraction},${providerScenario.scenario.targetScheduledVsRegularTasksRatio},${providerScenario.providerName},${cachedSettings.calibratedFrequency},${cachedSettings.throughput},${cachedSettings.cpuUtilization},${cachedSettings.doerCount},${cachedSettings.targetActiveSchedules}\n")
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
				if parts.length >= 8 then {
					val targetActiveSchedulesPerDoer = parts(0).toDouble
					val targetCanceledSchedulesFraction = parts(1).toDouble
					val targetScheduledVsRegularTasksRatio = parts(2).toDouble
					val providerName = parts(3)
					val calibratedFrequency = parts(4).toDouble
					val throughput = parts(5).toDouble
					val cpuUtilization = parts(6).toDouble
					val doerCount = parts(7).toInt
					val targetActiveSchedules = if parts.size > 8 then parts(8).toInt else (doerCount * targetActiveSchedulesPerDoer).round.toInt

					val key = ProviderScenario(Scenario(targetActiveSchedulesPerDoer, targetCanceledSchedulesFraction, targetScheduledVsRegularTasksRatio), providerName)
					cache(key) = CachedSettings(calibratedFrequency, throughput, cpuUtilization, doerCount, targetActiveSchedules)
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
	): CalibrationResult | Null = {
		val targetActiveSchedulesPerDoer = scenario.targetActiveSchedulesPerDoer
		val targetCanceledSchedulesFraction = scenario.targetCanceledSchedulesFraction
		val targetScheduledVsRegularTasksRatio = scenario.targetScheduledVsRegularTasksRatio

		inline val FEEDING_PERIOD_MINIMUM_LOWER_BOUND = 1.0
		inline val FEEDING_PERIOD_MAXIMUM_UPPER_BOUND = 10_000.0
		var feedingPeriodMillisPilotLowerBound: Double = FEEDING_PERIOD_MINIMUM_LOWER_BOUND
		var feedingPeriodMillisPilotUpperBound: Double = FEEDING_PERIOD_MAXIMUM_UPPER_BOUND
		var feedingPeriodMillisFinalLowerBound: Double = FEEDING_PERIOD_MINIMUM_LOWER_BOUND
		var feedingPeriodMillisFinalUpperBound: Double = FEEDING_PERIOD_MAXIMUM_UPPER_BOUND
		var experimentResultAtFeedingPeriodMillisLowerBound: ExperimentRunResult | Null = null

		inline val MILLIS_PER_SECOND = 1000

		/** gives the geometric mean of feedingPeriodMillisUpperBound and feedingPeriodMillisLowerBound */
		def nextPilotFeedingPeriodMillis: Double = MILLIS_PER_SECOND / math.sqrt(MILLIS_PER_SECOND * MILLIS_PER_SECOND / (feedingPeriodMillisPilotLowerBound * feedingPeriodMillisPilotUpperBound))

		def nextFinalFeedingPeriodMillis: Double = MILLIS_PER_SECOND / math.sqrt(MILLIS_PER_SECOND * MILLIS_PER_SECOND / (feedingPeriodMillisFinalLowerBound * feedingPeriodMillisFinalUpperBound))

		var feedingPeriodMillis: Double = cachedSettings.fold(nextPilotFeedingPeriodMillis) { settings =>
			(MILLIS_PER_SECOND * numberOfDoers).toDouble / (settings.doerCount * settings.calibratedFrequency)
		}

		var experimentDuration = (feedingPeriodMillis * 3.5).round.max(TARGET_EXPERIMENT_DURATION / 7)
		var attemptNumber = 0
		var calibrationResult: CalibrationResult | Null = null
		while calibrationResult eq null do {
			attemptNumber += 1
			val experiment = new Experiment(doerProvider, numberOfDoers, scenario, feedingPeriodMillis, experimentDuration)
			val runResult = experiment.run()
			val feedingFreq = MILLIS_PER_SECOND.toDouble / feedingPeriodMillis
			println(f"  $providerName%-15s ... Calibrating (S=${runResult.actualActiveSchedulesPerDoer}%5.2f, C=${runResult.actualCanceledSchedulesFraction}%5.2f, R=${runResult.actualScheduledVsRegularTaskRatio}%5.2f, CPU=${runResult.actualCpuUtilization * 100}%4.0f%%) | Freq=$feedingFreq%6.3f Hz | Attempt=$attemptNumber%-2d")

			val cpuSaturated = runResult.actualCpuUtilization >= 0.98
			if cpuSaturated then {
				if experimentDuration >= TARGET_EXPERIMENT_DURATION then {
					feedingPeriodMillisFinalLowerBound = feedingPeriodMillis
					experimentResultAtFeedingPeriodMillisLowerBound = runResult
				} else feedingPeriodMillisPilotLowerBound = feedingPeriodMillis
			} else {
				if experimentDuration >= TARGET_EXPERIMENT_DURATION then feedingPeriodMillisFinalUpperBound = feedingPeriodMillis else feedingPeriodMillisPilotUpperBound = feedingPeriodMillis
			}

			if experimentDuration >= TARGET_EXPERIMENT_DURATION then {
				if feedingPeriodMillisFinalUpperBound - feedingPeriodMillisFinalLowerBound <= feedingPeriodMillisFinalLowerBound / 1000 then {
					if experimentResultAtFeedingPeriodMillisLowerBound ne null then {
						calibrationResult = CalibrationResult(
							experimentResult = experimentResultAtFeedingPeriodMillisLowerBound,
							retries = attemptNumber - 1,
							calibratedFrequency = MILLIS_PER_SECOND.toDouble / feedingPeriodMillisFinalLowerBound
						)
					} else if feedingPeriodMillisFinalLowerBound - FEEDING_PERIOD_MINIMUM_LOWER_BOUND > 0.05 then {
						feedingPeriodMillisFinalLowerBound = (feedingPeriodMillisFinalLowerBound - 0.05).max(FEEDING_PERIOD_MINIMUM_LOWER_BOUND)
						feedingPeriodMillis = feedingPeriodMillisFinalLowerBound
					} else return null
				} else feedingPeriodMillis = nextFinalFeedingPeriodMillis
			} else {
				experimentDuration =
					if cpuSaturated then TARGET_EXPERIMENT_DURATION.max(feedingPeriodMillis.round.toInt * MINIMUM_STEPS)
					else {
						val cpuUtilizationError = math.max(0.0, 0.98 - runResult.actualCpuUtilization)
						val dampingScaleFactor = 1.0 / (1.0 + 4.0 * cpuUtilizationError)
						math.max(TARGET_EXPERIMENT_DURATION / 10, (TARGET_EXPERIMENT_DURATION * dampingScaleFactor).round.toInt)
					}
				feedingPeriodMillis =
					if experimentDuration >= TARGET_EXPERIMENT_DURATION then {
						feedingPeriodMillisFinalLowerBound = 1.0.max(feedingPeriodMillisPilotLowerBound - feedingPeriodMillisPilotLowerBound / 5.0)
						feedingPeriodMillisFinalUpperBound = feedingPeriodMillisPilotUpperBound + feedingPeriodMillisPilotUpperBound / 4.0
						nextFinalFeedingPeriodMillis
					} else if feedingPeriodMillis - FEEDING_PERIOD_MINIMUM_LOWER_BOUND > 0.05 then nextPilotFeedingPeriodMillis
					else return null
			}
		}
		calibrationResult
	}

	/** Represents a single isolated benchmark execution run.
	 *
	 * Owns the lifecycle of the doers, tokens, and verification parameters.
	 *
	 * @param doerProvider The DoerProvider instance.
	 * @param numberOfDoers Total number of doers to provision.
	 * @param scenario The execution target parameters (density, cancel fraction, relation ratio).
	 * @param feedingPeriodMillis Delay in milliseconds for scheduled timer tasks.
	 * @param experimentDurationMillis Run duration in milliseconds. */
	private class Experiment(
		private val doerProvider: SequencerProvider,
		private val numberOfDoers: Int,
		private val scenario: Scenario,
		private val feedingPeriodMillis: Double,
		private val experimentDurationMillis: Long
	) {
		private val doers: Array[SchedulingDoerBenchmark.Sequencer] = Array.tabulate(numberOfDoers)(i => doerProvider.provide(s"sel-doer-$i"))
		// Global accumulators for final metrics
		private val globalSchedulesCreated = new AtomicLong(0)
		private val globalRegularsCreated = new AtomicLong(0)
		private val globalSchedulesCanceled = new AtomicLong(0)
		private val globalSchedulesCompleted = new AtomicLong(0)
		private val globalRegularsCompleted = new AtomicLong(0)

		private val activeSchedulesTarget = (numberOfDoers * scenario.targetActiveSchedulesPerDoer).round.toInt.max(1)
		private val completedTokens = new CountDownLatch(activeSchedulesTarget)
		@volatile private var experimentTimerElapsed = false

		/** Executes a benchmark run and returns the result upon completion.
		 *
		 * Goal: Minimize distortion by reducing benchmark overhead, especially thread contention.
		 *
		 * How: Subdivide the generated load into independent tokens, each of which guarantees its portion of the load satisfies the scenario parameters.
		 *
		 * Execution: The main thread initiates the experiment by pre-populating the queue with the target active schedules. It then sleeps for the configured duration before setting the `@volatile` flag `experimentTimerElapsed` to `true`. Worker threads in the cooperative pool (and the background scheduler thread under the `ThreadDriven` scheduler) execute the token steps. When a token step detects `experimentTimerElapsed` is `true`, it halts further scheduling, adds its local progress counters to the global atomic variables, and counts down the `completedTokens` [[java.util.concurrent.CountDownLatch]]. Once all tokens have terminated, the main thread drains any remaining enqueued normal tasks and compiles the final [[ExperimentRunResult]].
		 * @return The metrics of the completed experiment run. */
		def run(): ExperimentRunResult = {
			val prepopulationLatch = new CountDownLatch(activeSchedulesTarget)

			val feedingPeriodMillisFloor = feedingPeriodMillis.floor.toInt
			val feedingPeriodMillisCeil = feedingPeriodMillisFloor + 1
			// Tokens are partitioned into two sets: the ones whose feeding period is the floor of `feedingPeriodMillis` and the ones whose period is the ceiling of `feedingPeriodMillis`.
			// The following math is to determine how many in each set in order to have equivalent throughput as if all tokens have `feedingPeriodMillis`.
			val numberOfFloorTokens = {
				// Given:
				//   load(period) = K/period
				//   n <= period <= n + 1
				//   load(period) = a*load(n) + b*load(n+1)
				// Then:
				//   a = n*(n + 1 - period) / period
				//   b = (n + 1)(period - n) / period
				val a = feedingPeriodMillisFloor * (feedingPeriodMillisCeil - feedingPeriodMillis) / feedingPeriodMillis
				val b = feedingPeriodMillisCeil * (feedingPeriodMillis - feedingPeriodMillisFloor) / feedingPeriodMillis
				// Given:
				//   numberOfFloorTokens + numberOfCeilTokens = totalNumberOfTokens
				//   numberOfFloorToken/a = numberOfCeilTokens/b
				// Then:
				//   numberOfFloorTokens = a * totalNumberOfTokens / (a + b)
				(a * activeSchedulesTarget / (a + b)).round.toInt
			}


			// Pre-populate schedules to start token timer loops
			for (i <- 0 until activeSchedulesTarget) {
				val doer = doers(i % numberOfDoers)

				doer.executeSequentially(() => {
					val token = new Token(i, doer, if i < numberOfFloorTokens then feedingPeriodMillisFloor else feedingPeriodMillisCeil)
					token.start()
					prepopulationLatch.countDown()
				})
			}

			// 1. Wait until all tokens are pre-populated and running
			prepopulationLatch.await()
			// 2. Trigger GC from the main thread after all setup objects exist and prepopulation closures have completed
			System.gc()
			// 3. Sleep 100ms to allow the GC cycle to fully complete and JVM threads to settle
			Thread.sleep(100)
			// 4. Capture start timestamp ONLY after GC is finished
			val nanoTimeAtStart = System.nanoTime()
			val workersSleepTimeAtStart = if ENABLE_SLEEP_TRACKING then doerProvider.workersSleepTimeNanos else Array.empty[Long]
			val schedulingThreadSleepTimeAtStart = if ENABLE_SLEEP_TRACKING then {
				doerProvider match {
					case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
					case _ => -1L
				}
			} else -1L
			// 5. Schedule the finalization of the experiment.
			Thread.sleep(experimentDurationMillis)
			// 6. Set the flat that signals experiment time has elapsed.
			experimentTimerElapsed = true
			// 7. Measure the exact time the experiment lasted, and how much the workers were sleeping during the experiment.
			val nanoTimeAtEnd = System.nanoTime()
			val workersSleepTimeAtEnd = if ENABLE_SLEEP_TRACKING then doerProvider.workersSleepTimeNanos else Array.empty[Long]
			val schedulingThreadSleepTimeAtEnd = if ENABLE_SLEEP_TRACKING then {
				doerProvider match {
					case t: CooperativeThreadDrivenSchedulerDp => t.schedulerSleepTimeNanos
					case _ => -1L
				}
			} else -1L
			completedTokens.await()

			val totalNormalTasksCreatedCount = globalRegularsCreated.get()

			// Drain phase: wait until all enqueued normal tasks have been completed
			val drainStart = System.currentTimeMillis()
			while doerProvider.currentLoad > 0 do {
				Thread.sleep(1)
				val drainDuration = System.currentTimeMillis() - drainStart
				if (drainDuration % 1000) == 0 then println(s"${doerProvider.getClass.getSimpleName} is shutting down. ${drainDuration}ms have elapsed.\n${doerProvider.diagnose(new StringBuilder)}")
			}

			val totalDurationNanos = nanoTimeAtEnd - nanoTimeAtStart
			val utilization = if SchedulingDoerBenchmark.ENABLE_SLEEP_TRACKING then {
				val poolSize = workersSleepTimeAtStart.length
				var totalSleepNanos = 0L
				var idx = 0
				while idx < poolSize do {
					totalSleepNanos += (workersSleepTimeAtEnd(idx) - workersSleepTimeAtStart(idx))
					idx += 1
				}

				val startupDelayNanos = (feedingPeriodMillis * 1000000.0).toLong
				val activeDurationNanos = totalDurationNanos - startupDelayNanos

				val jitterMarginNanos = math.max(0.0, SchedulingDoerBenchmark.MAX_JITTER_MS * (1.0 - experimentDurationMillis.toDouble / SchedulingDoerBenchmark.TARGET_EXPERIMENT_DURATION.toDouble)) * 1000000.0

				val workersUtilization = if activeDurationNanos <= 0 then 0.0 else {
					val inevitableSleep = poolSize * startupDelayNanos + jitterMarginNanos
					val activeSleep = math.max(0L, totalSleepNanos - inevitableSleep)
					1.0 - (activeSleep / (activeDurationNanos.toDouble * poolSize))
				}

				val schedulerUtilization = if schedulingThreadSleepTimeAtStart >= 0L && activeDurationNanos > 0 then {
					val schedSleep = schedulingThreadSleepTimeAtEnd - schedulingThreadSleepTimeAtStart
					val inevitableSleep = startupDelayNanos + jitterMarginNanos.toLong
					val activeSleep = math.max(0L, schedSleep - inevitableSleep)
					1.0 - (activeSleep.toDouble / activeDurationNanos.toDouble)
				} else 0.0

				math.max(workersUtilization, schedulerUtilization)
			} else {
				1.0
			}

			val completedScheduledTasksCount = globalSchedulesCompleted.get()
			val completedNormalTasksCount = globalRegularsCompleted.get()
			val totalScheduledTasksCreatedCount = globalSchedulesCreated.get()
			val totalScheduledTasksCanceledCount = globalSchedulesCanceled.get()

			val actualActiveSchedulesPerDoer = (totalScheduledTasksCreatedCount - completedScheduledTasksCount - totalScheduledTasksCanceledCount).toDouble / numberOfDoers.toDouble
			val actualCanceledSchedulesFraction = if totalScheduledTasksCreatedCount > 0 then totalScheduledTasksCanceledCount.toDouble / totalScheduledTasksCreatedCount.toDouble else 0.0
			val actualScheduledVsRegularTaskRatio = if completedNormalTasksCount > 0 then totalScheduledTasksCreatedCount.toDouble / completedNormalTasksCount.toDouble else Double.PositiveInfinity

			val totalOps = completedScheduledTasksCount + totalScheduledTasksCanceledCount + completedNormalTasksCount
			val throughput = totalOps.toDouble / (experimentDurationMillis.toDouble / 1000.0)

			ExperimentRunResult(
				throughput = throughput,
				actualActiveSchedulesPerDoer = actualActiveSchedulesPerDoer,
				actualCanceledSchedulesFraction = actualCanceledSchedulesFraction,
				actualScheduledVsRegularTaskRatio = actualScheduledVsRegularTaskRatio,
				actualCpuUtilization = utilization
			)
		}

		private class Token(index: Int, assignedDoer: Sequencer, tokenFeedingPeriodMillis: Int) {
			private val regularCompletedCount: AtomicLong = AtomicLong(0)
			private var lastUsedDelegateDoerIndex: Int = index

			def start(): Unit = doStep(0, 0, 0, 0)

			private def doStep(
				schedulesCreatedCount0: Long,
				regularsCreatedCount0: Long,
				schedulesCanceledCount0: Long,
				schedulesCompletedCount0: Long
			): Unit = {
				if experimentTimerElapsed then {
					globalSchedulesCreated.addAndGet(schedulesCreatedCount0)
					globalRegularsCreated.addAndGet(regularsCreatedCount0)
					globalSchedulesCanceled.addAndGet(schedulesCanceledCount0)
					globalSchedulesCompleted.addAndGet(schedulesCompletedCount0 - 1)
					globalRegularsCompleted.addAndGet(regularCompletedCount.get)
					completedTokens.countDown()
				} else {
					// 1. Determine now many of each tasks to create in this step
					// Given:
					// targetCanceledSchedulesFraction == schedulesCanceledCount1/schedulesCreatedCount1
					// schedulesCreatedCount1 == schedulesCreatedCount0 + 1 + schedulesCreatedAndCanceledInThisStep
					// schedulesCanceledCount1 == schedulesCanceledCount0 + schedulesCreatedAndCanceledInThisStep
					// targetScheduledVsRegularRatio == schedulesCreatedCount1/regularsCreatedCount1
					// regularsCreatedCount1 == regularsCreatedCount0 + regularsCreatedInThisStep
					// Then:
					val schedulesCreatedCount1 = ((schedulesCreatedCount0 - schedulesCanceledCount0 + 1) / (1 - scenario.targetCanceledSchedulesFraction)).round
					val schedulesCreatedAndCanceledInThisStep = schedulesCreatedCount1 - schedulesCreatedCount0 - 1
					val schedulesCanceledCount1 = schedulesCanceledCount0 + schedulesCreatedAndCanceledInThisStep
					val regularsCreatedCount1 = (schedulesCreatedCount1 / scenario.targetScheduledVsRegularTasksRatio).round

					// 1. Schedule and immediately cancel the required portion to cancel
					var remaining = schedulesCreatedAndCanceledInThisStep
					while remaining > 0 do {
						remaining -= 1
						val randomDoer = nextDelegateDoer()
						val toCancelSchedule = randomDoer.newDelaySchedule(1000)
						randomDoer.schedule(toCancelSchedule)(_ => ())
						randomDoer.cancel(toCancelSchedule)
					}

					// 2. Submit normal tasks second
					remaining = regularsCreatedCount1 - regularsCreatedCount0
					while remaining > 0 do {
						remaining -= 1
						val randomDoer = nextDelegateDoer()
						randomDoer.executeSequentially { () =>
							if !experimentTimerElapsed then {
								regularCompletedCount.incrementAndGet()
							}
						}
					}

					// 3. Schedule the single non-canceled scheduled task that, which also propagates this token.
					assignedDoer.schedule(assignedDoer.newDelaySchedule(tokenFeedingPeriodMillis)) { _ =>
						doStep(schedulesCreatedCount1, regularsCreatedCount1, schedulesCanceledCount1, schedulesCompletedCount0 + 1)
					}
				}
			}

			private def nextDelegateDoer(): Sequencer = {
				var nextIndex = lastUsedDelegateDoerIndex + 1
				if nextIndex >= numberOfDoers then nextIndex = 0
				lastUsedDelegateDoerIndex = nextIndex
				doers(nextIndex)
			}
		}
	}
}