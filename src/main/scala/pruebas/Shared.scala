package readren.matrix
package pruebas

import readren.taskflow.{Doer, Maybe}

import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, LinkedBlockingQueue, ScheduledExecutorService, ScheduledFuture, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable.ArrayBuffer

object Shared {

	class TestingAide(isMonitoringEnabled: Boolean = false, label: String = "") extends Matrix.Aide[DoerProvider] {
		override def buildDoerProvider(owner: Matrix[DoerProvider]): DoerProvider = new DoerProvider(owner, isMonitoringEnabled, label)
	}

	class DoerProvider(owner: Matrix[DoerProvider], isMonitoringEnabled: Boolean = false, label: String = "") extends Matrix.DoerProvider { thisDoerProvider =>
		
		override def pick(serial: Int): MatrixDoer = matrixDoers(serial % matrixDoers.length)

		class ExecutorInfo(val executor: ThreadPoolExecutor, var lastRunnable: Maybe[Runnable])

		private val executors: ArrayBuffer[ExecutorInfo] = ArrayBuffer.empty
		private var activityEvidence: Boolean = false
		private val monitoringSchedule = if isMonitoringEnabled then Maybe.some(startMonitoring()) else Maybe.empty
		private var oScheduler: Maybe[ScheduledExecutorService] = Maybe.empty

		private val matrixDoers: IArray[MatrixDoer] = {
			val availableProcessors = Runtime.getRuntime.availableProcessors()
			IArray.tabulate(availableProcessors) { index =>
				val doerId = index + 1
				new MatrixDoer(doerId, buildDoerAssistant(doerId), owner)
			}
		}
		
		private def buildDoerAssistant(doerId: Int): Doer.Assistant = new Doer.Assistant {

			private val doSiThExInfo = {

				val queue = new LinkedBlockingQueue[Runnable]()
				val newExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, queue)
				val executorInfo = ExecutorInfo(newExecutor, Maybe.empty)
				executors.addOne(executorInfo)
				executorInfo
			}

			override def queueForSequentialExecution(runnable: Runnable): Unit = {
				activityEvidence = true
				doSiThExInfo.lastRunnable = Maybe.some(runnable)
				doSiThExInfo.executor.execute(runnable)
			}

			override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()
		}

		def shutdown(): CompletableFuture[Void] = {
			monitoringSchedule.foreach(_.cancel(false))
			oScheduler.foreach { s =>
				s.shutdown()
				s.awaitTermination(1, TimeUnit.SECONDS)
			}
			CompletableFuture.runAsync(
				() => {
					executors.foreach { ei =>
						ei.executor.shutdown()
					}
					executors.foreach { ei =>
						ei.executor.awaitTermination(1, TimeUnit.SECONDS)
					}
				},
				CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
			).thenRunAsync(() => (), CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS))
		}

		private def startMonitoring(): ScheduledFuture[?] = {
			val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
			oScheduler = Maybe.some(scheduler)
			scheduler.scheduleWithFixedDelay(monitor, 2000, 2000, TimeUnit.MILLISECONDS)
		}

		private object monitor extends Runnable {
			override def run(): Unit = {
				if activityEvidence then activityEvidence = false
				else {
					println(diagnose())
					extraMonitors.forEach(_.run())
				}
			}
		}

		def diagnose(): StringBuilder = {
			var totalCompletedTaskCount: Long = 0
			val sb = StringBuilder()
			sb.append(label).append('\n')
			for (info, i) <- executors.zipWithIndex do {
				sb.append(i).append(") ")
				sb.append(" queue.size=").append(info.executor.getQueue.size)
				sb.append(", activeCount=").append(info.executor.getActiveCount)
				sb.append(", taskCount=").append(info.executor.getTaskCount)
				sb.append(", completedTaskCount=").append(info.executor.getCompletedTaskCount)
				// sb.append(", largestPoolSize=").append(info.executor.getLargestPoolSize)
				sb.append(", isTerminating=").append(info.executor.isTerminating)
				sb.append(", isTerminated=").append(info.executor.isTerminated)
				sb.append(", isShutdown=").append(info.executor.isShutdown)
				sb.append('\n')
				// info.lastRunnable.foreach(r => sb.append("Last runnable:\n").append(r.toString).append('\n'))
				totalCompletedTaskCount += info.executor.getCompletedTaskCount
			}
			sb.append("totalCompletedTasks=").append(totalCompletedTaskCount)
			sb
		}

		private val extraMonitors = new java.util.concurrent.ConcurrentLinkedQueue[Runnable]()

		def addMonitor(monitor: Runnable): Unit = {
			extraMonitors.offer(monitor)
		}
	}
}
