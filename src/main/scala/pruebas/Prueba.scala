package readren.matrix
package pruebas

import rf.{RegularRf, SynchronousMsgBufferRf}

import readren.taskflow.Doer

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.StdIn

object Prueba {

	class MatrixAide extends Matrix.Aide { thisMatrixAide =>
		override def reportFailure(cause: Throwable): Unit = cause.printStackTrace()

		private val executors: ArrayBuffer[ExecutorService] = ArrayBuffer.empty

		override def buildDoerAssistantForAdmin(adminId: Int): Doer.Assistant = new Doer.Assistant {

			private val doSiThEx = {
				val newExecutor = Executors.newSingleThreadExecutor()
				executors.addOne(newExecutor)
				newExecutor
			}

			override def queueForSequentialExecution(runnable: Runnable): Unit = doSiThEx.execute(runnable)

			override def reportFailure(cause: Throwable): Unit = thisMatrixAide.reportFailure(cause)
		}

		def shutdown(): CompletableFuture[Void] = {
			CompletableFuture.runAsync(
				() => executors.foreach(_.shutdown()),
				CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS)
				)
		}
	}


	sealed trait Answer

	case class Response(adminId: Int, childSerial: Int, text: String) extends Answer

	case object End extends Answer

	sealed trait Cmd

	case class ChildWasStopped(childIndex: Int) extends Cmd, Answer

	case class Spawn(childEndPointReceiver: Endpoint[Int] => Unit, replyTo: Endpoint[Response]) extends Cmd
	//	case class Send(text: String)

	case class SpawnResponse(endpoint: Endpoint[String])

	val numberOfChildren = 1000
	val numberOfMessagesPerChild = 1000

	@main def runPrueba(): Unit = {
		given ExecutionContext = ExecutionContext.global

		val totalFuture =
			for {
				r1 <- runPrueba(RegularRf)
				r2 <- runPrueba(SynchronousMsgBufferRf)
				r3 <- runPrueba(RegularRf)
				r4 <- runPrueba(SynchronousMsgBufferRf)
				r5 <- runPrueba(RegularRf)
				r6 <- runPrueba(SynchronousMsgBufferRf)
				r7 <- runPrueba(RegularRf)
				r8 <- runPrueba(SynchronousMsgBufferRf)
				r9 <- runPrueba(RegularRf)
				r10 <- runPrueba(SynchronousMsgBufferRf)
				r11 <- runPrueba(RegularRf)
				r12 <- runPrueba(SynchronousMsgBufferRf)
			} yield ()
		totalFuture.andThen(x => println(s"All matrix were shutdown returning: $x\nPress <enter> to exit"))

		StdIn.readLine()
	}

	def runPrueba(reactantFactory: ReactantFactory): Future[Unit] = {
		val csb: StringBuffer = new StringBuffer(99999)

		val matrixAide = new MatrixAide
		val matrix = new Matrix("myMatrix", matrixAide)
		csb.append("Matrix created\n")


		val counter: AtomicInteger = new AtomicInteger(0)

		var printer: Future[Unit] = Future.successful(())
		val numberOfStringBuilders = numberOfChildren
		val sbs: Array[StringBuilder] = Array.fill(numberOfStringBuilders)(new StringBuilder(9999))

		// SynchronousMsgBufferRf
		val nanoAtStart = System.nanoTime()
		val outReceiver = new Receiver[Answer] {
			override def submit(answer: Answer): Unit = {
				answer match {
					case message: Response =>
						val counterValue = counter.incrementAndGet()
						sbs(message.childSerial).append(f"($counterValue%6d)-${message.text}; ")

						if false then {
							val lsb = new StringBuilder(99999)
							lsb.append("\n>>>>>>>>>>>>>")
							for i <- 1 until numberOfStringBuilders if sbs(i).nonEmpty do lsb.append(f"\n$i%3d: ${sbs(i).toString}")
							lsb.append("\n------------")
							printer = printer.andThen { _ => println(lsb.toString()) }(ExecutionContext.global)
						}

					case ChildWasStopped(childSerial) =>
						sbs(childSerial).append(f"(${counter.get()}%6d) <| Stopped; ")

					case End =>
						val nanoAtEnd = System.nanoTime()
						val lsb = new StringBuilder(999999)
						lsb.append(s"\n+++ Total number of non-negative numbers sent to children: ${counter.get()} +++\n")
						lsb.append(s"\n+++ Factory: ${reactantFactory.getClass.getSimpleName} +++ Duration: ${(nanoAtEnd - nanoAtStart) / 1000000} ms +++\n")
						if false then {
							for i <- 0 until numberOfStringBuilders do {
								lsb.append(f"$i%3d: ${sbs(i)}\n")
								sbs(i).setLength(0)
							}
						}
						println(lsb)
				}
			}

			override def uri: URI = ???
		}

		val result = Promise[Unit]

		val outEndpoint = LocalEndpoint(outReceiver)
		matrix.spawn[Cmd](reactantFactory) { parent =>
			parent.admin.checkWithin()
			val parentEndpointForChild = parent.endpointProvider.local[ChildWasStopped]
			var childrenCount = 0
			var childrenIndexSeq = 0
			Behavior.handleMsgAndSignal {

				case spawn@Spawn(childEndPointReceiver, replyTo) =>
					parent.admin.checkWithin()
					parent.spawn[Int](reactantFactory) { child =>
						val childIndex = childrenIndexSeq
						childrenIndexSeq += 1
						child.admin.checkWithin()
						Behavior.supervise(Behavior.ignore.withMsgBehavior { n =>
							child.admin.checkWithin()
							if n >= 0 then {
								replyTo.tell(Response(child.admin.id, childIndex, f"${child.serial}%3d <=$n%3d"))
								Continue
							} else {
								parentEndpointForChild.tell(ChildWasStopped(childIndex))
								Stop
							}
						})
					}.map { child =>
						parent.watch(child.serial)
						childrenCount += 1
						// println(s"Child ${child.serial} spawned. Active children: ${parent.children.size}")
						child.endpointProvider.local[Int]
					}.trigger(true)(childEndPointReceiver)
					Continue

				case cws@ChildWasStopped(childIndex) =>
					parent.admin.checkWithin()
					childrenCount -= 1
					outEndpoint.tell(cws)
					if childrenCount == 0 then {
						outEndpoint.tell(End)
						Stop
					} else {
						// println(s"Child $childIndex stopped. Active children: $childrenCount")
						Continue
					}

			}(Behavior.handleSignal { s =>
				parent.admin.checkWithin()
				// println(s"Parent has received the signal: $s")
				Continue
			})
		}.trigger() { parent =>
			val parentEndpoint = parent.endpointProvider.local[Spawn]
			csb.append("Parent started\n")
			val futures = for j <- 0 until numberOfChildren yield Future {
				csb.append(s"Future $j begin: ")
				parentEndpoint.tell(Spawn(
					childEndpoint => Future {
						for i <- 0 until numberOfMessagesPerChild do childEndpoint.tell(i)
						childEndpoint.tell(-1)
					}(ExecutionContext.global),
					outEndpoint
					))
				csb.append(s"Future $j end. ")
			}(ExecutionContext.global)
			if false then {
				Future.sequence(futures)(ArrayBuffer, ExecutionContext.global).onComplete { _ =>
					println(s"\nFutures completed: csb=[${csb.toString}]")
				}(ExecutionContext.global)
			}
			parent.stopDuty.trigger() { _ =>
				matrixAide.shutdown().thenRun { () =>
					println("Shutdown completed normally")
					result.success(())
				}
			}
		}
		result.future
	}
}
