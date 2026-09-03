package readren.sequencer

import CausalFence.{ROLLBACK_APPLIED, ROLLBACK_IGNORED, RollbackApplication}
import GeneratorsForDoerTests.*

import org.scalacheck.effect.PropF
import org.scalacheck.{Arbitrary, Gen}
import readren.common.Maybe

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Promise
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

trait CausalFenceTests[D <: Doer : ClassTag] { self: DoerProviderTestBase[D] =>

	test("CausalFence: multiple stepped advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (initial: Int, updater: Int => Capture[Int]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[Int, doer.type](doer)(initial)

			def loop(currentValue: Int, repetition: Int): Unit = {
				if repetition == 9 then promise.trySuccess(())
				else {
					updater(currentValue).triggerHardy(true) { expectedNextState =>
						val advanceCapture = fence.advance[Int] { previousValue =>
							if previousValue != currentValue then break(s"repetition #$repetition mismatch")
							updater(previousValue)
						}
						advanceCapture.triggerCallbacks(true)(
							actualNextSuccessfulState => {
								if expectedNextState.fold(_ => true, _ != actualNextSuccessfulState) then break(s"Expected: $expectedNextState, got: Success($actualNextSuccessfulState)")
								else loop(actualNextSuccessfulState, repetition + 1)
							},
							actualNextFaultyState => {
								if expectedNextState.fold(_ !=== actualNextFaultyState, _ => true) then break(s"Expected: $expectedNextState, got: Failure($actualNextFaultyState)")
								else promise.trySuccess(())
							}
						)
					}
				}
			}

			run(loop(initial, 0))
			gate
		}
	}

	test("CausalFence: multiple simultaneous advances should serialize and commit in order") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF(
			smallIntGen,
			Gen.function1[Int, Capture[Int]](genSuccessfulCapture[Int]())
		) { (initial, updater) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(initial)

				val actualSteps = for i <- 0 to 9 yield fence.advance(updater)

				def loop(previousState: Int, repetition: Int): Task[List[Int]] = {
					if repetition > 9 then Task_ready(Nil)
					else for {
						nextState <- updater(previousState)
						followingStates <- loop(nextState, repetition + 1)
					} yield nextState :: followingStates
				}

				val expectedResultsTask = loop(initial, 0)

				for {
					actualResults <- doer.Task_sequenceToArray(actualSteps)
					expectedResults <- expectedResultsTask
				} do {
					if actualResults.toList != expectedResults then break(s"expected:${expectedResults.mkString(", ")}, actual:${actualResults.mkString(", ")}")
					else promise.trySuccess(())
				}
			}
			gate
		}
	}

	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using hoping tasks to avoid stack overflow (much faster than the version that uses random delays below)") {
		val generators = getGenerators
		import generators.*

		def buildHopingMono(serial: Int, hops: Int): Mono[Int] = {
			if hops <= 0 then Task_ready(serial)
			else Captor[Int]().seizeWith(buildHopingMono(serial, hops - 1), false)
		}

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		PropF.forAllF(
			for {
				swarmSize <- Gen.choose(1, 9)
				hopsHead <- Gen.choose(0, 9)
				hopsTail <- Gen.listOfN(99, Gen.choose(0, 9))
			} yield (swarmSize, hopsHead, hopsTail)
		) { (swarmSize: Int, hopsHead: Int, hopsTail: List[Int]) =>
			val hopsList = hopsHead :: hopsTail
			val topSerial = hopsList.size

			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capture[PrimaryState] = {
				var hasAdvanced = false
				for {
					nextState <- {
						fence.advanceIf { (previous: PrimaryState) =>
							if previous.serial >= topSerial then Maybe.empty
							else {
								val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
								val mono = buildHopingMono(previous.serial + 1, hopsList(previous.serial))
									.map(newSerial => (pathId, newSerial))
									.andThen { nextState =>
										val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
										if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Mono it returns completes, no other updater is started; and that is not happening.")
									}
								hasAdvanced = true
								Maybe.some(mono)
							}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capture returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if hasAdvanced && nextState.pathId != pathId then break(s"A consumer subscribed to the Capture returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capture` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capture returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capture returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capture` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capture returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => doer.Capture_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	test("CausalFence - synchronous consumer ordering and anchor freshness: synchronous consumers see up‑to‑date state deterministically - using random delays (very slow)") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = (pathId: Int, serial: Int)
		val initialState: PrimaryState = (0, 0)
		val topSerial = 99
		PropF.forAllF(Gen.choose(1, 9), Gen.oneOf(true, false)) { (swarmSize: Int, syncOnly: Boolean) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)
			var derivedSerial: Int = 0

			def path(pathId: Int): Capture[PrimaryState] = {
				for {
					nextState <- {
						fence.advance { (previous: PrimaryState) =>
							val commitedAtStart: PrimaryState = fence.committedState.getOrElse(break(s"Unexpected failing state at updater start: ${fence.committedState}"))
							val monoGenerator: Gen[Mono[Int]] = genSuccessfulMonoFrom(previous.serial + 1, syncOnly)
							val randomMono: Mono[Int] = monoGenerator.sample.get
							randomMono.map(newSerial => (pathId, newSerial))
								.andThen { nextState =>
									val committedAtEnd = fence.committedState.getOrElse(break(s"Unexpected failing state at updater end: ${fence.committedState}"))
									if commitedAtStart.serial != committedAtEnd.serial then break(s"In the interval between the updater passed to `advance` is called and the Task it returns completes, no other updater is started; and that is not happening.")
								}
						}
					}
					anchoredState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capture returned by advanceIf yielded an unexpected failing state: ${fence.committedState}"))
						if nextState.pathId != pathId then break(s"A consumer subscribed to the Capture returned by `advance` should see the state to which the advance transitioned to; and is not happening: pathId=$pathId, actual: ${nextState.pathId}")
						else if derivedSerial > nextState.serial then break(s"Consumers subscribed immediately (in a synchronously coupled manner) to the `Capture` returned by `advance`, should be executed in order of subscription before any other consumer, even before the updaters passed to subsequent calls to advance; and is not happening.")
						else if nextState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the Capture returned by `advance` should see the up-to-date state; and is not happening: current=$nextState, commited=$committedState")
						else derivedSerial = nextState.serial
						fence.causalAnchor()
					}
					recursiveState <- {
						val committedState = fence.committedState.getOrElse(break(s"The Capture returned by causalAnchor yielded an unexpected failing state: ${fence.committedState}"))
						if anchoredState.serial != committedState.serial then break(s"A consumer subscribed immediately (in a synchronously coupled manner) to the `Capture` returned by `causalAnchor` should see the the up-to-date state; and is not happening: current=$anchoredState, commited=${fence.committedState}")
						if nextState.serial < topSerial then path(pathId)
						else fence.committed
					}
				} yield {
					val committedState = fence.committedState.getOrElse(break(s"The Capture returned by `committed` yielded an unexpected failing state: ${fence.committedState}"))
					if recursiveState.serial != committedState.serial then break(s"followingState=$recursiveState, commited=${fence.committedState}")
					recursiveState
				}
			}

			val swarm: Seq[Mono[PrimaryState]] = Seq.tabulate(swarmSize) { n => Capture_defer(() => path(n)) }
			val checks = for array <- doer.Task_sequenceToArray(swarm) yield promise.trySuccess(())
			checks.triggerAndForget()
			gate
		}
	}

	test("CausalFence: `advance` should either, skip transitions if failed, or commit updated state if successful") {
		val generators = getGenerators
		import generators.*

		PropF.forAllNoShrinkF { (expectedState0: Int, firstUpdater: Int => Mono[Int], secondUpdater: Int => Mono[Int]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[Int, doer.type](doer)(expectedState0)

				fence.advance(firstUpdater)

				fence.advance(secondUpdater).subscribeSyncCallbacks(
					actualState2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => {
								if actualState2 != expectedState2 then break(s"Unexpected final state: actual=$actualState2, expected=$expectedState2")
								else promise.trySuccess(())
							},
							expectedFailure2 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure2")
						),
						expectedFailure1 => break(s"Unexpected final state: actual=$actualState2, expected=$expectedFailure1")
					),
					actualFailure2 => firstUpdater(expectedState0).subscribeSyncCallbacks(
						expectedState1 => secondUpdater(expectedState1).subscribeSyncCallbacks(
							expectedState2 => break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedState2"),
							expectedFailure2 => {
								if actualFailure2 !=== expectedFailure2 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure2")
								else promise.trySuccess(())
							},
						),
						expectedFailure1 => {
							if actualFailure2 !=== expectedFailure1 then break(s"Unexpected final state: actual=$actualFailure2, expected=$expectedFailure1")
							else promise.trySuccess(())
						}
					)
				)
			}
			gate
		}
	}

	test("CausalFence: rollback before commit should be applied") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (expectedInitialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			run {
				val fence = CausalFence[PrimaryState, doer.type](doer)(expectedInitialState)

				fence.advanceSpeculatively { (actualInitialState, rba) =>
					def doTheRollback(): Unit = {
						rba.rollback(
							true,
							new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
									if rollbackApplication != ROLLBACK_APPLIED then break("The rollback's CompletionObserver was told that the rollback was not applied despite it should")
									else if actualFinalSuccessState != expectedInitialState then break("The rollback's CompletionObserver was told that the previous state wasn't restored despite it should")
								}

								override def onError(e: Throwable, rollbackApplication: OriginId): Unit = break("The rollback's CompletionObserver was told that the previous state is a failure despite it isn't")
							}
						)
					}

					updater(actualInitialState).andThen(
						_ => doTheRollback(),
						_ => doTheRollback()
					)
				}.subscribeSyncCallbacks(
					actualFinalSuccessState => if actualFinalSuccessState != expectedInitialState then break("The `Capture` returned by `advanceSpeculatively` yielded an unexpected value"),
					_ => break("The `Capture` returned by `advanceSpeculatively` received a sticking/failure state despite it shouldn't")
				)

				fence.causalAnchor(new CompletionObserver[PrimaryState] {
					override def onSuccess(actualSuccessfulState: PrimaryState, application: RollbackApplication): Unit = {
						if actualSuccessfulState != expectedInitialState then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
					}

					override def onError(e: Throwable, originId: OriginId): Unit = break("The causal anchor`s CompletionObserver received a sticking/failure state despite it shouldn't")
				}
				).subscribeSyncCallbacks(
					actualSuccessfulState => if actualSuccessfulState != expectedInitialState then break("The `Capture` returned by `causalAnchor` yielded an unexpected value"),
					_ => break("The `Capture` returned by `causalAnchor` captured an error and it shouldn't")
				)

				fence.committed.subscribeSyncCallbacks(
					actualCommitted => {
						if actualCommitted != expectedInitialState then break("The `Capture` returned by `committed` captured an unexpected value")
						else promise.trySuccess(())
					},
					_ => break("The `Capture` returned by `committed` captured an error and it shouldn't")
				)
			}
			gate
		}
	}

	test("Generators: random functions are deterministic") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (state0: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			updater(state0).triggerHardy(false) { state1A =>
				updater(state0).triggerHardy(true) { state1B =>
					if state1A !=== state1B then break("not deterministic")
					else promise.trySuccess(())
				}

			}
			gate
		}
	}

	test("CausalFence: rollback after commit should be ignored") {
		val generators = getGenerators
		import generators.*

		type PrimaryState = Int
		PropF.forAllNoShrinkF { (initialState: PrimaryState, updater: PrimaryState => Mono[PrimaryState]) =>
			val promise = Promise[Unit]()

			given Promise[Unit] = promise

			doer.executeSequentially { () =>
				try {
					updater(initialState).triggerHardy(false) { expectedFinalState =>
						println(s"Begin: expectedFinalState=$expectedFinalState")

						val fence = CausalFence[PrimaryState, doer.type](doer)(initialState)

						fence.advanceSpeculatively { (actualInitialState, rba) =>
							def doTheRollback(): Unit = {
								run {
									rba.rollback(
										true,
										new CompletionObserver[PrimaryState] {
											override def onSuccess(actualFinalSuccessState: PrimaryState, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The rollback's CompletionObserver received an unexpected primary state")
											}

											override def onError(actualFinalFailureState: Throwable, rollbackApplication: OriginId): Unit = {
												if rollbackApplication != ROLLBACK_IGNORED then break("The rollback's CompletionObserver was told that the rollback wasn't ignored despite it should")
												else if expectedFinalState.fold(_ !=== actualFinalFailureState, _ => true) then break("The rollback's CompletionObserver received an unexpected primary state")
											}
										}
									)
								}
							}

							updater(actualInitialState).andThen(
								_ => doTheRollback(),
								_ => doTheRollback()
							)
						}.triggerHardy(true) { actualFinalState =>
							if actualFinalState !=== expectedFinalState then break(s"The `Capture` returned by `advanceSpeculatively` received an unexpected state")

							fence.causalAnchor(new CompletionObserver[PrimaryState] {
								override def onSuccess(actualFinalSuccessState: PrimaryState, originId: OriginId): Unit = {
									if expectedFinalState.fold(_ => true, _ != actualFinalSuccessState) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}

								override def onError(actualFinalErrorState: Throwable, originId: OriginId): Unit = {
									if expectedFinalState.fold(_ !=== actualFinalErrorState, _ => true) then break("The `causalAnchor`'s CompletionObserver received an unexpected value")
								}
							}).subscribeHardy(true) { actualAnchor =>
								if actualAnchor !=== expectedFinalState then break("The capturer returned by `causalAnchor` yielded an unexpected value")
							}

							fence.committed.subscribeHardy(true) { actualCommitedState =>
								if actualCommitedState !=== expectedFinalState then break(s"The capturer returned by `committed` yielded an unexpected value: expected=$expectedFinalState, got=$actualCommitedState")
								else promise.trySuccess(())
							}
						}
					}
				} catch {
					case cause: Throwable => promise.tryFailure(cause)
				}
			}
			gate
		}
	}
}
