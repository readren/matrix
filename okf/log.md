---
type: "Log"
title: "Knowledge Base Change Log"
description: "History of modifications to the OKF bundle."
tags: ["log", "changelog"]
timestamp: "2026-08-22T00:34:00Z"
---

## [2026-08-22T00:34:00Z]

- Documented Failover Retirement Recovery across newly elected leaders and Contiguous Log Slicing from log buffer offset in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-21T05:52:00Z]

- Documented Ghost Leader Reconfiguration Invariant and Asynchronous Learner Convergence vs. Quorum Progress Decoupling dynamics in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-04T16:48:00Z]

- Documented retirement driving mechanism, log-append retries, and snapshot fallback invariants during configuration retirement transitions in `okf/lazy-multi-raft-consensus.md`.
- Refined OKF maintenance guidelines in `AGENTS.md` and `.agents/skills/okf_maintenance/SKILL.md` to mandate high-level invariant focus, forbid code implementation details/variable names, and prevent raw session/debugging logs.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-04T13:08:00Z]

- Formulated structural design suggestions for `RetirementDriver` cleanup sequencing and `CandidateDecider` retiree voting eligibility constraints.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-04T13:04:00Z]

- Analyzed `ConsensusParticipantSdm.scala:3843` assertion error triggered at `scratch_2.log:2770-2772`, preventing driver cleanup on lag.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-03T21:17:00Z]

- Analyzed `CandidateDecider` and `whenVotingAnother` livelock when a candidate node votes for a higher-term `RETIREE` whose `commitIndex` exceeds the node's local log.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-03T19:05:00Z]

- Analyzed `ConsensusParticipantSdm` quiescence state machine and verified `RetirementDriver` callback object identity reference.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-02T12:48:00Z]

- Documented `Trial` sentinel update to `EMPTY = new AnyRef` and null-safety requirements in `isEqualTo`, `equals`, and `contains` when wrapping `Null` or `Maybe.empty` in `okf/common-primitives.md`.

## [2026-08-01T17:31:00Z]

- Added `trait SchedulingDoer extends Doer, SchedulingExtension` in `Doer.scala`.
- Made `SchedulingExtension` extend `ScheduledFluxExtension` (`trait SchedulingExtension extends ScheduledFluxExtension { thisDoer: Doer => ... }`).
- Replaced type intersection `Doer & SchedulingExtension` with `SchedulingDoer` across cluster services (`ParticipantService`), Akka integration (`ActorBasedSchedulingDoer`), benchmark suites (`SchedulingDoerBenchmark`), and test suites
  (`ScheduledMonoTests`, `ScheduledFluxTests`, `GeneratorsForDoerTests`).
- Updated `okf/sequencer-core.md` timestamp.

## [2026-08-01T16:27:00Z]

- Mixed `FluxExtension` and `LoopingExtension` directly into `Doer` trait (`trait Doer extends DoerCore, DoerTaskOps, FluxExtension, LoopingExtension`).
- Simplified provider facades (`CooperativeWorkersDp`, `CooperativeSchedulerDpCompanion`, `StandardSchedulingDp`) and test traits (`FluxDoerTests`, `LoopingDoerTests`, `ScheduledFluxTests`, `GeneratorsForDoerTests`) to eliminate redundant
  extension mixins and casts.
- Updated `okf/sequencer-core.md` timestamp.

## [2026-07-31T15:48:00Z]

- Added explicit type ascriptions `(Maybe.empty: Maybe[Int])` in `common/src/test/scala/MaybeTest.scala` for `map`, `flatMap`, `fold`, and `exists` tests to fix parameter type inference in closures.

## [2026-07-31T15:40:00Z]

- Removed invalid extension method `==` in `Maybe.scala` to resolve compiler warnings `[E194]` (unselectable extension method) and `[E121]` (unreachable case pattern match).

## [2026-07-31T10:53:00Z]

- Implemented `mapImpl` and `flatMapImpl` quote macros in `MaybeMacros.scala` to enforce compile-time anti-nesting and non-nullability checks on return type parameter `B` for `Maybe.map` and `Maybe.flatMap`.
- Refactored `map` and `flatMap` extension methods in `Maybe.scala` to delegate to `MaybeMacros.mapImpl` and `MaybeMacros.flatMapImpl`.
- Updated `okf/common-primitives.md`.

## [2026-07-30T13:35:00Z]

- Designed `Capturer` functional operand execution count test suite in `sequencer/core` (`MonoTests.scala`):
    - Documented property-based test checking that all factory methods and transformation operators (`apply`, `defer`, `fromFutureBuilder`, `combine`, `andThen`, `withFilter`, `map`, `flatMap`, `transform`, `transformWith`, `recover`,
      `recoverWith`, and guarded variants) execute passed functional operands at most once (0 or 1 time), even across multiple subscriptions.
    - Updated `okf/sequencer-core.md`.

## [2026-07-29T18:56:00Z]

- Extracted `ResultIncrementalCoalescingDoerTests` mix-in trait in `sequencer/core`:
    - Centralized `ResultIncrementalCoalescing` test suite (`first contender wins`, `second contender supersedes`, `second contender yields`, `new competition starts after previous completes`) into a reusable trait.
    - Mixed `ResultIncrementalCoalescingDoerTests` into all provider test suites supporting scheduling & looping (`StandardSchedulingDpTest`, `CooperativeContainedPollingSchedulerDpTest`, `CooperativeFlatPollingSchedulerDpTest`, etc.).
    - Updated `okf/sequencer-core.md` documentation guidelines.

## [2026-07-29T17:49:00Z]

- Created `CooperativeWorkersDoerTests` mix-in trait in `sequencer/providers`:
    - Extracted shared `CooperativeWorkersDp` thread-pool lifecycle, pending runnable tracking, and worker sleep/wakeup race condition tests into a reusable trait.
    - Updated `okf/sequencer-core.md` guidelines.

## [2026-07-29T17:34:00Z]

- Modularized `CooperativeWorkersDpTestNew.scala` in `sequencer/providers`:
    - Created `CooperativeWorkersDpTestNew` extending `DoerProviderTestBase[DoerFacade]` and mixing in `VanillaDoerTests[DoerFacade]`.
    - Configured `TestThreadFactory` and `onUnhandledException` bridging.
    - Verified 100% test pass rate (38/38 tests passed).

## [2026-07-29T17:25:00Z]

- Modularized `DoerProvider` testing framework in `sequencer/core` and `sequencer/providers`:
    - Centralized suite lifecycle, logging, property sample generators (`forAllTaskOperandExceptions`, `forAllSubscribeExceptions`), and exception assertions in `DoerProviderTestBase`.
    - Created capability-focused test traits (`VanillaDoerTests`, `FluxDoerTests`, `SchedulingDoerTests`, `ScheduledFluxDoerTests`, `LoopingDoerTests`).
    - Created single concrete provider test suites (`StandardSchedulingDpTestNew`, `CooperativeContainedPollingSchedulerDpTestNew`, etc.) mixing in supported extension traits.
    - Documented testing architecture and thread exception suppression in `okf/sequencer-core.md`.

## [2026-07-28T17:25:00Z]

- Implemented `ScheduledFluxExtension` in `sequencer/core/src/main/scala/ScheduledFluxExtension.scala`.
- Moved periodic stream scheduling operations (`task.scheduled`, `Flux_schedules`, `Flux_schedulesFlat`) to `ScheduledFluxExtension`, returning `Flux[A]` / `TimedFlux[A]`.
- Refactored single-shot delay implementations in `SchedulingExtension` (`Task_Delayed`, `Task_DelaysSupplier`, `Task_DelaysSupplierFlat`) returning `Task[A]` / `TimedTask[A]`.
- Mixed `ScheduledFluxExtension` into `StandardSchedulingDp.ProvidedDoerFacade` and `CooperativeSchedulerDpCompanion.SchedulingDoerFacade`.
- Added abstract test suite `ScheduledFluxDoerProviderTest` and updated `SchedulingDoerProviderTest` for single-shot delay assertions.
- Updated `okf/sequencer-core.md` documentation.

## [2026-07-28T17:20:00Z]

- Clarified public method mapping between `SchedulingExtension` (single-shot `Task`/`Mono` delays) and `ScheduledFluxExtension` (periodic `Flux` stream schedules).

## [2026-07-28T17:15:00Z]

- Evaluated trait naming options (`ScheduledFluxExtension` vs `TimedFluxExtension`) for the proposed Option 3 extension in `sequencer/core`.

## [2026-07-28T17:13:00Z]

- Analyzed architectural design options for integrating periodic scheduling primitives with `FluxExtension` to replace incorrect `Task` return types with `Flux` in `SchedulingExtension`.

## [2026-07-27T20:17:00Z]

- Fixed `emitterGen` in `GeneratorsForDoerTests.scala` by deferring `StreamEmitter` emissions to `subscribeSync` invocation.
- All 17 test cases in `FluxDoerProviderTest` suites now pass across all 7 `DoerProvider` implementations.

## [2026-07-27T20:09:00Z]

- Added concrete `FluxDoerProviderTest` suites for all `DoerProvider` implementations in `sequencer/providers`.
- Mixed `FluxExtension` into `StandardSchedulingDp.ProvidedDoerFacade` and `CooperativeSchedulerDpCompanion.SchedulingDoerFacade`.

## [2026-07-27T19:52:00Z]

- Documented `FluxExtension` testing infrastructure and abstract `FluxDoerProviderTest` suite specification in `okf/sequencer-core.md`.

## [2026-07-26T15:51:15Z]

- Updated `okf/common-primitives.md` to document how `Maybe`'s opaque type scope prevents macro `OrType` matching on nested `Maybe[T]` instances.

## [2026-07-25T13:28:00Z]

- Updated `okf/common-primitives.md` to reflect `Maybe`'s refactored `opaque type` definition and compile-time macro anti-nesting guards.

## [2026-07-22T17:45:00Z]

- Added architectural decision record `okf/adr-spare-slot-pattern-evaluation.md` evaluating the spare-slot zero-allocation pattern in `sequencer/core` primitives.

## [2026-07-22T15:35:00Z]

- Refined the DoerProvider selection guide in `okf/sequencer-core.md` by integrating structural "like/unlike" comparative points directly into every scheduler item description, removing redundant shared traits, and simplifying the workload
  descriptions.

## [2026-07-22T15:15:00Z]

- Aligned `CooperativeLocalPollingSchedulerDp` and `CooperativeContainedPollingSchedulerDp` descriptions in `okf/sequencer-core.md` using identical phrasing for shared features, highlighting memory reclamation behavior as their primary
  differentiator.

## [2026-07-22T15:00:00Z]

- Refactored the DoerProvider selection guide in `okf/sequencer-core.md` to sort all seven scheduling implementations in descending order of utility probability (with Local and Contained Polling first) and added comparative "like/unlike"
  analysis.

## [2026-07-22T14:30:00Z]

- Added documentation for the `CooperativeContainedPollingSchedulerDp` contained scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-18T02:12:36Z]

- Added the complete empirical benchmark results table to the bottom of `okf/sequencer-core.md`.

## [2026-07-18T02:04:31Z]

- Corrected Sharded Polling scheduler performance affirmations in `okf/sequencer-core.md` based on empirical benchmark results showing that Local Polling consistently outperforms Sharded Polling due to its lock-free execution model.

## [2026-07-16T22:05:00Z]

- Updated `okf/sequencer-core.md` with performance characteristics and trade-offs of Hierarchical, Sharded, and Local Polling schedulers discovered during benchmarking under load.

## [2026-07-15T04:35:00Z]

- Added high-level architecture documentation for the `ThreadDrivenScheduler` component to `okf/sequencer-core.md`.

## [2026-07-15T04:15:00Z]

- Documented the optional `trackSleepTime` constructor parameter and telemetry-overhead design toggle in `okf/sequencer-core.md`.

## [2026-07-14T23:12:00Z]

- Added the design goal, execution model, and mathematical correctness proof for Timer Timing Precision and millisecond rounding to `okf/sequencer-core.md`.

## [2026-07-14T19:30:00Z]

- Updated Hierarchical Polling scheduler cons in `okf/sequencer-core.md` to reflect lazy initialization of its private priority queue.

## [2026-07-14T02:56:00Z]

- Renamed all scheduling-enabled `DoerProvider` classes to follow a symmetrical, clean, and semantically honest naming convention in `okf/sequencer-core.md`.

## [2026-07-14T02:15:00Z]

- Added documentation for the `CooperativeShardedPollingSchedulerDp` sharded scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-14T01:27:00Z]

- Added documentation for the new `CooperativeLocalPollingSchedulerDp` thread-local scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-13T18:30:00Z]

- Added the DoerProvider Selection Guide detailing all eight scheduler and executor implementations in `okf/sequencer-core.md`.

## [2026-07-13T03:48:00Z]

- Documented two-level scheduling heap architecture and chronological ordering guarantees for cross-doer scheduling in `okf/sequencer-core.md`.

## [2026-07-09T14:30:00Z]

- Added ADR `okf/adr-simplify-error-handling.md` detailing the design decision to remove generic exception guarding and unify unhandled exception routing under `onUnhandledException`.
- Updated `okf/index.md` to reference the new ADR.

## [2026-07-09T00:08:20Z]

- Incorporated the final design discussion regarding quorum response times (Order Statistics / median latency) and availability vulnerabilities of small subgroups vs. full cluster replication in `okf/lazy-multi-raft-consensus.md` and
  `okf/transcript-lazy-multi-raft-design.md`.

## [2026-07-08T23:32:00Z]

- Added concept documentation for the Lazy Multi-Raft consensus architecture (`okf/lazy-multi-raft-consensus.md`).
- Included and translated the design discussion transcript on Lazy Multi-Raft to English (`okf/transcript-lazy-multi-raft-design.md`).
- Updated `okf/index.md` to reference the new documents.

## [2026-07-07T15:00:00Z]

- Added testing guidelines and compatibility analysis regarding the suppression of simulated exceptions and why changing the signature of `onUnhandledException` is avoided, documented within `okf/sequencer-core.md`.

## [2026-07-03T16:58:00Z]

- Documented `Muxer` encapsulation and `DefaultCapturer` unchecked variance inheritance design decisions in `okf/sequencer-core.md` and added explanatory Scaladoc notes in `sequencer/core/src/main/scala/Doer.scala`.

## [2026-07-01T12:15:00Z]

- Added and refined ADR `okf/adr-scheduling-subscription-design.md` detailing the design options and recommendation for exposing schedules in timed task subscriptions, detailing the new `.onSubscription` side-effect hook.
- Documented user-guide specifications for the scheduling split model, TimedSubscription access, and immediate subscription hooks in `okf/sequencer-core.md`.
- Implemented `TimedSubscription` and `TimedTask` traits, split scheduling extension methods (lazy on `Task` receiving duration properties, immediate/hot on `Capturer` receiving pre-built `Delay <: Schedule` directly), and updated scheduled
  supplier factories inside [SchedulingExtension.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala).
- Implemented the `onSubscription(Schedule => Unit)` hook and backing class `Task_OnSubscription` inside [SchedulingExtension.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala), providing
  synchronous pre-trigger access to schedules.
- Added a unit test in [SchedulingDoerProviderTest.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/test/scala/SchedulingDoerProviderTest.scala) verifying `onSubscription` lifecycle execution.

## [2026-07-01T11:59:48Z]

- Documented `ResultIncrementalCoalescingGrouped` architecture and reference lifetime invariants in `okf/sequencer-core.md`.
- Documented the `Muxer` iteration safety model and FIFO subscription order invariants in `okf/sequencer-core.md`.
- Documented `ResultIncrementalCoalescing` monotonic convergence invariants in `okf/sequencer-core.md`.

## [2026-06-26T01:42:00Z]

- Documented strict equality compatibility requirements (using `eq null` check and `.equals`) for `Trial`'s equality checks in `okf/common-primitives.md`.

## [2026-06-26T01:40:00Z]

- Documented NPE risk in direct `.equals` calls on empty Trial instances and the need for null-safe `==` checks in `okf/common-primitives.md`.

## [2026-06-26T01:25:00Z]

- Updated OKF documentation in `okf/common-primitives.md` to reflect the fix of `getOrElse` and `exists`, and the privacy change to `Trial.Failure` preventing nested collision issues. Addressed pending design arguments for exception
  comparison.

## [2026-06-25T18:51:15Z]

- Documented Task and Capturer semantic evaluation invariants in sequencer-core.md.
- Documented the introduction of shared helper classes (DefaultCapturer_FlatMap, DefaultCapturer_FlatMapGuarded, DefaultCapturer_TransformWith, DefaultCapturer_RecoverWith) to optimize Observable and Task pipelines.

## [2026-06-25T02:00:00Z]

- Optimized Capturer-returning combinators (withFilter, map, mapGuarded, flatMapGuarded, transform, transformWith, recover, recoverWith) on DefaultCapturer to use lightweight anonymous DefaultCapturer subclasses directly, removing Captor
  allocations.

## [2026-06-24T19:30:00Z]

- Documented transition function caching (`maybeMonoB`) design guidelines for `Capturer` combinators in `sequencer-core.md`.

## [2026-06-24T13:17:00Z]

- Documented Sequencer Core components and Captor zero-allocation implementation guidelines in sequencer-core.md.

## [2026-06-24T02:10:00Z]

## [2026-08-10T19:08:00Z]

- Documented per-peer in-flight append backpressure, monotonic serial tracking, and `AppendResult` ADT non-mutation invariants in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-11T20:15:00Z]

- Documented Raft §5.4.2 commitment verification invariants, current-term no-op entry insertion triggers, and committed-only log compaction boundaries in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-12T15:10:00Z]

- Documented `Retiring` participant `StateInfo` election bounding invariants (exposing excluding configuration change index/term to prevent election deadlocks) in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-12T20:16:00Z]

- Restructured and expanded Section 6 in `okf/lazy-multi-raft-consensus.md` to be fully self-contained, defining the Participant Role Lifecycle, Retirement Replication Driver Architecture, Quiescence Protocol, and Quiescence Preconditions.
- Corrected the Quiescence Precondition specification in `okf/lazy-multi-raft-consensus.md` to clarify that retirement driver clearance is evaluated against the participant's own local registry (for drivers it initiated as leader to catch
  up excluded followers), not a remote leader registry.
