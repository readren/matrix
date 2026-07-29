---
type: "Log"
title: "Knowledge Base Change Log"
description: "History of modifications to the OKF bundle."
tags: ["log", "changelog"]
timestamp: "2026-07-28T17:25:00Z"
---

# Change Log

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
- Implemented `TimedSubscription` and `TimedTask` traits, split scheduling extension methods (lazy on `Task` receiving duration properties, immediate/hot on `Capturer` receiving pre-built `Delay <: Schedule` directly), and updated
  scheduled supplier factories inside [SchedulingExtension.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala).
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

- Optimized Capturer-returning combinators (withFilter, map, mapGuarded, flatMapGuarded, transform, transformWith, recover, recoverWith) on DefaultCapturer to use lightweight anonymous DefaultCapturer subclasses directly, removing
  Captor allocations.

## [2026-06-24T19:30:00Z]

- Documented transition function caching (`maybeMonoB`) design guidelines for `Capturer` combinators in `sequencer-core.md`.

## [2026-06-24T13:17:00Z]

- Documented Sequencer Core components and Captor zero-allocation implementation guidelines in sequencer-core.md.

## [2026-06-24T02:10:00Z]

- Initialized OKF bundle structure.
