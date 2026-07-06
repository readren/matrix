---
type: "Log"
title: "Knowledge Base Change Log"
description: "History of modifications to the OKF bundle."
tags: ["log", "changelog"]
timestamp: "2026-07-09T14:30:00Z"
---

# Change Log

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
- Implemented `TimedSubscription` and `TimedTask` traits, split scheduling extension methods (lazy on `Task` receiving duration properties, immediate/hot on `LatchingTask` receiving pre-built `Delay <: Schedule` directly), and updated
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

- Documented Task and LatchingTask semantic evaluation invariants in sequencer-core.md.
- Documented the introduction of shared helper classes (DefaultCapturer_FlatMap, DefaultCapturer_FlatMapGuarded, DefaultCapturer_TransformWith, DefaultCapturer_RecoverWith) to optimize Observable and Task pipelines.

## [2026-06-25T02:00:00Z]

- Optimized LatchingTask-returning combinators (withFilter, map, mapGuarded, flatMapGuarded, transform, transformWith, recover, recoverWith) on DefaultCapturer to use lightweight anonymous DefaultCapturer subclasses directly, removing
  Covenant allocations.

## [2026-06-24T19:30:00Z]

- Documented transition function caching (`maybeMonoB`) design guidelines for `LatchingTask` combinators in `sequencer-core.md`.

## [2026-06-24T13:17:00Z]

- Documented Sequencer Core components and Covenant zero-allocation implementation guidelines in sequencer-core.md.

## [2026-06-24T02:10:00Z]

- Initialized OKF bundle structure.
