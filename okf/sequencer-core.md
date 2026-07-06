---
type: "Component"
title: "Sequencer Core Component"
description: "Core execution model, Task hierarchy, and Covenant (Captor) implementation details."
tags: ["sequencer", "task", "covenant", "captor"]
timestamp: "2026-07-07T15:00:00Z"
---

# Sequencer Core Component

This component defines the single-threaded deterministic sequencing primitives under matrix.

## Core Hierarchy

* `Observable` (future `Mono`): The base trait for all lazy computations.
* `Task`: A task that executes lazily and can be subscribed to multiple times.
* `LatchingTask` (future `Capturer`): A single-run task that caches its completed result.
* `Covenant` (future `Captor`): A latching task with an externally controllable completion hook (Promise-like).

## Semantic Invariants

* **Task Evaluation**: `Task` represents a lazy computation. Chained functional operands (e.g., functions passed to `map`, `flatMap`, `transformWith`) are evaluated **on every subscription**. Subscribing multiple times re-runs the entire
  pipeline and its side effects.
* **LatchingTask Evaluation**: `LatchingTask` caches its outcome (success or failure) once resolved. Chained functional operands are evaluated **at most once**. Subsequent subscriptions instantly yield the cached result without
  re-evaluating the transition functions or pipeline side effects.
* **Subscription Lifetime**: For pipelines returning a `LatchingTask`, discarding/unsubscribing from upstream mid-flight is semantically not supported because latching tasks are designed to guarantee run-to-completion once triggered.

## Subscription & Muxing

* `Muxer`: Manages a registry of observers (`MonoObserver`) and handles broadcasting.
* `ObservingSubscription`: Consolidates `Subscription` and `TargetProxy` to allow direct registry mapping within a `Muxer`, reducing allocations.
* **Iteration Safety**: To preserve the zero-allocation model, the `Muxer` implements re-entrancy safety using a recursion depth counter. During active iteration, removals write tombstones (nullify slots) instead of shifting, with array
  compaction deferred until the outermost iteration exits.
* **FIFO Subscription Order**: To guarantee that observers are notified in subscription order (FIFO), `addTarget` must not occupy an empty `maybeFirstTarget` if `maybeFollowingTargets` contains elements. Instead, it must append the new
  observer to the end of `maybeFollowingTargets`, allowing deferred compaction to promote the oldest active observer to `maybeFirstTarget` once the iteration exits.
* **Inheritance Model & Encapsulation**: To avoid extra allocations, `DefaultCapturer` directly extends `Muxer[A, MonoObserver]` instead of containing it as a component. Because `Muxer` is invariant on `A`, `DefaultCapturer` bypasses
  standard Scala variance limits using `@uncheckedVariance` on the parent type. This remains safe because all mutable and traversal methods on `Muxer` are `protected` or `private`, preventing type-unsafe operations from being exposed to
  external clients.

## Monotonic Convergence

* `ResultIncrementalCoalescing`: Manages the convergence of concurrent executions into a single, stable terminal result.
    * **Incumbency**: An arbitrator function decides if a new execution unseats the current `incumbent`.
    * **Synchronous and Nested Contention**: If an incumbent completes synchronously, it triggers down-chain observers and clears the active competition. If those observers start a new contention synchronously (nested contention), the state
      transitions to the new competition.
    * **Incumbency Guards**: Observers verify that the completing winner matches the current incumbent (`chosenWinner eq incumbent`) to prevent stale or unsubscribed notifications from corrupting state during nested contentions.
* `ResultIncrementalCoalescingGrouped`: Parameterized/grouped version of monotonic convergence using parameter keys `P`.
    * **Reference Lifetime**: Completed contender tasks (`incumbent`) and their subscriptions are nullified upon completion to prevent memory leaks, as the enclosing `Competition` task is returned to the user.

## Scheduling and Timing Extensions

The [SchedulingExtension](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala) trait extends `Doer` with temporal operators. Exposing underlying timer configurations and aligning execution behaviors is
structured as follows:

* **Timed Subscription Access**: Subscriptions that start a schedule (e.g., `delayed`, `timeLimited`, `Task_schedules`) return a [TimedSubscription](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala)
  which provides type-safe access to the underlying `Schedule` via its `schedule` member. Scheduled suppliers receive the `TimedSubscription` handle to allow cancellation and inspection from within the callback.
* **Immediate Subscription Hooks**: Callers can inspect the underlying `Schedule` before the task completes (e.g. for custom cancellation, logging, or pre-trigger checks) using the `.onSubscription(schedule => Unit)` side-effect hook on a
  `TimedTask`. This runs synchronously during `subscribeSync` right after the schedule is created, but before it can run.
* **Split Semantics (Task vs. LatchingTask)**:
    * **Task (Lazy Timer Start)**: Scheduling operations (`delayed`, `timeLimited`, `scheduled`, `retriedOnTimeout`) on a `Task` start their timers lazily when the task is **subscribed to**.
    * **LatchingTask (Immediate/Hot Timer Start)**: Single-shot scheduling operations (`delayed`, `timeLimited`) on a `LatchingTask` start their timers **immediately on operation call** (when the method is invoked). To ensure type safety
      against incorrect periodic timer reuse, these operations accept a pre-built `Delay` instance (where `Delay <: Schedule` represents single-shot delays). They return a `LatchingTask` that preserves caching guarantees and resolves to
      completion (either success or timeout) at most once. Periodic/retry operations are restricted from `LatchingTask` due to caching invariants.

## Implementation Guidelines

* **Zero-Allocation Pipelines**: Monadic combinators on `Capturer` are implemented using inline custom anonymous classes extending `Subscription` with `MonoObserver` (or `AbstractTask`) directly, bypassing intermediate wrapping steps.
  Combinators returning a `Capturer` are implemented via lightweight anonymous subclasses extending `DefaultCaptor[B] with MonoObserver[A]`, leveraging `fulfillSync` and `breakSync` to handle state propagation with zero intermediate
  allocations.
* **Testing Exception Suppression**: When testing thread pool execution components (e.g., `CooperativeWorkersWithPollingSchedulerDp`), unhandled exceptions thrown by asynchronous tasks terminate worker threads and propagate to the default
  uncaught exception handler (printing to stderr). To keep build logs clean, pass a custom `ThreadFactory` that intercepts the thread's uncaught exception handler to suppress simulated/expected test exceptions (such as `FaultyValue` and
  test-generated random throwables) while preserving printing of unexpected test environment bugs. Modifying the signature of `onUnhandledException` (e.g., to return a boolean indicating suppression status) should be avoided because it
  breaks binary/source compatibility across many provider subclasses and mixes log reporting with thread-pool lifecycle/recovery responsibilities.
