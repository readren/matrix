---
type: "Component"
title: "Sequencer Core Component"
description: "Core execution model, Task hierarchy, and Captor (Captor) implementation details."
tags: ["sequencer", "task", "captor", "captor"]
timestamp: "2026-07-15T04:35:00Z"
---

# Sequencer Core Component


This component defines the single-threaded deterministic sequencing primitives under matrix.

## Core Hierarchy

* `Observable` (future `Mono`): The base trait for all lazy computations.
* `Task`: A task that executes lazily and can be subscribed to multiple times.
* `Capturer` (future `Capturer`): A single-run task that caches its completed result.
* `Captor` (future `Captor`): A latching task with an externally controllable completion hook (Promise-like).

## Semantic Invariants

* **Task Evaluation**: `Task` represents a lazy computation. Chained functional operands (e.g., functions passed to `map`, `flatMap`, `transformWith`) are evaluated **on every subscription**. Subscribing multiple times re-runs the entire
  pipeline and its side effects.
* **Capturer Evaluation**: `Capturer` caches its outcome (success or failure) once resolved. Chained functional operands are evaluated **at most once**. Subsequent subscriptions instantly yield the cached result without
  re-evaluating the transition functions or pipeline side effects.
* **Subscription Lifetime**: For pipelines returning a `Capturer`, discarding/unsubscribing from upstream mid-flight is semantically not supported because latching tasks are designed to guarantee run-to-completion once triggered.

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
* **Split Semantics (Task vs. Capturer)**:
    * **Task (Lazy Timer Start)**: Scheduling operations (`delayed`, `timeLimited`, `scheduled`, `retriedOnTimeout`) on a `Task` start their timers lazily when the task is **subscribed to**.
  * **Capturer (Immediate/Hot Timer Start)**: Single-shot scheduling operations (`delayed`, `timeLimited`) on a `Capturer` start their timers **immediately on operation call** (when the method is invoked). To ensure type safety
    against incorrect periodic timer reuse, these operations accept a pre-built `Delay` instance (where `Delay <: Schedule` represents single-shot delays). They return a `Capturer` that preserves caching guarantees and resolves to
    completion (either success or timeout) at most once. Periodic/retry operations are restricted from `Capturer` due to caching invariants.
* **Two-Level Scheduling & Chronological Ordering**:
    * **Hierarchical Queue**: Scalable scheduling uses a two-level heap structure (a global min-heap of active doers, and a per-doer private min-heap of schedules). This limits global queue operations to $O(\log D)$ (where $D$ is the number
      of active doers) instead of $O(\log N)$ (total schedules), drastically reducing lock contention on `thisProvider`.
    * **Ordering & Linearization**: Chronological execution order of schedules across different doers is guaranteed up to the serialization point of the queue pop (the `thisProvider.synchronized` block in
      `pollEmptyDoerWithEarliestElapsedSchedule`). Concurrent updates to schedules programmed after a worker thread has popped a doer are subject to standard race conditions (i.e. they do not preempt a popped doer's dispatched runnables).
* **Timer Timing Precision & Rounding Proof**:
  To ensure that a scheduled task is never executed earlier than expected (i.e. actual execution delay is $\ge$ requested delay), matrix uses a strict combination of rounding up during scheduling and rounding down during polling:
    * **Design Goal**: The actual execution time $T_{\text{actual}}$ must satisfy $T_{\text{actual}} \ge T_{\text{expected}}$ where $T_{\text{expected}} = T_{\text{start}} + D$ ($T_{\text{start}}$ is the request time, $D$ is the delay). The
      overshoot error $T_{\text{actual}} - T_{\text{expected}}$ should be as small as possible but never negative.
    * **Scheduling Rounding**: Target time is scheduled using `currentTimeRoundedUp` ($\lceil T \rceil$):
      $$T_{\text{scheduled}} = \lceil T_{\text{start}} \rceil + D$$
    * **Polling & Sleeping Rounding**: Worker threads check for expiration and sleep duration using `currentTimeRoundedDown` ($\lfloor T \rfloor$):
      $$\lfloor T_{\text{actual}} \rfloor \ge T_{\text{scheduled}} \iff T_{\text{actual}} \ge T_{\text{scheduled}}$$
    * **Mathematical Proof**:
      Since $T_{\text{actual}} \ge T_{\text{scheduled}}$, substituting the formula yields:
      $$T_{\text{actual}} \ge \lceil T_{\text{start}} \rceil + D$$
      Since the ceiling of any real number is greater than or equal to the number itself ($\lceil T \rceil \ge T$), we have:
      $$T_{\text{actual}} \ge T_{\text{start}} + D = T_{\text{expected}}$$
      $$T_{\text{actual}} - T_{\text{expected}} \ge 0 \quad (\text{Never negative})$$
      Moreover, the maximum overshoot is bounded by the ceiling rounding error:
      $$T_{\text{actual}} - T_{\text{expected}} \le \lceil T_{\text{start}} \rceil - T_{\text{start}} < 1\text{ ms}$$
      Thus, the overshoot is strictly bounded to $< 1$ millisecond, representing the absolute minimum possible error within the constraints of a millisecond-resolution timer.

## DoerProvider Selection Guide

When configuring execution environments, select the `DoerProvider` implementation that matches the scheduling, priority, load balancing, and contention requirements:

### 1. Schedulers (Support for `SchedulingExtension` / Timer tasks)

* **CooperativeFlatPollingSchedulerDp (Flat Polling)**:
  * **Workload**: Low to moderate schedule density (few active timers overall) or sparse timers (at most one schedule per doer).
  * **Pros**: Low constant overhead, minimal GC footprint, and simpler lock structure (no nested doer-local locks in `program`/`cancel`).
  * **Cons**: Global schedule management scales at $O(\log N)$ (where $N$ is total schedules), increasing lock contention under massive numbers of timers.
* **CooperativeHierarchicalPollingSchedulerDp (Hierarchical Polling)**:
  * **Workload**: High timer density (many concurrent active timers grouped under each active doer/actor).
  * **Pros**: Decouples global schedule management into a two-level queue, scaling at $O(\log D)$ (where $D$ is the number of active doers) instead of $O(\log N)$.
  * **Cons**: Nested lock complexity (locks both `owner` and `thisProvider` during schedule modification) and potential allocation overhead, though the private priority queue is initialized lazily on demand only for doers that actually use
    scheduling.
* **CooperativeThreadDrivenSchedulerDp (Thread-Driven)**:
  * **Workload**: Complex scheduling environments where queue management should be offloaded from worker threads.
  * **Pros**: Offloads timer management and worker wakeup triggers to a dedicated scheduler thread, isolating worker execution pools from timer overhead.
  * **Cons**: Requires an additional active thread, increasing system resource usage.
* **CooperativeShardedPollingSchedulerDp (Sharded Polling)**:
  * **Workload**: Multi-threaded scheduling environments with multiple active doers.
  * **Pros**: Partitions scheduling heaps per worker thread to avoid global queue lock contention.
  * **Cons**: Requires synchronization locks on the worker's monitor for scheduling, canceling, and polling operations.
* **CooperativeLocalPollingSchedulerDp (Local Polling)**:
  * **Workload**: High-frequency scheduling environments where queue contention and lock overhead must be completely eliminated.
  * **Pros**: Keeps scheduling priority queues strictly thread-local to each worker, requiring zero locks or synchronization. Cancellation (`cancel` and `cancelAll`) is optimized to $O(1)$ lazy evaluation.
  * **Cons**: Canceled schedules are cleaned up lazily when they expire, meaning they occupy memory in the priority queue until their scheduled time is reached.
* **StandardSchedulingDp (Dedicated Thread)**:
  * **Workload**: Testing or extremely small-scale production with very few doers where independent thread behavior is required.
  * **Pros**: Bypasses shared worker pools; every doer has its own private `ScheduledExecutorService` (1 thread per doer), eliminating scheduling interference or global queue locks.
  * **Cons**: Extremely high thread overhead; does not scale to large numbers of doers.

### 2. General Executors (Asynchronous task execution only)

* **CooperativeWorkersDp (Default Cooperative)**:
  * **Workload**: Standard asynchronous task execution without scheduling or custom priority constraints.
  * **Pros**: Highly efficient shared cooperative thread pool with $O(1)$ lock-free task polling.
  * **Cons**: No native support for timer tasks or priority queuing.
* **CooperativeWorkersTieredDp (Tiered Priority)**:
  * **Workload**: Dual-priority task execution (regular vs. high-priority doers).
  * **Pros**: Polls high-priority doers from a separate priority queue before processing regular doers, ensuring priority task precedence.
  * **Cons**: No scheduling or timer support.
* **RoundRobinDp (Static Load Balancing)**:
  * **Workload**: Static number of long-lived doers where execution load is evenly distributed.
  * **Pros**: Simple round-robin doer mapping matching the thread pool size exactly (1 thread per doer), removing worker-coordination overhead.
  * **Cons**: No scheduling or dynamic load balancing.
* **LeastLoadedFixedWorkerDp (Dynamic Load Balancing)**:
  * **Workload**: Abundant, short-lived doers created on demand.
  * **Pros**: Dynamically routes new doers to the worker thread with the shortest execution queue at allocation time.
  * **Cons**: Binds doers statically to threads at creation time; no scheduling support.

## Implementation Guidelines

* **Zero-Allocation Pipelines**: Monadic combinators on `Capturer` are implemented using inline custom anonymous classes extending `Subscription` with `MonoObserver` (or `AbstractTask`) directly, bypassing intermediate wrapping steps.
  Combinators returning a `Capturer` are implemented via lightweight anonymous subclasses extending `DefaultCaptor[B] with MonoObserver[A]`, leveraging `fulfillSync` and `breakSync` to handle state propagation with zero intermediate
  allocations.
* **Testing Exception Suppression**: When testing thread pool execution components (e.g., `CooperativeFlatPollingSchedulerDp`), unhandled exceptions thrown by asynchronous tasks terminate worker threads and propagate to the default
  uncaught exception handler (printing to stderr). To keep build logs clean, pass a custom `ThreadFactory` that intercepts the thread's uncaught exception handler to suppress simulated/expected test exceptions (such as `FaultyValue` and
  test-generated random throwables) while preserving printing of unexpected test environment bugs. Modifying the signature of `onUnhandledException` (e.g., to return a boolean indicating suppression status) should be avoided because it
  breaks binary/source compatibility across many provider subclasses and mixes log reporting with thread-pool lifecycle/recovery responsibilities.
* **Sleep-Time Tracking Toggle**: Cooperative worker-pool based providers (`CooperativeWorkersDp` and subclasses) accept a `trackSleepTime: Boolean` constructor parameter. When set to `false` (default), worker threads bypass high-frequency
  `System.nanoTime()` measurements and atomic/volatile metrics updates during worker loop sleep transitions. This eliminates telemetry-related CPU and system timer overhead in latency-sensitive production environments.
* **ThreadDrivenScheduler Component**: The `ThreadDrivenScheduler` manages task scheduling and periodic/one-shot executions using a dedicated background thread and an array-based binary min-heap, while executing task runnables cooperatively
  on the worker thread pool.

