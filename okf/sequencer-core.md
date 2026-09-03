---
type: "Component"
title: "Sequencer Core Component"
description: "Core execution model, Task hierarchy, and Capture (Captor) implementation details."
tags: ["sequencer", "task", "captor", "flux", "testing"]
timestamp: "2026-09-01T23:10:00Z"
---

# Sequencer Core Component

This component defines the single-threaded deterministic sequencing primitives under matrix.

## Core Hierarchy

* `Observable` (future `Mono`): The base trait for all lazy computations.
* `Task`: A task that executes lazily and can be subscribed to multiple times.
* `Capture`: A single-run task that caches its completed result.
* `Captor`: A latching task with an externally controllable completion hook (Promise-like).

## Semantic Invariants

* **Task Evaluation**: `Task` represents a lazy computation. Chained functional operands (e.g., functions passed to `map`, `flatMap`, `transformWith`) are evaluated **on every subscription**. Subscribing multiple times re-runs the entire
  pipeline and its side effects.
* **Capture Evaluation**: `Capture` caches its outcome (success or failure) once resolved. Chained functional operands are evaluated **at most once**. Subsequent subscriptions instantly yield the cached result without re-evaluating the
  transition functions or pipeline side effects.
* **Subscription Lifetime**: For pipelines returning a `Capture`, discarding/unsubscribing from upstream mid-flight is semantically not supported because latching tasks are designed to guarantee run-to-completion once triggered.

## Subscription & Muxing

* `Muxer`: Manages a registry of observers (`MonoObserver`) and handles broadcasting.
* `ObservingSubscription`: Consolidates `Subscription` and `TargetProxy` to allow direct registry mapping within a `Muxer`, reducing allocations.
* **Iteration Safety**: To preserve the zero-allocation model, the `Muxer` implements re-entrancy safety using a recursion depth counter. During active iteration, removals write tombstones (nullify slots) instead of shifting, with array
  compaction deferred until the outermost iteration exits.
* **FIFO Subscription Order**: To guarantee that observers are notified in subscription order (FIFO), `addTarget` must not occupy an empty `maybeFirstTarget` if `maybeFollowingTargets` contains elements. Instead, it must append the new
  observer to the end of `maybeFollowingTargets`, allowing deferred compaction to promote the oldest active observer to `maybeFirstTarget` once the iteration exits.
* **Inheritance Model & Encapsulation**: To avoid extra allocations, `DefaultCapture` directly extends `Muxer[A, MonoObserver]` instead of containing it as a component. Because `Muxer` is invariant on `A`, `DefaultCapture` bypasses
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

The [SchedulingExtension](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala) and [ScheduledFluxExtension](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/ScheduledFluxExtension.scala)
traits extend `Doer` with temporal operators. Exposing underlying timer configurations and aligning execution behaviors is structured as follows:

* **Separation of Task (Mono) vs. Flux (Stream) Scheduling**:
    * Single-shot delays (`delayed`, `timeLimited`, `Task_sleeps`, `Task_delays`, `Task_delaysFlat`, `retriedOnTimeout`) return a `Task[A]` / `TimedTask[A]` in `SchedulingExtension` and complete at most once.
    * Periodic time-driven schedules (`task.scheduled(kind, ...)`, `Flux_schedules`, `Flux_schedulesFlat`) emit multi-value push streams and return a `Flux[A]` / `TimedFlux[A]` in `ScheduledFluxExtension`.
* **Timed Subscription Access**: Subscriptions that start a schedule (e.g., `delayed`, `timeLimited`, `Flux_schedules`) return a `TimedSubscription` which provides type-safe access to the underlying `Schedule` via its `schedule` member.
  Scheduled suppliers receive the `TimedSubscription` handle to allow cancellation and inspection from within the callback.
* **Immediate Subscription Hooks**: Callers can inspect the underlying `Schedule` before the task or flux completes using `.andOnSubscription(schedule => Unit)` on a `TimedTask` or `TimedFlux`. This runs synchronously during `subscribeSync`
  right after the schedule is created, but before it can run.
* **Split Semantics (Task vs. Capture)**:
    * **Task (Lazy Timer Start)**: Single-shot scheduling operations (`delayed`, `timeLimited`, `retriedOnTimeout`) on a `Task` start their timers lazily when the task is **subscribed to**.
  * **Capture (Immediate/Hot Timer Start)**: Single-shot scheduling operations (`delayed`, `timeLimited`) on a `Capture` start their timers **immediately on operation call** (when the method is invoked). To ensure type safety against
    incorrect periodic timer reuse, these operations accept a pre-built `Delay` instance (where `Delay <: Schedule` represents single-shot delays). They return a `Capture` that preserves caching guarantees and resolves to completion (either
    success or timeout) at most once. Periodic/retry operations are restricted from `Capture` due to caching invariants.
* **Two-Level Scheduling & Chronological Ordering**:
    * **Hierarchical Queue**: Scalable scheduling uses a two-level heap structure (a global min-heap of active doers, and a per-doer private min-heap of schedules). This limits global queue operations to $O (\log D)$ (where $D$ is the
      number of active doers) instead of $O (\log N)$ (total schedules), drastically reducing lock contention on `thisProvider`.
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

* **CooperativeLocalPollingSchedulerDp (Local Polling)**:
    * **Workload**: Like `Contained`, it is suited for general-use cooperative scheduling. Unlike `Contained`, it is optimal when timer cancellations are rare or memory footprint from canceled timers is not a constraint.
    * **Pros**: Like `Contained` (and unlike `Sharded`, `Flat`, and `Hierarchical`), it partitions priority heaps thread-locally per worker to eliminate global lock contention. Like `Contained`, it avoids cross-thread queue synchronization
      by scheduling directly on the worker thread currently executing the doer.
    * **Cons**: Unlike `Contained` (and `Sharded`, `Flat`, `Hierarchical`, and `ThreadDriven`), cancellation is always lazy; canceled schedules remain in the priority queue until their timer expires. Unlike `Sharded`, worker queues are
      populated dynamically, which can lead to queue-load drift across workers.
    * **Comparison**: Like `Contained`, it maintains thread-local priority queues per worker to eliminate global lock contention. Unlike `Contained`, it does not restrict doers to a single worker's scheduling queue, which allows doer
      schedules to drift across workers and necessitates lazy memory reclamation for canceled timers.
* **CooperativeContainedPollingSchedulerDp (Contained Polling)**:
    * **Workload**: Like `Local`, it is suited for general-use cooperative scheduling. Unlike `Local`, it is optimal when timers are frequently canceled and memory must be reclaimed immediately.
    * **Pros**: Like `Local` (and unlike `Sharded`, `Flat`, and `Hierarchical`), it partitions priority heaps thread-locally per worker to eliminate global lock contention. Like `Local`, it avoids cross-thread queue synchronization by
      scheduling directly on the worker thread currently executing the doer. Unlike `Local`, it reclaims memory from canceled schedules immediately ($O (1)$ cleanup) when canceled on the worker executing the parent doer.
    * **Cons**: Like `Local`, worker queues are populated dynamically, which can lead to queue-load drift across workers. Like `Local`, scheduling from non-worker threads requires delegating the operation through the doer's execution queue.
    * **Comparison**: Like `Local`, it partitions scheduling heaps per worker thread and uses identical internal delegation for scheduling from non-worker threads. Unlike `Local`, cancellation immediately removes the schedule from the
      priority queue if called from the worker thread that owns it.
* **CooperativeShardedPollingSchedulerDp (Sharded Polling)**:
    * **Workload**: Unlike `Local` and `Contained`, it is suited for workloads where queue-load drift must be avoided and scheduling balance across workers is critical.
    * **Pros**: Like `Local` and `Contained`, it partitions priority heaps per worker thread to avoid global queue lock contention. Unlike `Local` and `Contained`, it uses static hash partitioning of schedules across worker queues
      (`hashCode % threadPoolSize`) to guarantee perfect load balancing.
    * **Cons**: Unlike `Local` and `Contained`, it requires synchronization locks on the worker's monitor for scheduling, canceling, and polling. Unlike `Contained`, canceled timers are reclaimed lazily when they expire.
    * **Comparison**: Like `Local` and `Contained`, it partitions scheduling queues across worker threads to avoid a single global bottleneck. Unlike `Local` and `Contained`, it enforces strict queue assignment via hashing and requires
      synchronization locks to access worker queues.
* **CooperativeFlatPollingSchedulerDp (Flat Polling)**:
    * **Workload**: Unlike `Hierarchical`, it is suited for workloads with very small pool size (<=2), low schedules cancellation percentage, and both: low schedule density (few active timers overall) or sparse timers (at most one schedule
      per doer).
    * **Pros**: Unlike `Hierarchical`, it uses a single flat priority queue rather than nesting doer-local heaps, resulting in lower constant overhead and simpler lock structure (no nested doer-local locks in `program` or `cancel`).
    * **Cons**: Like `Hierarchical` (and unlike `Local`, `Contained`, and `Sharded`), it coordinates all schedules under a single global provider lock, causing lock contention under high timer counts.
    * **Comparison**: Like `Hierarchical`, it coordinates all schedules under a single global provider lock. Unlike `Hierarchical`, it maintains a single flat priority queue rather than nesting doer-local timer heaps.
* **CooperativeHierarchicalPollingSchedulerDp (Hierarchical Polling)**:
    * **Workload**: Unlike `Flat`, it is suited for workloads with high timer density per doer (many concurrent active timers grouped under each active doer).
    * **Pros**: Unlike `Flat`, it decouples global queue size into a two-level queue structure (global queue of active doers, local queue of schedules per doer) to reduce global heap operations.
    * **Cons**: Like `Flat` (and unlike `Local`, `Contained`, and `Sharded`), it coordinates all schedules under a single global provider lock. Unlike `Flat`, it introduces nested lock complexity (locks both `owner` and `thisProvider`
      during schedule modification).
    * **Comparison**: Like `Flat`, it manages timers using centralized global coordination. Unlike `Flat`, it delegates timer detail ordering to doer-local priority heaps to reduce global heap operations.
* **CooperativeThreadDrivenSchedulerDp (Thread-Driven)**:
    * **Workload**: Unlike all other cooperative schedulers, it is suited for workloads where timer management overhead must be completely offloaded from execution worker threads.
    * **Pros**: Unlike all other cooperative schedulers, it offloads timer management and worker wakeup triggers to a dedicated background scheduler thread.
    * **Cons**: Unlike all other cooperative schedulers, it requires an additional active thread, introducing thread coordination and context switching overhead.
    * **Comparison**: Like `Flat` and `Hierarchical`, it manages timers in a centralized structure. Unlike all other cooperative schedulers, it uses a dedicated background thread to trigger wakeups rather than cooperatively executing timer
      evaluations within the worker pool.
* **StandardSchedulingDp (Dedicated Thread)**:
    * **Workload**: Unlike all other schedulers, it is suited for testing or extremely small-scale isolation where cooperative execution is not desired.
    * **Pros**: Unlike all other schedulers, every doer has its own private `ScheduledExecutorService` (1 thread per doer), eliminating scheduling interference or global queue locks.
    * **Cons**: Unlike all other schedulers, it has extremely high thread overhead and does not scale to large numbers of doers.
    * **Comparison**: Unlike all other schedulers, it does not use a shared cooperative worker pool, allocating a private thread per doer instead.

### 2. General Executors (Asynchronous task execution only)

* **CooperativeWorkersDp (Default Cooperative)**:
    * **Workload**: Standard asynchronous task execution without scheduling or custom priority constraints.
    * **Pros**: Highly efficient shared cooperative thread pool with $O (1)$ lock-free task polling.
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

## Testing Architecture Guidelines

* **Modular Trait-Based Test Hierarchy**: Test cases are partitioned into reusable, capability-focused traits matching `Doer` and `DoerProvider` abstractions:
    - `VanillaDoerTests`: Standard `Doer` invariant tests.
    - `MonoTests`: Standard `Task` and `Captor` invariants, factory, and operator tests.
    - `FluxTests`: `FluxExtension` stream factory and operator tests.
    - `ScheduledMonoTests`: Single-shot `SchedulingExtension` delay tests.
    - `ScheduledFluxDoerTests`: Multi-shot`ScheduledFluxExtension` periodic push stream tests.
    - `LoopingDoerTests`: `LoopingExtension` iterative combinator tests.
    - `CausalFenceTests`: `CausalFence` invariants tests.
    - `ResultIncrementalCoalescingDoerTests`: `ResultIncrementalCoalescing` convergence, superseding, and yielding invariant tests.
    - `CooperativeWorkersChildTests`: `CooperativeWorkersDp` thread-pool lifecycle, pending runnable tracking, and worker sleep/wakeup race condition tests.
* **Abstract Harness (`DoerProviderTestBase`)**: Manages suite lifecycle, logging (`ScribeConfig`), `unhandledExceptionObserver` tracking, and reusable test harness helpers. Property sample generation (`forAllTaskOperandExceptions`,
  `forAllSubscribeExceptions`) and assertion evaluation (`checkTaskOperandExceptionHandling`, `checkMonoObserverExceptionNotCaught`) are centralized in `DoerProviderTestBase`, while feature traits define their specific test cases cleanly
  without boilerplate.
* **Unified Concrete Provider Suites**: Concrete `DoerProvider` implementations extend `DoerProviderTestBase` and mix in only the capability traits matching their supported extensions. This enables a single concrete test suite per provider
  while running 100% of applicable extension test cases.
* **Exception Suppression (`TestThreadFactory`)**: When testing worker-pool providers (`CooperativeWorkersDp` and subclasses), pass `threadFactory = new TestThreadFactory` in the provider constructor. `TestThreadFactory` sets a custom
  `UncaughtExceptionHandler` on worker threads to suppress expected test exceptions (such as `FaultyValue` and `"Simulated..."`) from printing stack traces to stderr, while allowing unexpected environment failures to propagate.

## Implementation Guidelines

* **Zero-Allocation Pipelines**: Monadic combinators on `Capture` are implemented using inline custom anonymous classes extending `Subscription` with `MonoObserver` (or `AbstractTask`) directly, bypassing intermediate wrapping steps.
  Combinators returning a `Capture` are implemented via lightweight anonymous subclasses extending `DefaultCapture[B] with MonoObserver[A]`, leveraging `captureSync` and `trapSync` to handle state propagation with zero intermediate
  allocations.
* **Testing Exception Suppression**: When testing thread pool execution components (e.g., `CooperativeFlatPollingSchedulerDp`), unhandled exceptions thrown by asynchronous tasks terminate worker threads and propagate to the default uncaught
  exception handler (printing to stderr). To keep build logs clean, pass a custom `ThreadFactory` that intercepts the thread's uncaught exception handler to suppress simulated/expected test exceptions (such as `FaultyValue` and
  test-generated random throwables) while preserving printing of unexpected test environment bugs. Modifying the signature of `onUnhandledException` (e.g., to return a boolean indicating suppression status) should be avoided because it
  breaks binary/source compatibility across many provider subclasses and mixes log reporting with thread-pool lifecycle/recovery responsibilities.
* **Sleep-Time Tracking Toggle**: Cooperative worker-pool based providers (`CooperativeWorkersDp` and subclasses) accept a `trackSleepTime: Boolean` constructor parameter. When set to `false` (default), worker threads bypass high-frequency
  `System.nanoTime()` measurements and atomic/volatile metrics updates during worker loop sleep transitions. This eliminates telemetry-related CPU and system timer overhead in latency-sensitive production environments.
* **ThreadDrivenScheduler Component**: The `ThreadDrivenScheduler` manages task scheduling and periodic/one-shot executions using a dedicated background thread and an array-based binary min-heap, while executing task runnables cooperatively
  on the worker thread pool.

## Scheduling DoerProvider Benchmark Results

The following tables show empirical throughput measurements (operations per second) for each scheduler under various target active schedules densities, cancellation fractions, and scheduled-to-regular task ratios. All benchmarks were
executed with a thread pool size of 8. Note that, given the benchmark cancels schedules immediately within the same doer with which it created it, the `contained` variant is heavily favored. Otherwise, its throughput would be very slightly
slower than the `local` variant.

### Active schedules per Doer: 0.1 | Canceled percentage: 10%

| Provider     | Sched/Regul= 0.1 (300.0k doers) | Sched/Regul= 1.0 (300.0k doers) | Sched/Regul= 10.0 (500.0k doers) |
|:-------------|:--------------------------------|:--------------------------------|:---------------------------------|
| Local        | 17.5M                           | 7.45M                           | 4.97M                            |
| Contained    | 19.6M                           | 9.38M                           | 8.14M                            |
| Sharded      | 20.3M                           | 9.27M                           | 6.69M                            |
| Flat         | 8.04M                           | 1.74M                           | 0.896M                           |
| Hierarchical | 8.53M                           | 1.86M                           | 0.921M                           |
| ThreadDriven | 6.97M                           | 0.889M                          | 0.371M                           |

### Active schedules per Doer: 1.0 | Canceled percentage: 10%

| Provider     | Sched/Regul= 0.1 (30.0k doers) | Sched/Regul= 1.0 (30.0k doers) | Sched/Regul= 10.0 (50.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 18.1M                          | 6.03M                          | 5.13M                           |
| Contained    | 18.7M                          | 7.74M                          | 7.80M                           |
| Sharded      | 20.0M                          | 6.22M                          | 6.61M                           |
| Flat         | 8.00M                          | 1.48M                          | 0.918M                          |
| Hierarchical | 8.65M                          | 1.57M                          | 0.888M                          |
| ThreadDriven | 7.05M                          | 0.749M                         | 0.369M                          |

### Active schedules per Doer: 10.0 | Canceled percentage: 10%

| Provider     | Sched/Regul= 0.1 ( 3.0k doers) | Sched/Regul= 1.0 ( 3.0k doers) | Sched/Regul= 10.0 ( 5.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 28.0M                          | 11.7M                          | 7.19M                           |
| Contained    | 33.8M                          | 18.0M                          | 3.94M                           |
| Sharded      | 29.5M                          | 13.9M                          | 8.61M                           |
| Flat         | 11.0M                          | 1.83M                          | 1.03M                           |
| Hierarchical | 27.2M                          | 9.96M                          | 5.20M                           |
| ThreadDriven | 6.67M                          | 0.807M                         | 0.376M                          |

### Active schedules per Doer: 0.1 | Canceled percentage: 50%

| Provider     | Sched/Regul= 0.1 (300.0k doers) | Sched/Regul= 1.0 (300.0k doers) | Sched/Regul= 10.0 (300.0k doers) |
|:-------------|:--------------------------------|:--------------------------------|:---------------------------------|
| Local        | 20.5M                           | 6.16M                           | 4.44M                            |
| Contained    | 27.6M                           | 11.3M                           | 10.2M                            |
| Sharded      | 24.0M                           | 9.74M                           | 7.76M                            |
| Flat         | 9.79M                           | 2.17M                           | 1.36M                            |
| Hierarchical | 11.3M                           | 2.71M                           | 1.68M                            |
| ThreadDriven | 15.9M                           | 1.61M                           | 0.678M                           |

### Active schedules per Doer: 1.0 | Canceled percentage: 50%

| Provider     | Sched/Regul= 0.1 (30.0k doers) | Sched/Regul= 1.0 (30.0k doers) | Sched/Regul= 10.0 (30.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 23.6M                          | 6.15M                          | 4.47M                           |
| Contained    | 26.2M                          | 10.6M                          | 6.15M                           |
| Sharded      | 24.9M                          | 10.1M                          | 7.59M                           |
| Flat         | 9.04M                          | 2.46M                          | 1.24M                           |
| Hierarchical | 11.6M                          | 2.70M                          | 1.65M                           |
| ThreadDriven | 15.9M                          | 1.83M                          | 0.668M                          |

### Active schedules per Doer: 10.0 | Canceled percentage: 50%

| Provider     | Sched/Regul= 0.1 ( 3.0k doers) | Sched/Regul= 1.0 ( 3.0k doers) | Sched/Regul= 10.0 ( 3.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 16.9M                          | 9.07M                          | 5.90M                           |
| Contained    | 36.5M                          | 25.7M                          | 16.2M                           |
| Sharded      | 31.1M                          | 14.0M                          | 8.88M                           |
| Flat         | 13.1M                          | 3.63M                          | 1.72M                           |
| Hierarchical | 33.0M                          | 10.1M                          | 7.35M                           |
| ThreadDriven | 22.4M                          | 1.68M                          | 0.740M                          |

### Active schedules per Doer: 0.1 | Canceled percentage: 90%

| Provider     | Sched/Regul= 0.1 (250.0k doers) | Sched/Regul= 1.0 (300.0k doers) | Sched/Regul= 10.0 (300.0k doers) |
|:-------------|:--------------------------------|:--------------------------------|:---------------------------------|
| Local        | 27.2M                           | 8.42M                           | 6.44M                            |
| Contained    | 42.4M                           | 23.7M                           | 21.3M                            |
| Sharded      | 34.6M                           | 13.0M                           | 8.92M                            |
| Flat         | 18.7M                           | 4.88M                           | 2.83M                            |
| Hierarchical | 27.3M                           | 7.42M                           | 3.69M                            |
| ThreadDriven | 25.5M                           | 6.92M                           | 4.43M                            |

### Active schedules per Doer: 1.0 | Canceled percentage: 90%

| Provider     | Sched/Regul= 0.1 (30.0k doers) | Sched/Regul= 1.0 (30.0k doers) | Sched/Regul= 10.0 (30.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 25.1M                          | 8.58M                          | 5.97M                           |
| Contained    | 38.5M                          | 22.2M                          | 20.4M                           |
| Sharded      | 29.8M                          | 12.3M                          | 8.36M                           |
| Flat         | 18.3M                          | 4.76M                          | 2.76M                           |
| Hierarchical | 28.0M                          | 8.46M                          | 3.94M                           |
| ThreadDriven | 24.6M                          | 7.37M                          | 4.16M                           |

### Active schedules per Doer: 10.0 | Canceled percentage: 90%

| Provider     | Sched/Regul= 0.1 ( 3.0k doers) | Sched/Regul= 1.0 ( 3.0k doers) | Sched/Regul= 10.0 ( 3.0k doers) |
|:-------------|:-------------------------------|:-------------------------------|:--------------------------------|
| Local        | 26.0M                          | 8.62M                          | 5.29M                           |
| Contained    | 34.6M                          | 33.2M                          | 27.4M                           |
| Sharded      | 36.5M                          | 13.8M                          | 9.28M                           |
| Flat         | 21.5M                          | 6.29M                          | 3.60M                           |
| Hierarchical | 35.5M                          | 9.55M                          | 5.37M                           |
| ThreadDriven | 27.9M                          | 8.05M                          | 4.71M                           |

## Flux Testing Infrastructure

`FluxExtension` defines push-based multi-element stream primitives (`Flux[A]`), factory methods (`Flux_empty`, `Flux_apply`, `Flux_fromIterable`, `Flux_generate`, `Flux_fromMonos`, `StreamEmitter`), transformations (`map`, `scan`, `buffer`,
`zip`, `take`, `takeWhile`, `foldWhile`), and matrix flatMap tensors (`flatMap`, `Tensor`).

The abstract `FluxDoerProviderTest[D <: Doer & FluxExtension]` suite provides a provider-agnostic harness ensuring all concrete `DoerProvider` implementations satisfy the invariants of `Flux` streams, execution scoping, cancellation, and
error handling. `GeneratorsForDoerTests` supports property-based test generation for `Flux` instances.
