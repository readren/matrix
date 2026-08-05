---
type: "Decision"
title: "Design of Scheduling and Timing Extensions"
description: "Analysis of exposing Schedule instances and split semantics in Scheduling and Timing Extensions."
tags: ["design-history", "sequencer", "scheduling"]
timestamp: "2026-07-01T20:16:00Z"
---

# Design of Scheduling and Timing Extensions

## Context

In `SchedulingExtension.scala`, task subscriptions that create a `Schedule` instance internally (e.g., `scheduled`, `delayed`, `timeLimited`, `Task_schedules`) hide the underlying `Schedule` instance from both the subscriber and the
supplier functions. This limits control and status query operations like `wasActivated`, `isCanceled`, or custom cancellation.

A TODO in the codebase proposes:
> Define `trait TimedTask[A] extends Task[A]` whose `subscribeSync` method returns a `Subscription & Schedule` type.

Additionally, comments on supplier methods suggest changing `supplier: Schedule => A` to `supplier: Schedule & Subscription => A`.

## Alternatives & Pitfalls

### Option 1: Inheritance from Schedule (via Intersection `Subscription & Schedule` or Trait Extension)

- **Concept**: Require that the returned subscription handle inherits from both `Subscription` and `Schedule`, either via a Scala 3 intersection type `Subscription & Schedule` or a named trait
  `trait TimedSubscription extends Subscription with Schedule`.
- **Pitfalls**:
    1. **Multiple Inheritance Constraints**: On the JVM/Scala, a class can inherit from at most one class. In concrete implementations (e.g., `ActorBasedSchedulingDoer` defining `Schedule = Plan` and `StandardSchedulingDp` defining
       `Schedule = TSchedule`), `Schedule` is represented by concrete or abstract classes. Forcing the subscription class (or a named trait implementing both) to extend `Schedule` leads to multiple-inheritance compile errors if the
       subscription or other traits have class dependencies.
    2. **Delegation / State Synchronization Pitfalls**:
       `Mono_Scheduled.subscribeSync` instantiates a schedule via `buildSchedule`. To return a `Subscription & Schedule` that wraps the created schedule, the wrapper subclass must override all methods to delegate to the wrapped instance.
       However, if `Schedule` implementations use direct package-private field access or pattern matching (e.g. `TSchedule`'s `canceled` field or `SingleTime` / `FixedRate` case classes), passing the subclass wrapper to those methods will
       read or write fields of the wrapper instead of the wrapped schedule instance. This breaks internal invariants and state synchronization.

### Option 2: `TimedSubscription` Trait (Recommended Alternative)

- **Concept**: Define a new trait extending `Subscription` that explicitly exposes the schedule:
  ```scala
  trait TimedSubscription extends Subscription {
    def schedule: Schedule
  }
  ```
  And define a specialized task trait:
  ```scala
  trait TimedTask[+A] extends Task[A] {
    override def subscribeSync(downChainObserver: MonoObserver[A]): TimedSubscription
  }
  ```
- **Advantages**:
    1. **Separation of Concerns**: Avoids conflating a subscription (control handle for a stream connection) with a schedule (timing configuration and state).
    2. **Ease of Implementation**: Subscription wrappers only need to implement the `TimedSubscription` trait and return the schedule reference via `def schedule`. This works perfectly regardless of whether `Schedule` is a trait, concrete
       class, final, or opaque type.
    3. **No Delegation Pitfalls**: The original `Schedule` instance is returned as-is, ensuring that direct field access, pattern matching, and state mutations on the schedule remain fully correct.
    4. **Clean API**: Suppliers in factory methods like `Task_schedules` can take `TimedSubscription => A`, giving them full access to both `unsubscribe()` and the underlying `schedule`.

### Redundancy of `ScheduleBase`

It might seem tempting to define a global `ScheduleBase` trait for `Schedule` to inherit from, and have `TimedSubscription` return `ScheduleBase`. However, this is unnecessary and reduces type safety:

1. **Direct Path-Dependent Typing**: Since `TimedSubscription` is defined inside [SchedulingExtension](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala) (which has a self-type/extends `Doer`), it can
   directly reference the path-dependent type member `Schedule`. No type projection or common base trait is needed to make the return type compilation work.
2. **Loss of Strict Path Checking**: Using a global non-path-dependent `ScheduleBase` would weaken type safety. The compiler would no longer enforce that a schedule belongs to the specific `Doer` instance on which operations (like `cancel`
   or `isCanceled`) are being called. Relying on the path-dependent `Schedule` type member keeps these checks strong.

## Target API: Split Extension Model (Task vs. Capturer)

We define specific scheduling extension behaviors for both `Task` and `Capturer` to preserve their native lifecycle and evaluation semantics:

### 1. Reusable `Task` Semantics (Lazy Timer Start)

For [Task](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/Doer.scala) instances, the timer starts only when the task is **subscribed to**. This preserves the lazy execution invariants of `Task`:

- `delayed`: Delays execution of the task after subscription.
- `timeLimited`: Limits execution time starting from subscription.
- `scheduled`: Runs the task repeatedly according to a schedule.
- `retriedOnTimeout`: Retries the task execution upon timeout.

These return `TimedTask` (or `Task` for retries) and are defined via:

```scala
extension [A](thisTask: Task[A]) {
  inline def delayed(delay: MilliDuration): TimedTask[A] = ...
  inline def timeLimited(limit: MilliDuration): TimedTask[Maybe[A]] = ...
  inline def scheduled(kind: ScheduleKind, initialDelay: MilliDuration, loopDelay: MilliDuration): TimedTask[A] = ...
  def retriedOnTimeout(limit: MilliDuration, maxRetries: Int): Task[Maybe[A]] = ...
}
```

### 2. Caching `Capturer` Semantics (Immediate/Hot Timer Start)

For [Capturer](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/Doer.scala) (which represents a single-run caching computation), the timer starts **immediately on operation call** (when the method is called). Since a cached
task completes at most once, periodic schedules are semantically invalid.

To enforce this at compile time, we introduce a new abstract type member `Delay <: Schedule` representing single-shot delay timers. `newDelaySchedule` returns `Delay`, and `Capturer` operations accept `Delay` instead of the general
`Schedule` type:

- `delayed`: Subscribes to the underlying latching task after a delay determined by the provided delay schedule.
- `timeLimited`: Enforces a timeout determined by the provided delay schedule.

These return `Capturer` and are defined via:

```scala
extension [A](thisLatchingTask: Capturer[A]) {
  inline def delayed(schedule: Delay): Capturer[A] = ...
  inline def timeLimited(timer: Delay): Capturer[Maybe[A]] = ...
}
```

They are backed by dedicated `LatchingTask_Delayed` and `LatchingTask_TimeLimited` classes extending `DefaultCaptor`.

### 3. Immediate Subscription Hooks

To allow safe access to the underlying `Schedule` before the task completes (e.g. for logging, custom cancellation, or pre-trigger status checks), `TimedTask` exposes a side-effect hook `.onSubscription`:

```scala
inline final def onSubscription(inline action: Schedule => Unit): TimedTask[A]
```

This hook is executed synchronously during the `subscribeSync` call on the sequencer thread immediately after the schedule is created, but **before** it can trigger execution. To prevent resource leaks if the user-defined `action` fails,
the implementation catches any `NonFatal` exception, calls `unsubscribe()` to cancel the active schedule, and propagates the error.





