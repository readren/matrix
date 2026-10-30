---
type: "Decision"
title: "Design of Scheduling and Timing Extensions"
description: "Analysis of exposing Schedule instances and split semantics in Scheduling and Timing Extensions."
tags: ["design-history", "sequencer", "scheduling"]
timestamp: "2026-10-29T21:25:00Z"
---

# Design of Scheduling and Timing Extensions

## Context

In `SchedulingExtension.scala`, task subscriptions that create a `Schedule` instance internally (e.g., `scheduled`, `delayed`, `timeLimited`, `Task_schedules`) hide the underlying `Schedule` instance from both the subscriber and the
supplier functions. This limits control and status query operations like `wasActivated`, `isCanceled`, or custom cancellation.

A proposal in the codebase suggested defining a `TimedTask` trait whose subscription handle exposes both stream subscription control and timing configuration.

Additionally, comments on supplier methods suggested enabling suppliers to interact directly with the underlying schedule handle.

## Alternatives & Pitfalls

### Option 1: Inheritance from Schedule (via Intersection `Subscription & Schedule` or Trait Extension)

- **Concept:** Require that the returned subscription handle inherits from both `Subscription` and `Schedule`, either via a Scala 3 intersection type `Subscription & Schedule` or a named trait
  `trait TimedSubscription extends Subscription with Schedule`.
- **Pitfalls:**
    1. **Multiple Inheritance Constraints:** On the JVM and Scala, a class can inherit from at most one class. In concrete implementations (e.g., `ActorBasedSchedulingDoer` defining `Schedule = Plan` and `StandardSchedulingDp` defining
       `Schedule = TSchedule`), `Schedule` is represented by concrete or abstract classes. Forcing the subscription class (or a named trait implementing both) to extend `Schedule` leads to multiple-inheritance compile errors if the
       subscription or other traits have class dependencies.
    2. **Delegation and State Synchronization Pitfalls:** If `Schedule` implementations use direct package-private field access or pattern matching (e.g., `TSchedule`'s canceled status or `SingleTime` / `FixedRate` cases), subclass
       delegation wrappers can fail to synchronize state correctly across the wrapper and the wrapped schedule instance.

### Option 2: `TimedSubscription` Trait (Chosen Architecture)

- **Concept:** Define a new trait extending `Subscription` that explicitly exposes the schedule:
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
- **Advantages:**
    1. **Separation of Concerns:** Avoids conflating a subscription (control handle for a stream connection) with a schedule (timing configuration and state).
    2. **Ease of Implementation:** Subscription wrappers only need to implement the `TimedSubscription` trait and return the schedule reference via `def schedule`. This works regardless of whether `Schedule` is a trait, concrete class, or
       opaque type.
    3. **No Delegation Pitfalls:** The original `Schedule` instance is returned directly, ensuring that pattern matching and state mutations on the schedule remain fully correct.
    4. **Clean API:** Suppliers in factory methods like `Task_schedules` can take `TimedSubscription => A`, giving them full access to both `unsubscribe()` and the underlying `schedule`.

### Redundancy of `ScheduleBase`

Defining a global `ScheduleBase` trait for `Schedule` to inherit from is unnecessary and reduces type safety:

1. **Direct Path-Dependent Typing:** Since `TimedSubscription` is defined inside `SchedulingExtension` (which extends `Doer`), it directly references the path-dependent type member `Schedule`. No type projection or common base trait is
   needed.
2. **Loss of Strict Path Checking:** Using a global non-path-dependent `ScheduleBase` would weaken type safety. The compiler would no longer enforce that a schedule belongs to the specific `Doer` instance on which operations (like `cancel`
   or `isCanceled`) are being called. Relying on the path-dependent `Schedule` type member preserves strict path checking.

## Target API: Split Extension Model (Task vs. Capture)

We define specific scheduling extension behaviors for both `Task` and `Capture` to preserve their native lifecycle and evaluation semantics:

### 1. Reusable `Task` Semantics (Lazy Timer Start)

For [`Task`](sequencer-core.md) instances, the timer starts only when the task is **subscribed to**. This preserves the lazy execution invariants of `Task`:
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

### 2. Caching `Capture` Semantics (Immediate/Hot Timer Start)

For [`Capture`](sequencer-core.md) (which represents a single-run caching computation), the timer starts **immediately on operation call** (when the method is invoked). Since a cached task completes at most once, periodic schedules are
semantically invalid.

To enforce this at compile time, we introduce a dedicated abstract type member `Delay <: Schedule` representing single-shot delay timers. Single-shot operations accept `Delay` instead of the general `Schedule` type:
- `delayed`: Subscribes to the underlying latching task after a delay determined by the provided delay schedule.
- `timeLimited`: Enforces a timeout determined by the provided delay schedule.

These return `Capture` and are defined via:

```scala
extension [A](thisCapture: Capture[A]) {
  inline def delayed(schedule: Delay): Capture[A] = ...
  inline def timeLimited(timer: Delay): Capture[Maybe[A]] = ...
}
```

### 3. Immediate Subscription Hooks

To allow safe access to the underlying `Schedule` before the task completes (e.g. for logging, custom cancellation, or pre-trigger status checks), `TimedTask` exposes a side-effect hook `.onSubscription`:

```scala
inline final def onSubscription(inline action: Schedule => Unit): TimedTask[A]
```

This hook is executed synchronously during the `subscribeSync` call on the sequencer thread immediately after the schedule is created, but **before** it can trigger execution. To prevent resource leaks if the user-defined `action` fails,
the implementation catches any `NonFatal` exception, calls `unsubscribe()` to cancel the active schedule, and propagates the error.
