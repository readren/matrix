---
type: "Decision"
title: "Simplify Sequencer Error Handling and Unify Exception Routing"
description: "Decision to remove generic exception guarding (isGuarded) and simplify unhandled error routing by removing onFailureReported and reportFailure."
tags: ["design-history", "sequencer"]
timestamp: "2026-07-09T14:30:00Z"
---

# ADR: Simplify Sequencer Error Handling and Unify Exception Routing

## Context

Previously, the `sequencer/core` module supported a complex, parallel error handling mechanism:

1. **Unhandled Exceptions (`onUnhandledException`)**: An exception thrown inside a `Runnable` passed to `executeSequentially` escapes the runnable, crashing the worker thread. The `DoerProvider` catches this, reports it, and restarts the
   worker thread.
2. **Panic / Reported Failures (`onFailureReported`)**: If an exception is thrown in a "guarded" side-effecting task operation (like `andThen` or `foreach`), or if a `Future` callback fails on `ownSingleThreadExecutionContext`, the
   exception is caught, wrapped in a `PanicException`, and reported to `Doer.reportFailure` (delegating to `onFailureReported`). The pipeline then continues with the rest of the flow without crashing or restarting the worker thread.

This split had several disadvantages:

* **Silent Failures vs. Fail-Fast**: Sweeping exceptions via generic `isGuarded` flags allows pipelines to continue executing under broken assumptions, leading to silent state corruption.
* **Inconsistent Behavior**: Exceptions thrown in direct task execution and Future callbacks behaved completely differently at the thread level (one crashed/restarted the worker, while the other was swallowed and reported as a panic).
* **Code Complexity**: The implementation required complex macros (`reportPanicExceptionImpl`), `PanicException` wrapping, and redundant `onFailureReported` hooks on all `DoerProvider` implementations.

## Options Considered

1. **Option 1: Keep Guarded Options & `reportFailure` / `PanicException`**
    - Keeps the existing split behavior. Allows side-effects to fail gracefully without crashing worker threads.
    - *Verdict*: Rejected because it encourages silent failures (violating fail-fast) and retains parallel error-reporting paths.

2. **Option 2: Unified Fail-Fast & Rethrow (Chosen)**
    - Remove `isGuarded` and guarded side-effecting operations (like `andThen`/`foreach`). Any exception in these operations is considered unexpected and halts the execution pipeline.
    - Delete `Doer.reportFailure`, `PanicException`, and `reportPanicException`.
    - Delete `DoerProvider.onFailureReported`.
    - Make `ownSingleThreadExecutionContext.reportFailure` throw the exception directly. Since it escapes `Promise$Transformation.run`, it crashes the worker thread, triggering a standard `onUnhandledException` and worker restart.
    - *Verdict*: Accepted. It dramatically simplifies the library, establishes perfect behavioral consistency, and guarantees robust fail-fast semantics.

## Consequences

* **Simpler Library Code**: Deletes macro-driven panic reporting and redundant provider callbacks.
* **Consistent Thread Invariants**: Every uncaught exception now triggers `onUnhandledException` and restarts the worker thread, whether it originates in a direct task runnable or a `Future` callback.
* **Explicit User Responsibility**: Developers must handle expected exceptions explicitly (e.g. using `try-catch`, `recover`, or custom extensions) rather than relying on a generic `isGuarded` parameter.
