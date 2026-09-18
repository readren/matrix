---
type: "ADR"
title: "CausalFence and the Decoupled Mutation Contract"
description: "Architectural decision and specification for safe, non-blocking sequential state mutation using CausalFence."
tags: [ "adr", "concurrency", "causalfence", "state-machine", "consensus" ]
timestamp: "2026-09-17T21:05:00Z"
---

# ADR: CausalFence and the Decoupled Mutation Contract

## Context

Implementing a deterministic consensus algorithm (Raft) requires strictly sequential state transitions. However, consensus nodes frequently perform asynchronous I/O (network replication, disk flushes, log compaction snapshots).

Traditional paradigms handle this poorly:

1. **Single-Threaded Event Loops & Actors**: Sequential by default, but async I/O forces the thread to block or the actor to "stash" incoming messages, leading to complex buffering, backpressure, and memory leaks.
2. **Mutexes (Lock-and-Release)**: Locking state, unlocking it to perform async I/O, and re-locking it introduces severe race conditions. By the time the thread re-locks, the state (e.g., the current `Term` or `Role`) may have fundamentally
   changed, resulting in "stale state poisoning" unless mitigated by pervasive defensive boilerplate.

## Decision

We introduced `CausalFence` as a non-blocking state mutex built around a promise queue. Instead of synchronizing threads or message inboxes, it strictly synchronizes the *state itself* (`PrimaryState`). This decouples the state's causal
timeline from the underlying execution engine (`Doer`), allowing the node to multiplex tasks while guaranteeing mathematically safe state transitions.

### 1. The Game-Changing Invariant & Temporal Window of Causal Safety

By attaching continuations synchronously to the `Capture` returned by `advance`, `advanceIf`, or `causalAnchor`, the framework mathematically guarantees execution ordering.

- The continuation is inherently scheduled in the exact causal timeline slot immediately following the anchored mutation.
- The continuation is guaranteed to receive the *fresh, correct state* corresponding to that specific moment in time.
- State references should not be passed to deferred or asynchronous boundaries without re-anchoring, as they instantly become stale once the initial execution block yields.

### 2. The Decoupled Mutation Contract (An Asymmetric Performance Decision)

While the `CausalFence` enforces the sequential timeline, the consensus algorithm specifically enforces an asymmetric **Decoupled Mutation Contract** across all internal reactive notification boundaries (such as `CommitIndexAwaiter`
fulfillment and state event observers).

To prevent stalling the timeline and avoid poisoning nested execution contexts, state mutations and their resulting side-effects must be strictly separated.

- **Synchronous Updates Only:** All writes to the protected state (e.g., updating the term, appending records, truncating the log) must occur strictly within the synchronous updater function passed to `advance`.
- **Decoupled Side-Effects:** Consequential side-effects (e.g., initiating log compaction, triggering replication pipelines, appending no-op records, or starting second-phase configuration changes) MUST NOT be executed synchronously inside
  the updater or synchronously by an observer. They must be decoupled, chained onto the returned `Capture`, deferred via `sequencer.Capture_defer`, or dispatched via `sequencer.run`.

#### Architectural Rationale: The Hot-Path Performance Asymmetry

This contract is an intentional, asymmetric performance optimization:

- **Zero-Cost Happy Path for Client Commands**: Over 99.999% of commit index advancements represent client command replication. On this hot path, committed commands are applied to the state machine, client responses are formed, and awaiters
  are resolved. Applying committed records to the state machine does not mutate the `PrimaryState` or touch the `CausalFence`. Because the contract guarantees that awaiters do not mutate the fence, `recalculateCommitIndex` can notify
  awaiters synchronously on the current thread stack. This eliminates task-queue allocations, trampoline hops, and dispatch latency, allowing the happy path to execute with absolute minimum overhead.
- **Asymmetric Burden on Mutating Callers**: In exchange for zero-overhead execution on the dominant hot path, callers that execute state-mutating logic bear the architectural burden of explicit self-deferral. Seldom-traveled paths—such as
  fallback branches of command replication (appending no-op records) and configuration change completions (transitioning from transitional to stable configurations)—must explicitly wrap their continuations in `Capture_defer`. This
  sacrifices caller simplicity on rare paths to maximize performance on the critical path.

#### Rejection of Alternative 1: Synchronous Inline Re-Anchoring

A naive alternative to the contract would be to permit observers to mutate the state synchronously, assuming the notifier could simply re-anchor after calling each observer. This alternative is structurally impossible:

- **The Asynchronous Promise Reality**: `CausalFence.causalAnchor()` is fundamentally an asynchronous primitive that returns a `Capture[PrimaryState]`, not a synchronous value. The moment an advance is enqueued onto the fence (such as an
  observer appending an entry to disk), the new state does not exist synchronously; it is an in-flight operation awaiting persistence or pipeline sequencing.
- **Inability to Resume Synchronously**: A synchronous loop (such as `recalculateCommitIndex` iterating over fulfilled awaiters) cannot block or inline-await an asynchronous promise. To pass a freshly anchored state to subsequent awaiters,
  the entire remainder of the loop would have to be severed and converted into an asynchronous callback chain (`awaiter.seizeWith(causalAnchor())`), destroying synchronous stack execution.

#### Rejection of Alternative 2: Dynamic Mutation Detection in Notifier Loops

Another tempting alternative is dynamic fallback detection: having `recalculateCommitIndex` notify synchronously by default, but dynamically check if the fence was modified
(`!primaryStateFence.committedState.is(primaryState0) || !primaryStateFence.isEmpty`), re-anchoring remaining awaiters only when a mutation occurs. This alternative was formally rejected due to two fatal concurrency hazards:

- **Intra-Batch Turn Splitting (Execution Reordering)**: If a commit index advancement satisfies multiple awaiters simultaneously (e.g., a configuration change and subsequent client commands), and an early awaiter mutates the fence,
  dynamically re-anchoring subsequent awaiters via `seizeWith(causalAnchor())` defers them to future sequencer turns. Consequently, awaiters that achieved consensus within the exact same batch are arbitrarily split across different
  execution frames, violating linear dispatch expectations.
- **Stack Re-Entrancy and State Invalidation**: Allowing an observer to mutate the fence synchronously executes arbitrary state mutation logic while `recalculateCommitIndex` is actively running on the call stack. The observer's callback
  could alter roles (triggering abdication or follower transitions), schedule pipeline drives, or mutate the pending awaiter collection while the outer loop is still traversing it.

#### The Resulting Purity Invariant

By enforcing the Decoupled Mutation Contract, `recalculateCommitIndex` maintains the strict assertion:
`assert(primaryStateFence.committedState.is(primaryState0))`
This mathematical invariant proves that commit index calculation and notification is strictly pure with respect to the causal state. It guarantees that a valid `PrimaryState` reference remains pristine and temporally safe from the beginning
of the notification loop to the end, eliminating re-entrancy bugs, stale-state references, and batch turn-splitting.

## Consequential Designs

### Allocation-Free, Re-entrancy-Safe Awaiters

Because the Decoupled Mutation Contract guarantees that `causalAnchor` subscribers do not synchronously trigger cascading state mutations, we can optimize internal pub/sub mechanics (like `CommitIndexAwaiter`s) to run entirely synchronous,
zero-allocation algorithms.

The awaiter fulfillment algorithm (`recalculateCommitIndex`) uses an **In-Place Partition & Reverse-Execution** pattern:

1. **In-place Partition:** Fulfilled awaiters are swapped to the end of the buffer (from `partitionIdx` to the end), avoiding the allocation of temporary `List`s or `Array`s.
2. **Reverse Execution:** The buffer is iterated backwards (`length - 1` down to `partitionIdx`). For each element, `remove(j)` is called, followed by execution of the awaiter's captor.
    - *Safety against Appends:* If an observer synchronously appends a new awaiter, it is placed at the end of the buffer (index `> j`), and the `remove(j)` safely shifts it left without disrupting the current iteration.
    - *Safety against Clears:* If an observer triggers a shutdown (clearing the buffer), the `j < length` check safely aborts the loop, avoiding `IndexOutOfBoundsException`.
3. **No Rescan Loops Required:** Because all mutating observers are decoupled via the contract, notifying an observer mathematically cannot synchronously advance the commit index. Therefore, an outer "rescan" loop to catch cascaded
   fulfillments is obsolete.
