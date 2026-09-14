---
type: "ADR"
title: "CausalFence and the Decoupled Mutation Contract"
description: "Architectural decision and specification for safe, non-blocking sequential state mutation using CausalFence."
tags: [ "adr", "concurrency", "causalfence", "state-machine", "consensus" ]
timestamp: "2026-09-09T00:22:00Z"
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

## The Invariants

### 1. The Game-Changing Invariant

By attaching continuations synchronously to the `Capture` returned by `advance`, `advanceIf`, or `causalAnchor`, the framework mathematically guarantees execution ordering.

- The continuation is inherently scheduled in the exact causal timeline slot immediately following the anchored mutation.
- The continuation is guaranteed to receive the *fresh, correct state* corresponding to that specific moment in time.
- State references should not be passed to deferred or asynchronous boundaries without re-anchoring, as they instantly become stale once the initial execution block yields.

### 2. The Decoupled Mutation Contract (Design Decision)

While the `CausalFence` enforces the timeline, the consensus algorithm specifically enforces a **Decoupled Mutation Contract** across its internal components (such as observer notifications and `Capture` fulfillments).

To prevent stalling the timeline and avoid poisoning nested execution contexts, state mutations and their resulting side-effects must be strictly separated.

- **Synchronous Updates Only:** All writes to the protected state (e.g., updating the term, appending records, truncating the log) must occur strictly within the synchronous updater function passed to `advance`.
- **Decoupled Side-Effects:** Consequential side-effects (e.g., initiating log compaction, triggering replication pipelines) MUST NOT be executed synchronously inside the updater or synchronously by an observer. They must be decoupled,
  chained onto the returned `Capture`, or dispatched via `sequencer.run`.

**Alternative Considered & Rationale:**
An alternative would be to permit observers (e.g., subscribers to `Captor.captureSync`) to mutate the `PrimaryState` synchronously. However, if this were allowed, the caller invoking the observer would have to defensively assume its local
`PrimaryState` reference was instantly poisoned. It would be forced to execute a `causalAnchor()` to fetch the fresh state before proceeding to the next line of code. By adopting the Decoupled Mutation Contract, the consensus layer entirely
avoids the heavy performance overhead and syntactic boilerplate of constantly re-anchoring state references.

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
