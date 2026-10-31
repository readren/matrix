---
type: "Decision"
title: "CausalFence and the Decoupled Mutation Contract"
description: "Architectural decision and specification for safe, non-blocking sequential state mutation using CausalFence."
tags: [ "design-history", "concurrency", "consensus", "sequencer" ]
timestamp: "2026-10-30T19:10:00Z"
---

# ADR: CausalFence and the Decoupled Mutation Contract

## Context

Implementing a deterministic consensus algorithm (Raft) in the [Lazy Raft Consensus Protocol Specification](lazy-multi-raft-consensus.md) requires strictly sequential state transitions. However, consensus nodes frequently perform
asynchronous I/O (network replication, disk flushes, log compaction snapshots).

Traditional paradigms handle this poorly:

1. **Single-Threaded Event Loops & Actors:** Sequential by default, but async I/O forces the thread to block or the actor to "stash" incoming messages, leading to complex buffering, backpressure, and memory leaks.
2. **Mutexes (Lock-and-Release):** Locking state, unlocking it to perform async I/O, and re-locking it introduces severe race conditions. By the time the thread re-locks, the state (e.g., the current `Term` or `Role`) may have fundamentally
   changed, resulting in "stale state poisoning" unless mitigated by pervasive defensive boilerplate.

## Decision

We introduced `CausalFence` as a non-blocking state mutex built around a promise queue. Instead of synchronizing threads or message inboxes, it strictly synchronizes the *state itself* (`PrimaryState`). This decouples the state's causal
timeline from the underlying execution engine ([`Doer`](sequencer-core.md)), allowing the node to multiplex tasks while guaranteeing mathematically safe state transitions in the [Multi-Raft System Architecture](system-architecture.md)
and [Lazy Raft Consensus Protocol Specification](lazy-multi-raft-consensus.md).

### 1. The Game-Changing Invariant & Temporal Window of Causal Safety

By attaching continuations synchronously to the `Capture` returned by `advance`, `advanceIf`, or `causalAnchor`, the framework mathematically guarantees execution ordering.
- The continuation is inherently scheduled in the exact causal timeline slot immediately following the anchored mutation.
- The continuation is guaranteed to receive the *fresh, correct state* corresponding to that specific moment in time.
- **Temporal Window of Causal Validity:** A state reference yielded by the fence or passed into an execution scope is temporally valid *only within the synchronous execution block directly attached to that operation*. Crossing an
  asynchronous boundary or suspension point (e.g., awaiting an external RPC, a timer, or an unanchored asynchronous effect) yields the execution context, allowing interleaved tasks to advance the fence. Upon resuming past an asynchronous
  suspension, all previously held state references are temporally invalidated and poisoned; execution must explicitly re-anchor via `causalAnchor()` before accessing state or driving downstream transitions.

### 2. The Decoupled Mutation Contract (An Asymmetric Performance Decision)

While the `CausalFence` enforces the sequential timeline, the consensus algorithm specifically enforces an asymmetric **Decoupled Mutation Contract** across all internal reactive notification boundaries (such as `CommitIndexAwaiter`
fulfillment and state event observers; see [Decoupled Mutation Contract & Causal State Synchronization](lazy-multi-raft-consensus.md#6-decoupled-mutation-contract--causal-state-synchronization)).

To prevent stalling the timeline and avoid poisoning nested execution contexts, state mutations and their resulting side-effects must be strictly separated.
- **Synchronous Updates Only:** All writes to the protected state (e.g., updating the term, appending records, truncating the log) must occur strictly within the synchronous updater function passed to `advance`.
- **Decoupled Side-Effects:** Consequential side-effects (e.g., initiating log compaction, triggering replication pipelines, appending no-op records, or starting second-phase electorate transitions) MUST NOT be executed synchronously inside
  the updater or synchronously by an observer. They must be decoupled, chained onto the returned `Capture`, deferred via `sequencer.Capture_defer`, or dispatched via `sequencer.run`.

#### Architectural Rationale: The Hot-Path Performance Asymmetry

This contract is an intentional, asymmetric performance optimization:

- **Zero-Cost Happy Path for Client Commands:** Over 99.999% of commit index advancements represent client command replication. On this hot path, committed commands are applied to the state machine, client responses are formed, and awaiters
  are resolved. Applying committed records to the state machine does not mutate the `PrimaryState` or touch the `CausalFence`. Because the contract guarantees that awaiters do not mutate the fence, `recalculateCommitIndex` can notify
  awaiters synchronously on the current thread stack. This eliminates task-queue allocations, trampoline hops, and dispatch latency, allowing the happy path to execute with absolute minimum overhead.
- **Asymmetric Burden on Mutating Callers:** In exchange for zero-overhead execution on the dominant hot path, callers that execute state-mutating logic bear the architectural burden of explicit self-deferral. Seldom-traveled paths—such as
  fallback branches of command replication (appending no-op records), electorate transition completions (transitioning from joint to sole electorates), and sequential electorate transition chaining—must explicitly wrap their continuations
  in `Capture_defer`.
- **Role Exit Decoupling & Re-Entrancy Prevention:** During role transitions (e.g., leader abdication), in-flight watermark awaiters must be resolved in a decoupled manner (e.g., via `sequencer.run`). Because observers react to abdication
  by delegating vacated commands to the incoming role—which synchronously triggers role evaluations and state fence mutations—resolving awaiters synchronously inside `handleExit` creates re-entrant state mutations on the caller's call stack
  (such as an enclosing `onAppendRecords` turn). Decoupling awaiter seizure ensures that role transitions and enclosing RPC turns complete atomically before vacated operations are processed.

#### Rejection of Alternative 1: Synchronous Inline Re-Anchoring

A naive alternative to the contract would be to permit observers to mutate the state synchronously, assuming the notifier could simply re-anchor after calling each observer. This alternative is structurally impossible:

- **The Asynchronous Promise Reality:** `CausalFence.causalAnchor()` is fundamentally an asynchronous primitive that returns a `Capture[PrimaryState]`, not a synchronous value. The moment an advance is enqueued onto the fence (such as an
  observer appending an entry to disk), the new state does not exist synchronously; it is an in-flight operation awaiting persistence or pipeline sequencing.
- **Inability to Resume Synchronously:** A synchronous loop (such as iterating over fulfilled watermark awaiters during commit advancement) cannot block or inline-await an asynchronous promise. To pass a freshly anchored state to subsequent
  awaiters,
  the entire remainder of the loop would have to be severed and converted into an asynchronous callback chain (`awaiter.seizeWith(causalAnchor())`), destroying synchronous stack execution.

#### Rejection of Alternative 2: Dynamic Mutation Detection in Notifier Loops

Another tempting alternative is dynamic fallback detection: having commit index advancement notify synchronously by default, but dynamically check if the fence was modified
(`!primaryStateFence.committedState.is(primaryState0) || !primaryStateFence.isEmpty`), re-anchoring remaining awaiters only when a mutation occurs. This alternative was formally rejected due to two fatal concurrency hazards:

- **Intra-Batch Turn Splitting (Execution Reordering):** If a commit index advancement satisfies multiple awaiters simultaneously (e.g., an electorate change and subsequent client commands), and an early awaiter mutates the fence,
  dynamically re-anchoring subsequent awaiters via `seizeWith(causalAnchor())` defers them to future sequencer turns. Consequently, awaiters that achieved consensus within the exact same batch are arbitrarily split across different
  execution frames, violating linear dispatch expectations.
- **Stack Re-Entrancy and State Invalidation:** Allowing an observer to mutate the fence synchronously executes arbitrary state mutation logic while commit index advancement is actively running on the call stack. The observer's callback
  could alter roles (triggering abdication or follower transitions), schedule pipeline drives, or mutate the pending awaiter collection while the outer loop is still traversing it.

#### The Resulting Purity Invariant

By enforcing the Decoupled Mutation Contract, commit index advancement maintains the strict assertion: `assert(primaryStateFence.committedState.is(primaryState0))`. This mathematical invariant proves that commit index calculation and
notification is strictly pure with respect to the causal state. It guarantees that a valid `PrimaryState` reference remains pristine and temporally safe from the beginning of the notification loop to the end, eliminating re-entrancy bugs,
stale-state references, and batch turn-splitting.

---

## Consequential Designs

### Allocation-Free, Re-entrancy-Safe FIFO Awaiter Dispatch

Because the Decoupled Mutation Contract guarantees that `causalAnchor` subscribers do not synchronously trigger cascading state mutations, internal pub/sub mechanics (such as commit watermark awaiters) execute via synchronous,
zero-allocation dispatch without re-entrancy hazards or rescan loops.

Commit watermark awaiters are registered in non-decreasing target index order. Consequently, whenever the commit index advances, fulfilled awaiters form a contiguous prefix at the head of the pending queue. The fulfillment engine processes
this queue using a strictly sequential, allocation-free FIFO dispatch mechanism:

1. **Strict FIFO Execution:** Awaiters are dequeued and resolved strictly in their original registration order from the head of the buffer. This guarantees that state machine command applications and client response dispatches follow strict
   causal order, upholding linearizability and preventing catch-up duplicate executions or watermark regressions.
2. **Atomic Dequeue Prior to Notification:** Each fulfilled awaiter is popped from the head of the buffer immediately before its continuation is invoked. This provides complete re-entrancy isolation against concurrent buffer modifications:
  - *Safety against Appends:* If an observer synchronously registers a new watermark awaiter, the new awaiter targets an index strictly greater than the current commit index and is appended to the tail of the buffer, leaving the ongoing
    head dispatch loop unaffected.
  - *Safety against Clears:* If an observer triggers leadership abdication or shutdown, role exit logic atomically seizes and clears all remaining awaiters for deferred notification, causing subsequent loop head inspections to terminate
    cleanly without index exceptions or redundant executions.
3. **Purity and Loop Decoupling:** Because all state-mutating observers are decoupled via `Capture_defer` or task deferral per the Decoupled Mutation Contract, observer notification mathematically cannot synchronously advance the commit
   index. Therefore, an outer rescan loop is obsolete, preserving stack purity and zero allocation on the happy path.

### Centralized Leader Command Application and Follower Parity

To uphold linearizability under concurrent client command pipelining ($W > 1$) while avoiding per-command promise allocations, state machine command execution in the leader role adheres to the Follower Parity principle:

1. **Unified Sequential Application Loop:** Rather than spawning decentralized, competing catch-up loops per in-flight command, state machine execution is strictly serialized within a single sequential applier loop. This loop processes
   committed entries strictly in log order from the last applied index up to the current commit watermark.
2. **Allocation-Free FIFO Response Buffer:** In-flight client command responses are buffered in a flat circular array (`ArrayDeque`) in log registration order. When the centralized applier commits and applies a client command to the state
   machine, it pops the matching response captor from the head of the buffer and fulfills it with the state machine result without intermediate promise chaining or lambda allocation.
3. **Re-Entrancy and Abdication Safety:** Prior-role applier tasks from earlier terms are drained before leader state machine processing begins. If leadership is vacated while commands are in flight, the pending response buffer is
   atomically seized and cleared, and client failover notifications are dispatched on a subsequent sequencer turn per the Decoupled Mutation Contract.
