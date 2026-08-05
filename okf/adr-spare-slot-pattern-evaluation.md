---
type: "Decision"
title: "Evaluation of the Spare-Slot Pattern in Sequencer Primitives"
description: "Architectural analysis of the spare-slot zero-allocation pattern evaluated in sandbox DoerSandbox2."
tags: ["design-history", "sequencer", "performance", "task"]
timestamp: "2026-07-22T17:45:00Z"
---

# Architectural Decision Record: Spare-Slot Pattern Evaluation

## Context

In `sequencer/core` (evaluated inside [DoerSandbox2.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/sandbox/DoerSandbox2.scala)), reactive computation primitives (`Task`, `Capturer`, `Flux`, `Tensor`) are used for
deterministic single-threaded sequencing.

Standard reactive pipeline operators (such as `map`, `flatMap`, `scan`) allocate a new `Subscription` or `Observer` state object whenever a downstream consumer subscribes. Under high throughput, these short-lived allocations contribute to
GC allocation pressure.

The **Spare-Slot Pattern** attempts to eliminate this allocation for the primary (first) subscriber by having the operator node instance itself double as the `Subscription` and `MonoObserver`/`FluxObserver`, storing state in reserved fields
(`downChainObserverSlot`, `upChainSubscriptionSlot`). Secondary subscribers fall back to allocated delegate instances.

## Evaluation

### Advantages

1. **Zero-Allocation First Subscriber**: Eliminates 1 heap object allocation per operator node in 1-to-1 subscriber pipelines.
2. **Reduced GC Allocation Rate**: Lowers GC churn in synthetic micro-benchmarks with millions of pipeline subscriptions per second.

### Disadvantages & Risks

1. **Sentinel State Complexity**: Handshake between synchronous source completion and subscription requires sentinel checks (e.g. `upChainSubscriptionSlot eq this`
   in [SpareSlotTaskOp](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/sandbox/DoerSandbox2.scala#L404-L432)) to avoid infinite recursion or lost unsubscriptions.
2. **State Leakage & Teardown Rigidity**: Operators double as state containers, requiring strict clearing (`resetState()`, nullifying slots) on completion, error, or unsubscription. Missed teardowns lead to memory leaks.
3. **Dual Code-Path Overhead**: Every operator must implement both the fast-path (spare slot) and the multi-subscriber fallback path (`subscribeDelegate` / `createDelegate`), doubling codebase size and test matrix per operator.
4. **Negligible Real-World Bottleneck**: Network NIO dispatching, serialization, and payload handling dominate runtime latency. The microsecond/nanosecond saved per subscription allocation rarely translates to measurable macro application
   speedup.

## Decision

The spare-slot pattern introduces high structural complexity, subtle sentinel-based synchronization edge cases, and code duplication across operator families. Unless empirical macro-level benchmarks demonstrate that subscription allocations
are a dominant bottleneck in real-world workloads, the pattern **is not worth the extra complexity** for general task and sequence primitives.
