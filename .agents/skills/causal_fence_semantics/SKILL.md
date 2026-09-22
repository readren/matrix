---
name: causal-fence-semantics
description: Conceptual overview, usage patterns, and strict constraints for using CausalFence and CausalStuckableFence. Trigger this skill whenever you encounter CausalFence or CausalStuckableFence in the codebase, are asked to implement deterministic sequential state transitions, or need to safely anchor decoupled updates to a causal timeline (e.g., as seen in the consensus module).
---

# CausalFence & CausalStuckableFence Semantics

This skill provides LLM agents with the conceptual framework and practical constraints required to write correct code using `CausalFence` and `CausalStuckableFence`.

## Conceptual Analogy

Think of `CausalFence` as a **Non-blocking Mutex implemented via a Promise Queue**.

- **Like an Actor:** It guarantees that state mutations run strictly sequentially one at a time.
- **Unlike an Actor:** It only synchronizes the specific state it wraps. The underlying `Doer` is not blocked and can multiplex other tasks while waiting for asynchronous operations.
- **Like Event Sourcing:** It provides an explicit, observable sequence of state transitions, allowing decoupled components to "anchor" derived updates to specific points in time.
- **The Stuckable Variant (`CausalStuckableFence`):** A specialized variant designed for irreversible, terminal states. Once the causal chain becomes "stuck," it remains stuck forever. It safely absorbs and enqueues future updates without
  executing them. This is essential for components that have shut down, encountered fatal errors, or entered a final lifecycle phase where further state transitions must be permanently ignored without throwing exceptions to callers.

## Canonical Usage Pattern (Decoupled Derived State)

When Component A updates the primary state, and Component B needs to update derived state strictly after A, but before any future Component C:

```scala
// Component A enqueues an async mutation and Component B synchronously attaches to that exact link
for state1 <- fence.advance { state0 => computeNextStateAsync(state0) } yield updateDerivedState(state1)
```

This is the "Game-changing invariant": by attaching synchronously to the `LatchingDuty` returned by `advance` or `causalAnchor`, the framework mathematically guarantees execution ordering.

## Component Selection Heuristics

- Use **`CausalFence`**: For standard, always-forward state machines that never need to permanently halt their sequence queue.
- Use **`CausalStuckableFence`**: When the state machine has a terminal state or irreversible condition (like shutdown or fatal error) where all subsequent updates must be permanently halted and safely absorbed without executing or throwing
  exceptions.

## Method Selection Heuristics

- Use **`advance`**: When the primary state mutation involves an asynchronous effect or returns a `Duty`.
- Use **`jump`**: When the primary state mutation is entirely synchronous and CPU-bound.
- Use **`advanceSpeculatively`**: When the asynchronous operation might fail or be invalidated, requiring an explicit rollback to the previous state using the provided `RollbackAccessor`.
- Use **`causalAnchor`**: When a standalone component needs to read the state and execute a side-effect safely within the causal timeline, without mutating the primary state itself.

## Hard Constraints & Anti-Patterns

1. **Never block the thread:** Do not use `Thread.sleep` or blocking `Await` calls inside the updaters. It will stall the `Doer`.
2. **Never attempt to cancel an executing update:** If an update becomes moot, handle it via `RollbackAccessor` inside the `advanceSpeculatively` block. Do not attempt to drop Runnables from the execution queue.
3. **Never persist sequence links:** Do not store the `LatchingDuty` returned by `advance` in class fields or external state to subscribe to later. Once the synchronous execution block finishes, that link is obsolete. If you need to anchor
   to the current state at a later time, always call `causalAnchor()` to retrieve the fresh tail.
4. **Lexical Scope vs. Temporal Invalidation Invariant:** A `State` reference yielded by `advance`, `advanceIf`, `jump`, or `causalAnchor` (or received as a method parameter) is temporally valid **only within the synchronous execution block
   directly attached to that operation**. Crossing any asynchronous generator (`<-`) in a `for` comprehension (such as a network RPC, delay schedule, or external `Capture`) suspends execution, allowing interleaved sequencer tasks to advance
   the fence. This immediately invalidates and poisons all preceding `State` variables. Reusing a `State` variable across an asynchronous generator without immediately re-anchoring via `fence.causalAnchor()` is an automatic correctness
   defect.

### Anti-Pattern: Reusing Lexically Scoped State Across Async Generators

```scala
// ❌ ANTI-PATTERN: Lexical reuse across an asynchronous suspension
def executePhase(state0: PrimaryState): sequencer.Capture[Unit] = {
	for {
		rpcResult <- queryPeersAsync(state0) // Asynchronous suspension gap
		// BUG: state0 remains lexically in scope, but is temporally stale! Interleaved tasks may have advanced the fence.
		outcome <- processResult(state0, rpcResult)
	} yield outcome
}

// ✅ CORRECT PATTERN: Mandatory re-anchoring across asynchronous gaps
def executePhase(state0: PrimaryState): sequencer.Capture[Unit] = {
	for {
		rpcResult <- queryPeersAsync(state0) // Asynchronous suspension gap
		state1 <- fence.causalAnchor() // Mandatory re-anchoring to the committed tail!
		outcome <- processResult(state1, rpcResult)
	} yield outcome
}
```

## Mandatory Causal Fence Pre-Commit Checklist

Before finalizing any change that interacts with `CausalFence` or `CausalStuckableFence`:

1. **Async Generator Audit**: Inspect every `for` comprehension containing a `State` variable. If an asynchronous method call (`RPC`, `schedule`, `accumulateDiscoveryQuorum`, etc.) appears on the right-hand side of a `<-`, verify that all
   `State` variables defined above that line are never referenced below it.
2. **Re-anchor Assertion**: Verify that the line immediately following the asynchronous generator retrieves a fresh anchor:
   ```scala
   freshState <- primaryStateFence.causalAnchor()
   ```
3. **Restart Parameter Audit**: Verify that any restart invocation (such as `updateRole(state)`) passes a freshly anchored instance, never an unanchored variable passed down from a previous phase.
