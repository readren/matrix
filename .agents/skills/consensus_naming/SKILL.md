---
name: consensus-naming
description: Provides semantic naming conventions for the consensus module, distinguishing between boundary bridges (on...) and internal logic handlers (handle...).
---

# Consensus Naming Conventions

This skill provides the naming patterns required to maintain semantic honesty and architectural separation in the consensus module.

## Core Principle: Semantic Directionality

Naming must reflect whether a method is a **boundary event** (bridge) or an **internal transition** (implementation).

### 1. Bridge API & Boundary Notifications (`on...`)

Use the `on...` prefix for methods that represent events crossing a module boundary (Consensus ↔ Cluster, Consensus ↔ Observer, Remote ↔ Local).

* **Logic**: "On [Event], the [Recipient] performs its own action."
* **Categories**:
    * **Inbound RPCs**: `onHowAreYou`, `onAppendRecords` (in `Delegate`).
    * **Observers**: `onActiveConfigChanged`, `onBecameQuiesced` (in `NotificationListener`).
    * **Advisory Hooks (Gifts)**: `onActiveConfigChanged`, `onQuiesced` (in `ClusterParticipant`). These are methods provided to the host for its own convenience.
* **Constraint**: Never use transitive verbs like `notify` or `report` on the recipient object (e.g., avoid `cluster.notifyQuiesced`). Use `on...` to correctly frame the recipient as the actor reacting to the event.

### 2. Internal Logic Handlers (`handle...`)

Use the `handle...` prefix for methods that process internal lifecycle events or state machine transitions within the `Role` hierarchy.

* **Logic**: "Handle the internal [Event/Transition] as part of the core algorithm."
* **Standard Pair**: Use `handleEntry` and `handleExit` (rather than `onEnter` or `onLeave`) for role lifecycle hooks.
* **State Changes**: Use `handleActiveConfigChanged` for the internal logic that updates the role's state when the primary configuration changes.

## Summary Table

| Context               | Pattern     | Purpose                                               |
|:----------------------|:------------|:------------------------------------------------------|
| **Outbound to Host**  | `on...`     | Informs the host (e.g., `cluster.onQuiesced`)         |
| **Inbound from Host** | `on...`     | Reacts to host/remote (e.g., `delegate.onHowAreYou`)  |
| **Internal to Role**  | `handle...` | Core algorithm transitions (e.g., `role.handleEntry`) |
