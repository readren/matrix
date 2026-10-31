---
type: "Concept"
title: "Multi-Raft System Architecture"
description: "Architectural design, scalability analysis, and trade-offs of the distributed Multi-Raft application framework with co-located persistence and control/data plane separation."
tags: [ "user-guide", "consensus", "nexus" ]
timestamp: "2026-10-29T20:20:00Z"
---

# Multi-Raft System Architecture

This document describes the architectural foundation of the distributed application framework, centering on a hierarchical consensus topology that separates the **Control Plane** from the **Data Plane** using a reactive, heartbeat-less
**Lazy Multi-Raft** protocol with co-located persistence. For the single-group consensus state machine and protocol specification, see [Lazy Raft Consensus Protocol Specification](lazy-multi-raft-consensus.md).

---

## 1. Concurrency, Contention, and Gunther's Universal Scalability Law (USL)

When scaling distributed systems and actor-based runtimes, performance degradation is often non-linear. To model and analyze scaling limits, we apply **Gunther's Universal Scalability Law (USL)**. The law relates throughput \(X (N)\) to
concurrency level or node count \(N\):

\[X (N) = \frac{\lambda N}{1 + \alpha (N - 1) + \beta N (N - 1)}\]

Where:

- \(\lambda\): The scale factor representing the baseline throughput of a single standalone node.
- \(\alpha\): The **Contention Coefficient**, representing serialization, queueing, and locking delays due to competition for shared linear resources.
- \(\beta\): The **Coherence Coefficient**, representing the crosstalk or synchronization penalty required to maintain a consistent state across all nodes. This penalty grows quadratically \(O (N^2)\) with node count.

### Superlinear Performance Degradation in Standard Raft

In a standard, monolithic Raft implementation, performance degrades superlinearly as the cluster scales due to three primary bottlenecks:

1. **Leader Contention (\(\alpha\) factor):** All client reads, writes, and metadata coordination are routed through a single leader node. The leader must serialize data, process network packets, and maintain \(N-1\) concurrent round-trip
   replication streams, creating a severe CPU and network bottleneck.
2. **Physical Coherence Costs (\(\beta\) factor):** Every write command requires a write-ahead log (WAL) write with disk synchronization (`fsync`) across a majority quorum (\(N/2 + 1\)). As \(N\) scales, tail latencies from random I/O
   delays on slower followers drag down overall system throughput.
3. **Leader Election Network Storms:** Under extreme load, leader heartbeats may be delayed or dropped. This triggers election timeouts, causing followers to flood the network with `RequestVote` RPCs. Coordination traffic spikes from \(O
   (N)\) to \(O (N^2)\), dropping useful throughput to zero.

---

## 2. Architectural Pillars of the Lazy Multi-Raft Framework

To bypass the scaling ceilings of monolithic consensus systems (such as standard Akka or Orleans clustering), this framework employs a modular, shared-nothing architecture defined by three core pillars:

```
                  +-----------------------------------+
                  |        Global Control Raft        |
                  |  (Topology & Shard Assignments)   |
                  +-----------------------------------+
                                    |
            +-----------------------+-----------------------+
            |                                               |
            v (Local Replication)                           v (Local Replication)
+-----------------------+                       +-----------------------+
|  Node 1 (Coordinator) |                       |  Node 2 (Coordinator) |
|                       |                       |                       |
|  +-----------------+  |                       |  +-----------------+  |
|  |  Lazy Shard A   |  |                       |  |  Lazy Shard B   |  |
|  |   (Raft Leader) |  |                       |  |  (Raft Follower) |  |
|  +-----------------+  |                       |  +-----------------+  |
|           |           |                       |           |           |
|           v           |                       |           v           |
|  +-----------------+  |                       |  +-----------------+  |
|  | Local DB (Rocks)|  |                       |  | Local DB (Rocks)|  |
|  +-----------------+  |                       |  +-----------------+  |
+-----------------------+                       +-----------------------+
```

### I. Reactive Lazy Consensus (Heartbeat-less)

Standard Raft requires periodic background heartbeats to maintain leader status and detect node failures. The **Lazy Raft** variant eliminates background heartbeats entirely:

- **Zero Idle Load:** When there is no client traffic, background network and CPU usage across inactive consensus groups is exactly zero.
- **On-Demand Fault Detection:** Node failure and leadership loss are detected reactively upon the arrival of client commands or state inquiries (see [State Discovery Phase (
  `HowAreYou` / Pre-Vote)](lazy-multi-raft-consensus.md#i-multi-phase-reactive-election-architecture)).
- **Adaptive Pipelining ("First-Stitch" Recovery):** Because dormant shards emit no background heartbeats, an idle or failed shard has no active leader until client traffic arrives. When a client sends a command to a leaderless node, the
  node does not reject the request or force the client to retry. Instead, it pauses the client's command in memory while using that very request as a trigger to run a fast, on-demand leader election
  (see [Multi-Phase Reactive Election Architecture](lazy-multi-raft-consensus.md#i-multi-phase-reactive-election-architecture)). The moment the new leader takes office, it immediately executes the paused command in its initial replication
  batch. To the client, the request succeeds seamlessly without error handling—the brief election process simply manifests as a few milliseconds of extra latency on that first write.

### II. Multi-Raft Sharding (Blast Radius Isolation)

Instead of forcing all actors or entities to coordinate through a single monolithic consensus group, the framework partitions system state into numerous **independent Raft groups** (one per Shard or Entity):

- **Blast Radius Isolation:** Contention, disk I/O latency, or temporary node failures within Shard A do not impact or block Shards B, C, or D.
- **Massive Actor Scale:** Because inactive Lazy Rafts consume zero CPU and network resources while idle, the framework supports millions of individual Raft groups in memory, achieving linearizable consistency at granular entity boundaries.

### III. Shared-Nothing Co-localized Persistence

Traditional actor frameworks separate compute nodes from a centralized database tier. This framework couples compute and storage on each physical node:

- **Zero-Network Reads:** Shard leaders read state from a co-located, embedded database on the same physical host (via memory mapping or fast IPC) with microsecond latency.
- **Local Quorum Writes:** Network writes are restricted strictly to replicating the write-ahead log across the small quorum of nodes assigned to that specific shard (typically 3 replicas, needing only 2 votes).
- **Linear Scaling:** Adding physical nodes scales compute power and I/O capacity concurrently, avoiding centralized storage bottlenecks.

---

## 3. Control Plane vs. Data Plane Separation

The framework enforces a strict hierarchical division between control operations and application data operations:

1. **Global Control Plane (Standard or Lazy Raft):** Manages shard routing tables, cluster membership, and coordinates node-level Shard Coordinators.
2. **Data Plane (Lazy Multi-Raft):** Handles application messages, entity replication, and local state transitions within individual shard partitions.

### Fault Tolerance & Network Partitions

- **Global Raft Authority:** If a network partition isolates a Shard Coordinator in a minority partition, the Global Raft (maintaining majority quorum elsewhere) detects the loss and reassigns shard leadership in the majority partition. The
  isolated Coordinator is blocked from modifying shard topologies (see [Participant Exclusion, Retirement Dynamics & Quiescence Protocol](lazy-multi-raft-consensus.md#1-participant-exclusion-retirement-dynamics--quiescence-protocol)
  and [Electorate Change Protocol & Response Semantics](lazy-multi-raft-consensus.md#5-electorate-change-protocol--response-semantics)).
- **Routing Replication Trade-off (Total vs. Subset Replication):**
  - *Total Replication (Chosen Architecture):* The cluster routing table is replicated to all nodes in the system. Because shard topology updates are infrequent and the consensus protocol is lazy (having zero background heartbeat traffic),
    the overhead of replicating topology changes to all nodes is negligible. All nodes can perform **Zero-RTT Routing**, redirecting client commands directly to the authoritative shard leader in a single network hop.
  - *Quorum Response Latency Analysis:* Under standard Raft, scaling quorum size (\(N/2 + 1\)) is assumed to degrade write latency. However, because the leader dispatches replication requests in parallel over non-blocking NIO channels,
    quorum latency is governed by **Order Statistics** (specifically the distribution of the median value). The leader does not wait for the slowest node, but for the \( (N/2 + 1)\)-th node. As \(N\) scales, the availability of competing
    healthy nodes mitigates tail-latency spikes, keeping quorum response time nearly constant.
  - *Availability and Blast Radius:* Relying on a small fixed subset of nodes (e.g., a 5-node control group) creates a distributed single point of failure. If 3 of those 5 nodes fail, the entire cluster halts, even if dozens of other nodes
    are healthy. By contrast, configuring all nodes as voting members in the Global Raft offers massive resilience: in a 49-node cluster, 25 nodes must fail simultaneously to halt the control plane.

---

## 4. Agnostic Persistence via Inversion of Control (IoC)

The framework strictly separates the consensus engine's durability mechanism from the user's business state, allowing application developers to employ any embedded database engine (such as RocksDB, SQLite, or LMDB) as long as it is
co-located on the node.

```
+------------------------------------------------------------+
|                  Matrix Runtime Framework                  |
+------------------------------------------------------------+
       |                                             |
       v (Storage SPI: Workspace)                    v (Application SPI: StateMachine)
+----------------------------+               +----------------------------+
| Consensus Log (WAL)        |               | State Machine (State)      |
| Exclusive to Framework     |               | Defined by Application     |
| (Raw record buffer & meta) |               | (Domain state & snapshots) |
+----------------------------+               +----------------------------+
```

### Separation of Concerns: Storage SPI vs. Consensus State Machine

To prevent storage implementations from having to duplicate complex consensus logic, the framework enforces a strict architectural boundary between storage I/O and consensus invariant verification:

1. **Storage SPI (`Workspace`):** An unopinionated, sequential storage interface provided by the host environment or tests. It provides primitive I/O operations: appending raw records, truncating suffixes on log conflict, truncating
   prefixes on compaction, and storing term/vote metadata and serialized snapshots. It remains completely oblivious to Raft invariants, term validation, or membership mechanics
   (see [Storage SPI Contract & Persistence Boundary](lazy-multi-raft-consensus.md#ii-storage-spi-contract-workspace--storage--persistence-boundary)).
2. **Consensus State Machine (`PrimaryState`):** The internal consensus component that enforces Raft invariants: validating Log Matching properties, orchestrating log fusion, tracking electorate changes across log and snapshot boundaries,
   and managing volatile commit indices (see [Decoupled Mutation Contract & Causal State Synchronization](lazy-multi-raft-consensus.md#6-decoupled-mutation-contract--causal-state-synchronization)).

By pushing all Raft invariants into the consensus state machine, custom storage implementers only need to implement raw record storage and truncation without handling array-copy conflict resolution or retrospective configuration discovery.

### Delegation of Client Command Deduplication to the State Machine

The consensus engine treats client commands as opaque payloads, strictly limiting its responsibility to durable ordering, quorum replication, and commit index advancement. Monotonic request sequencing, per-client deduplication, freshness
ordering, and retry filtering are decoupled from the consensus log and delegated to the application-level state machine.

This architectural boundary is necessitated by log compaction: once committed log entries are compacted and truncated past snapshot boundaries, the consensus log buffer no longer retains historical client entries to evaluate client-specific
sequence freshness or detect replayed commands. Because client session state and sequence watermarks are maintained within the application state machine, deduplication state is naturally captured across snapshots and transferred during
snapshot installation without coupling the storage SPI to application identity schemas
(see [Application State Machine SPI & Deduplication Boundaries](lazy-multi-raft-consensus.md#iii-application-state-machine-spi-statemachine--deduplication-boundaries)).

---

## 5. Architectural Trade-off Summary

| Characteristic       | Traditional Frameworks (Akka / Orleans)                  | Matrix Lazy Multi-Raft Framework                                  |
|:---------------------|:---------------------------------------------------------|:------------------------------------------------------------------|
| **Idle Load**        | High (constant background Gossip & Heartbeats)           | Zero (no-traffic background quiescence)                           |
| **Failover Pattern** | Storm-inducing (simultaneous cluster-wide elections)     | Progressive (triggered on-demand per shard upon client command)   |
| **Data Locality**    | Network-bound (external database queries over TCP)       | Co-located (local reads via memory/IPC in microsecond latencies)  |
| **Consistency**      | Often Eventual Consistency or CRDTs                      | Strong Consistency (Linearizable) per shard                       |
| **Scaling Cost**     | Quadratic coordination bottleneck (\(O(N^2)\) crosstalk) | Linear (independent shard quorums, zero crosstalk between shards) |

---

## 6. Runtime Integration & Component Relationships

The framework ties together distinct foundation modules to achieve deterministic distributed execution:

- **Single-Threaded Execution (`sequencer/core`):** Every consensus participant and shard state machine executes strictly within a dedicated single-threaded sequencer ([`Doer`](sequencer-core.md)) using causal promises for ordered mutations
  ([CausalFence and the Decoupled Mutation Contract](adr-causal-fence-state-synchronization.md)). This eliminates thread synchronization, locks, and actor re-entrancy bugs on the hot path.
- **Actor Runtime (`nexus/core`):** Shard coordinators and service delegates are managed as lightweight actants structured via the [Component Definition Module Pattern](adr-component-definition-module-pattern.md) with explicit supervision
  and lifecycle hooks.
- **Cluster Transport (`nexus/cluster`):** Inter-node communication, channel serialization, and asynchronous connection lifecycles are handled through non-blocking NIO channels without polling.
- **Consensus Engine (`consensus`):** Individual shard replicas and the global control group execute the reactive protocol defined in the [Lazy Raft Consensus Protocol Specification](lazy-multi-raft-consensus.md).
