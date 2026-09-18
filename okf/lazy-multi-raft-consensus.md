---
type: "Concept"
title: "Lazy Multi-Raft Consensus Architecture"
description: "Architectural design, scalability analysis, and trade-offs of the reactive Lazy Multi-Raft consensus engine with co-located persistence."
tags: ["user-guide", "design-history", "consensus", "nexus"]
timestamp: "2026-09-17T21:05:00Z"
---

# Lazy Multi-Raft Consensus Architecture

This document describes the architectural foundation of the distributed application framework, centering on a hierarchical consensus topology that separates the **Control Plane** from the **Data Plane** using a reactive, heartbeat-less *
*Lazy Multi-Raft** protocol.

---

## 1. Concurrency, Contention, and Gunther's Universal Scalability Law (USL)

When scaling actor-based systems and services, performance degradation is often non-linear. To model and analyze this behavior, we apply **Gunther's Universal Scalability Law (USL)**. The law relates throughput \(X (N)\) to the number of
nodes or concurrency level \(N\):

\[X (N) = \frac{\lambda N}{1 + \alpha (N - 1) + \beta N (N - 1)}\]

Where:

- \(\lambda\): The scale factor (throughput of a single node).
- \(\alpha\): The **Contention Coefficient**, representing serialization or queueing delays due to competition for shared linear resources.
- \(\beta\): The **Coherence Coefficient**, representing the crosstalk or synchronization penalty to maintain a consistent state across all nodes. This penalty grows quadratically \(O (N^2)\) with the number of nodes.

### Superlinear Performance Degradation in Standard Raft

In a standard, monolithic Raft implementation, performance degrades superlinearly as the cluster scales due to three primary factors:

1. **Leader Contention (\(\alpha\) factor):** All client reads, writes, and metadata coordination are routed through a single leader. The leader must serialize data, process network packets, and maintain \(N-1\) concurrent round-trip
   connections, creating a severe bottleneck.
2. **Physical Coherence Costs (\(\beta\) factor):** Every write command requires a write-ahead log (WAL) write with disk synchronization (`fsync`) across a majority quorum (\(N/2 + 1\)). As \(N\) grows, tail latencies from random I/O delays
   on slower followers drag down overall system throughput.
3. **Leader Election Network Storms:** Under extreme load, leader heartbeats may be delayed or dropped. This triggers election timeouts, causing followers to flood the network with `RequestVote` RPCs. The coordination traffic mutates from
   \(O (N)\) to \(O (N^2)\), dropping useful throughput to zero.

---

## 2. Architectural Pillars of the Lazy Multi-Raft Framework

To bypass the scaling ceilings of monolithic consensus systems like Akka/Orleans, this framework employs a modular, shared-nothing architecture defined by three core pillars:

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

Standard Raft requires background heartbeats to maintain leader status and detect failures. The **Lazy Raft** variant eliminates background heartbeats entirely:

- **Idle Cost is Zero:** When there is no client traffic, background network and CPU usage is zero.
- **On-Demand Fault Detection:** Node failure is detected reactively when a client command is dispatched.
- **Adaptive Pipelining ("First-Stitch" Recovery):** When a timeout indicates leader failure, the client's command serves as the trigger for the voting phase. The command is either embedded within the `RequestVote` message or queued at the
  head of the new leader's log buffer, reducing the recovery latency penalty.

### II. Multi-Raft Sharding (Blast Radius Isolation)

Instead of forcing all actors or entities to coordinate via a single consensus group, the framework partitions the system into numerous **independent Raft groups** (one per Shard/Entity):

- **Failure Isolation:** Contention, I/O latency, or failure within Shard A does not block Shards B, C, or D.
- **Massive Actor Scale:** Because inactive Lazy Rafts consume no network or CPU resources, the framework can support millions of individual Raft groups in memory, achieving linearizable consistency at the actor level.

### III. Shared-Nothing Co-localized Persistence

Traditional actor frameworks separate compute nodes from a centralized database cluster. This framework couples them:

- **Zero-Network Reads:** Shard leaders read state from a co-located, stand-alone database on the same physical node (e.g., via fast memory or IPC) in microsecond latencies.
- **Local Quorum Writes:** Network writes are restricted to replicating the WAL to the small quorum of nodes containing the replicas of that specific shard (e.g., 3 nodes, needing only 2 votes).
- **Linear Scaling:** Adding a node to the cluster scales compute power and I/O capacity concurrently, avoiding centralized DB bottlenecks.

---

## 3. Control Plane vs. Data Plane Separation

The framework enforces a strict hierarchical division between control operations and data operations:

1. **Global Control Plane (Standard/Lazy Raft):** Manages shard routing tables, cluster membership, and coordinates Shard Coordinators.
2. **Data Plane (Lazy Multi-Raft):** Handles application messages and entity replication.

### Fault Tolerance & Network Partitions

- **Global Raft Authority:** If a network partition isolates a Shard Coordinator in a minority partition, the Global Raft (maintaining majority quorum elsewhere) detects the loss and spawns a replacement Coordinator in the majority
  partition. The isolated Coordinator is blocked from making topology modifications.
- **Routing Replication Trade-off (Total vs. Subset Replication):**
    - *Total Replication (Chosen):* The cluster routing table is replicated to all nodes in the system. Because shard topology updates are highly static and the consensus protocol is lazy (having zero background heartbeat traffic), the
      overhead of replicating topology changes to all nodes is negligible. All nodes can perform **Zero-RTT Routing**, redirecting client commands directly to the correct shard leader in a single hop.
    - *Quorum Response Latency Analysis:* Under standard Raft, scaling the quorum size (\(N/2 + 1\)) is assumed to degrade write performance. However, because the leader dispatches replication requests in parallel (using non-blocking NIO
      channels), the quorum latency is governed by **Order Statistics** (specifically the distribution of the median value). The leader does not wait for the slowest node, but for the \( (N/2 + 1)\)-th node. As \(N\) scales, the
      availability of more nodes competing to respond mitigates tail-latency spikes, making the quorum response time nearly constant.
    - *Availability and Blast Radius:* Relying on a small select subset of nodes (e.g., a 5-node control group) creates a distributed single point of failure. If 3 of these 5 nodes fail, the entire cluster halts, even if dozens of other
      nodes are healthy. By contrast, using all nodes as voting members in the Global Raft offers massive resilience: in a 49-node cluster, 25 nodes must fail simultaneously to halt the control plane.

---

## 4. Agnostic Persistence via Inversion of Control (IoC)

The framework separates the consensus mechanism's state from the user's business state, allowing developers to utilize any database engine (e.g., RocksDB, SQLite, Redis, or Graph DBs) as long as it is co-located on the node.

### Separation of Logs and State

```
+------------------------------------------------------------+
|                       Your Framework                       |
+------------------------------------------------------------+
       |                                             |
       v (Interface 1)                               v (Interface 2)
+----------------------------+               +----------------------------+
| Consensus Log (WAL)        |               | State Machine (State)      |
| Exclusive to Framework     |               | Defined by User            |
| (e.g., RocksDB, Raft Log)  |               | (e.g., PostgreSQL, Redis)  |
+----------------------------+               +----------------------------+
```

### Abstraction Interfaces

To ensure zero-cost abstractions, the framework models persistence interfaces using typeclasses in Scala 3 and traits in Rust, allowing static dispatch and compiler inlining.

#### Scala 3.8 Typeclass Definition

```scala
// Abstraction for user-defined state machine behavior and persistence
trait StateMachine[State, Command, Event]:
  def applyCommand(state: State, cmd: Command): Either[Throwable, Event]
  def applyEvent(state: State, event: Event): State
  def persistState(state: State): Unit // Hook for co-located user DB
```

#### Rust Trait Mapping

```rust
pub trait StateMachine {
    type State;
    type Command;
    type Event;

    fn apply_command(&self, state: &Self::State, cmd: Self::Command) -> Result<Self::Event, String>;
    fn apply_event(&mut self, state: &mut Self::State, event: Self::Event);
}
```

### The `Workspace` and `Accessible` Persistence Split

To ensure the log storage layer (`Workspace`) remains a pure, "dumb" I/O abstraction devoid of consensus logic, the framework splits log persistence concerns into two layers:

1. **`Workspace` (Storage SPI):** A synchronous, in-memory facade provided by the host environment (or tests) to store and truncate the log buffer and snapshots. It provides only primitive operations like `truncateSuffix`, `truncatePrefix`,
   and `appendRecord`, and is completely isolated from Raft invariants.
2. **`Accessible` (Consensus Driver):** An internal, consensus-aware wrapper around `Workspace` used by the active Raft roles (`Leader`, `Follower`, etc.). It enforces Raft's Log Matching invariant (handling conflict resolution and
   truncation during appends), tracks `ConfigChange` offsets across the log and snapshots, and manages log scanning and caching.

By pushing Raft invariants up into the `Accessible` layer, the `Workspace` interface is radically simplified. Custom storage implementers no longer need to write complex array-copy conflict resolution or scan backwards for configuration
changes during truncations.

---

## 5. Architectural Trade-off Summary

| Characteristic       | Traditional Frameworks (Akka / Orleans) | Lazy Multi-Raft Framework                   |
|:---------------------|:----------------------------------------|:--------------------------------------------|
| **Idle Load**        | High (constant Gossip & Heartbeats)     | Zero (no-traffic background quiescence)     |
| **Failover Pattern** | Storm-inducing (simultaneous elections) | Progressive (triggered on-demand per shard) |
| **Data Locality**    | Network-bound (external DB queries)     | Co-located (local reads via memory/IPC)     |
| **Consistency**      | Often Eventual / CRDTs                  | Strong Consistency (Linearizable) per shard |
| **Scaling Cost**     | Quadratic coordination bottleneck       | Linear (independent shard quorums)          |

---

## 6. Participant Exclusion, Retirement Dynamics & Quiescence Protocol

When cluster membership changes, nodes removed from the active cluster topology must be safely transitioned from active consensus participation to complete network quiescence without violating linearizability or causing election deadlocks.

### I. Participant Role Lifecycle & Reconfiguration Overview

A participant node in the consensus engine moves through distinct operational roles during its lifecycle:

```
[ Starting / Isolated ]
         |
         v
[ Candidate / Follower / Leader ]  <-- Active Cluster Member
         |
         | (Excluded by Stable Configuration Change)
         v
    [ Retiring ]                   <-- Catching up log entries up to Excluding Index
         |
         | (Retirement Drivers Finish/Abort & Cluster Quiescence Authorized)
         v
    [ Quiesced ]                   <-- Zero Network / Background Load
```

- **Active Roles (Leader, Follower, Candidate)**: Process client commands, participate in leader elections, and replicate log entries.
- **Retiring Role**: Entered when a stable configuration change excludes the participant from the cluster. The participant stops proposing client commands and cannot win elections for subsequent terms. It remains active solely to catch up
  its local log to the excluding configuration entry.
- **Quiesced Role**: Terminal resting state where the participant halts all background network traffic, timers, and RPC handling.

---

### II. Retirement Replication Driver Architecture

When a configuration transition excludes one or more participants (moving from a transitional configuration to a stable configuration), the leader initiates targeted replication processes (pipelines) to push records to the excluded
participants until they reach the retirement boundary (the StableConfigChange that excluded them).

- **Leader Push over Follower Pull**: The cluster leader actively drives replication to non-active participants. This eliminates the need for a pull-based `RetirementDriver` on the follower. Because the new leader relies on the cluster
  transport layer (via `ClusterParticipant.getOtherProbableParticipants`) to discover all live nodes, it can spin up pipelines for any non-active nodes (including those excluded by past leaders). Consequently, an abdicating leader can
  safely tear down its own pipelines, knowing the new leader will pick up the slack.
- **Strict Upper Boundary (TargetIndexBound)**: The pipeline restricts the logs sent to the participant strictly up to the log index of the excluding configuration change (`sccIndex`).
- **LeaderCommit Clamping & Ghost Records**: When sending `AppendRecords`, the `leaderCommit` sent to the excluded participant MUST NEVER exceed the `sccIndex`. If the retiring participant has unverified, trailing "ghost" records in its log
  (e.g., from a past term where it was active), an empty `AppendRecords` (bounded by `sccIndex`) will not truncate them. If the leader sent a `leaderCommit > sccIndex`, the participant would execute
  `commitIndex = min(leaderCommit, indexOfLastNewEntry)` and falsely commit its ghost records, corrupting its state machine. Therefore, `requestLeaderCommit` is tightly clamped.
- **Snapshot Fallback (Wiping Ghost Records)**: If the leader's log has been compacted past the `sccIndex`, it cannot send a `requestLeaderCommit <= sccIndex` (as `getRecordTermAt` would throw an `IndexOutOfBoundsException`). Instead, the
  pipeline falls back to sending an `InstallSnapshot`. This safely resolves the dilemma: because the snapshot fully replaces the follower's log, all unverified trailing ghost records are wiped out. The follower safely adopts the verified
  state (which includes its own exclusion) and its `commitIndex` safely advances past `sccIndex` without risking corruption.
- **Stateless Retiring Role**: The follower only transitions to the `Retiring` role *after* it has successfully committed the excluding configuration change. Because it has already committed its own exclusion, it has no need to process
  further records. Thus, the `Retiring` role is appropriately stateless and rejects all incoming `AppendRecords` or `InstallSnapshot` requests (unless they contain a new configuration that re-includes it). This rejection signals the
  leader's pipeline to terminate.
- **Non-Fatal Pipeline Aborts (`GracefullyReleased`)**: Background pipelines rely on `CausalFence` operations (like `causalAnchor()`). These operations can fail non-fatally with a `GracefullyReleased` exception if the role loses
  statefulness (e.g., the leader demotes). When this happens, the capture fails gracefully, and the replication pipeline silently and safely halts, recognizing that the leader role is shutting down.
- **Driver Lifecycle & Registry Cleanup**:
    - The leader maintains an active tracking registry of all running retirement drivers (`retiringLearnersById`).
    - The pipeline is terminated and the participant is removed from the registry once it acknowledges it has committed the `sccIndex` (e.g., by responding with an `AppendResult_Rejected` citing the `RETIRING` role).

---

### III. Quiescence Protocol & Convergence Preconditions

Quiescence authorization ensures that a retiring participant does not shut down prematurely before its peers have recognized its exclusion and authorized its departure.

- **Quiescence Authorization Flow**:
    1. Upon committing an excluding configuration change, the leader broadcasts quiescence authorization permissions (`PermitQuiesce`) to all excluded participants.
    2. Retiring participants acknowledge the permission and record the authorized configuration change index.
    3. If initial permission requests fail due to transient network drops, the leader retries permission delivery up to a configured retry limit before clearing remaining unacknowledged permissions.

- **Convergence Preconditions for Quiescence**:
  A participant in the `Retiring` role can transition to `Quiesced` if and only if four independent conditions converge:
    1. **Role Eligibility**: The participant's current role is `Retiring`.
    2. **Local Driver Clearance**: The participant's local retirement drivers tracking registry is empty. If the participant acted as leader during the configuration transition, all retirement drivers it initiated to catch up excluded
       followers must have completed or aborted and unregistered from its local registry.
    3. **Permission Grant**: Explicit quiescence authorization has been granted by the cluster.
    4. **Index Verification**: The authorized configuration change index is greater than or equal to the participant's excluding configuration index.

---

### IV. Retiring Participant Election StateInfo Invariant

- **Excluding-Config Index Bounding in StateInfo**: To prevent election deadlock during cluster reconfiguration, a participant in the retiring state must report the term and index of the excluding configuration change as its committed state
  information (`StateInfo`), even if its local log contains committed records at higher indices.
- **Election Deadlock Prevention**: Reporting post-exclusion log indices during election coordination would allow an excluded node with higher log indices to block active cluster members from achieving a decisive election outcome. Bounding
  the exposed state to the excluding configuration change ensures active participants can elect a valid leader without being blocked by retiring nodes.

## Per-Peer In-Flight Append Backpressure & Domain Result ADT

### I. In-Flight Serial Tracking and Sliding Window

- **Monotonic Serial Generation**: Every append RPC emitted to a learner is tagged with a monotonically increasing serial number per peer.
- **Self-Healing Response Acknowledgement**: The leader tracks the highest acknowledged append serial number per peer. Processing a higher serial number automatically acknowledges all preceding in-flight requests for that peer.
- **In-Flight Bound Enforcement**: The difference between the last emitted append serial number and the last acknowledged append serial number defines the in-flight count. When this count reaches the configured threshold
  (`maxInFlightAppendsPerPeer`), further emissions to that peer are deferred.

### II. AppendResult ADT & Progress Non-Mutation Invariants

- **Domain Outcome Safety**: Append RPC outcomes are explicitly typed as ADT variants (`Accepted`, `Rejected`, `SkippedDueToBackpressure`, `SkippedOutOfConfiguration`, `Failed`) eliminating magic-number status flags and heap-allocated error
  wrappers.
- **Backpressure Deferral Invariant**: Deferring an append request due to in-flight backpressure emits a `SkippedDueToBackpressure` domain result. Processing `SkippedDueToBackpressure` is a strict no-op on peer progress state, leaving
  pessimistic and optimistic replication indices completely unchanged.

---

## 7. Raft §5.4.2 Commitment and Log Compaction Safety Invariants

### I. Strict Commitment Verification Before State Machine Application

- **Uncommitted Entry Barrier**: A leader must never apply a command record to its state machine, respond success to a client, or trigger log compaction based solely on peer RPC transport success.
- **Raft §5.4.2 Transitive Commitment Constraint**: Log entries from previous terms cannot be committed directly by majority acknowledgment alone. They can only be committed indirectly by committing a log entry from the leader's current
  term.
- **Upfront Current-Term Entry Insertion for Prior-Term Records**: Under Raft §5.4.2, a leader cannot advance its commit index directly on an uncommitted entry appended in a previous term. If an uncommitted entry (such as a recovered stable
  configuration change) has a term strictly lower than the leader's active term and no subsequent entry in the active term exists in the log, the leader must append a current-term no-op transition entry upfront before awaiting commitment.
  Awaiting commitment directly on a previous-term entry without a current-term entry in the log deadlocks, as commitment verification rules permanently refuse to advance the commit index.
- **In-Flight Reconfiguration Pipeline Serializability**: Upon assuming leadership with an uncommitted configuration change in the persistent log, the replication pipeline driving that configuration change to commitment must be linked to
  the leader's in-flight configuration change completion handle. Subsequent configuration change requests must serialize behind this recovery pipeline. Permitting subsequent configuration changes to execute before the recovered
  configuration change reaches the commit index causes false transitional-phase rejections, as the cluster configuration remains derived from the uncommitted transitional state.
- **Commit Watermark Barrier Invariant**: Awaiting record commitment functions strictly as a monotonic watermark barrier, never an RPC-style request-reply operation. The barrier never resolves with an uncommitted or failure status while the
  leader remains in office. When the barrier resolves for an active leader, the target record index is mathematically guaranteed to be committed. In the event of quorum loss or network partition, the barrier remains suspended indefinitely
  until quorum is restored or the leader is deposed and exits office, eliminating phantom retry loops in post-barrier continuations.
- **Peer vs. Retiree Replication Pipeline Disjunction**: An active peer participating in the current cluster configuration cannot simultaneously be tracked as a retiring participant. When the active configuration changes to include a
  participant, any pending retirement pipeline and quiescence authorization for that participant must be cancelled and removed. Furthermore, if an active peer responds with a role of `RETIRING` (because it has not yet received the
  configuration change that includes it), the leader must treat the rejection as needing earlier records rather than abandoning replication. The leader must rewind replication to include the configuration entry that brings the follower out
  of retirement, preventing deadlock.

### II. Universal Log Fusion & Obsolete Suffix Truncation Invariants

- **Universal Scope**: Log matching and fusion invariants apply to all participants processing replication RPCs (`AppendRecords`), regardless of their current role (`Follower`, `Joining`, `Isolated`, or `Retiring`).
- **Conflict Truncation**: During log fusion, if an existing local record at index $i$ differs in term from the corresponding record in the incoming batch, the local log is truncated from index $i$ onwards before appending the remaining
  batch entries.
- **Obsolete Trailing Suffix Truncation**: When all entries in an incoming batch match the local log without term conflicts, but the local log holds uncommitted entries extending beyond the applied batch:
    - The evaluation boundary advances to the first uncommitted entry, calculated as $\max (\text{appliedIndex} + 1, \text{commitIndex} + 1)$.
    - If the entry at this uncommitted boundary was written under a term strictly lower than the active leader's term, that entry and all subsequent records represent uncommitted remnants from a superseded term.
    - The participant must truncate its log starting at this uncommitted boundary.
- **Committed Data Invariant**: Suffix truncation is strictly bounded by the participant's verified commit baseline (`commitIndex + 1`), ensuring committed records are never mutated or truncated.
- **Current-Term Invariant**: If uncommitted trailing records have terms matching the active leader's term, they are preserved as valid extensions established under the current leadership tenure.

### III. Log Compaction Boundaries

- **Committed-Only Compaction Invariant**: Log compaction and log truncation must operate strictly within the bounds of committed log entries. A node must never truncate log entries beyond its verified commit index.
- **Compaction Truncation Bounds**: Log compaction truncation index must never fall below the in-memory log buffer offset. The retained log record count is bounded by the compaction threshold to guarantee strictly positive buffer bounds.
- **Recovered Commit Floor Invariant**: Commands applied to the state machine or embedded in a persistent snapshot are mathematically guaranteed to have been committed by quorum. Upon startup or crash recovery, a participant must advance
  its volatile commit index to reflect the recovered applied command index and snapshot boundary, ensuring candidate state information accurately advertises the verified committed baseline during leader elections.
- **State Machine Synchronization**: State machine application index and log compaction truncation point must never exceed the verified commit index.

### IV. Client Response Contract & Commit Watermark Invariant

- **Commit Watermark Invariant**: Whenever a leader successfully commits and applies a client command to its state machine, the positive acknowledgement (`Processed`) must expose both the resulting `RecordIndex` of the committed command in
  the distributed log and the application's `StateMachineResponse`.
- **Decoupled Session Consistency**: Exposing `RecordIndex` directly in the consensus response contract allows client runtimes, gateway routers, and caching layers to track monotonically increasing commit watermarks without deserializing or
  coupling to application-specific `StateMachineResponse` envelopes, enabling standardized read-your-writes and session-level linearizable consistency across cluster participants.

---

## 8. Ghost Leader Reconfiguration & Learner Convergence Dynamics

### I. Ghost Leader Reconfiguration Invariant

- **Ghost Leader Definition**: When a leader commits a stable configuration change that excludes itself from the target electorate, it enters a transitional ghost leader state. The ghost leader remains responsible for driving followers to
  commit the excluding configuration change, but cannot initiate or lead subsequent configuration transitions.
- **Electorate Validation Scope**: A ghost leader evaluates its retirement condition (verifying that all included learners have committed the excluding configuration change) strictly against the current active configuration's electorate.
  Historical tracking capacities or learners excluded by prior configurations must not influence this eligibility check, preventing stale tracking data from stalling the leader's retirement.
- **Exclusion Rejection Barrier**: If a ghost leader receives a request for a new configuration change from which it is also excluded:
    - If all active learners in the current configuration have committed the excluding configuration change, the ghost leader transitions immediately to `Retiring` and reports its exclusion.
    - If one or more active learners have not yet committed the excluding configuration change, the ghost leader cannot transition to `Retiring` and must reject the request with a status instructing the caller to wait for the ghost leader
      to be deposed (`WAIT_GHOST_LEADER_IS_DEPOTED`), rather than synchronously re-triggering replication loops.

### II. Asynchronous Learner Convergence vs. Quorum Progress Decoupling

- **Quorum vs. Universal Convergence Decoupling**: Replication waves complete successfully as soon as a majority quorum of active peers acknowledges append RPCs. Conversely, leader retirement transitions require universal (100%)
  acknowledgment across all active learners.
- **Asynchronous Retry Decoupling**: When a replication wave completes via majority quorum while lagging or unreachable learners remain pending, retries targeting unreachable learners must be scheduled asynchronously on dedicated timers.
  Ghost leaders must never execute re-entrant, synchronous replication loops during configuration change request handling.

---

## 9. On-Demand Election Protocol, Ballot Invariants & Promotion Dynamics

### I. Multi-Phase Reactive Election Architecture

Unlike monolithic consensus systems with background heartbeat timeouts, leader elections are executed on-demand in coordinated phases:

1. **State Discovery Phase (`HowAreYou` / Pre-Vote)**:
    - An uncoordinated participant (`Isolated`) encountering client commands or leadership loss broadcasts state discovery queries (`HowAreYou`) to all reachable peers in the active configuration.
    - This phase is term-neutral and acts as a reactive Pre-Vote: querying nodes do not advance their terms, ensuring active leaders are never disrupted by exploratory queries from partitioned or restarting nodes.
    - Responding peers expose a snapshot of their local consensus state (`StateInfo`), containing their current term, election rank, commit index, log tail metadata, and current ballot.

2. **Deterministic Candidate Ranking (`decideMyVote`)**:
    - Each participant evaluates contenders using a deterministic, total-order comparison function over `(currentTerm, isLeading, lastRecordTerm, lastRecordIndex, isCandidate, isInCommonConfig, participantId)`.
    - **Precedence Order & Safety Invariants**:
        - `currentTerm`: Higher terms strictly dominate lower terms.
        - `isLeading` over Log Completeness: An active leader (`ER_LEADING`) within the same term takes precedence over other participants regardless of log tail comparisons. Assuming term invariants hold, this avoids unnecessary leadership
          churn, abdications, and re-elections during exploratory discovery.
        - Log Completeness over `isCandidate`: Log completeness (`lastRecordTerm`, then `lastRecordIndex`) strictly dominates candidate status (`ER_CANDIDATE`). Lagging candidates cannot bypass more complete logs held by retiring
          (`ER_RETIREE`) or joining (`ER_JOINER`) participants. If a non-candidate peer holds newer committed entries, the comparison prioritizes that peer's state, preventing the candidate from voting for itself or achieving nomination
          until it absorbs those committed records.
        - `isCandidate` over Non-Candidate: Among participants with equal terms and identical log completeness, active candidates take precedence over passive participants (followers, retirees, joiners).
        - `isInCommonConfig`: Participants belonging to the common configuration set take precedence over non-common peers to preserve configuration stability during joint consensus.
        - `participantId`: Deterministic tie-breaker ensuring that all participants with identical peer views select the exact same candidate.

3. **Candidate Elevation & Preemptive Term Bumping**:
    - When a participant deterministically selects itself and has observed responses from a majority of active participants, it initiates Phase 2.
    - Before issuing vote solicitations, the candidate advances its term to $T_{\text{target}} = \max (T_{\text{observed}}) + 1$, records its vote for itself, and persists $(T_{\text{target}}, \text{votedFor} = \text{self})$ in stable
      storage via the primary state causal fence.

4. **Voting Phase (`ChooseALeader`) & Voter Epoch Fencing**:
    - The candidate solicits explicit votes via `ChooseALeader` carrying $T_{\text{target}}$ and its updated `StateInfo`.
    - Responding peers observing $T_{\text{target}} > T_{\text{local}}$ atomically advance their local term to $T_{\text{target}}$ and clear prior votes in persistent storage before evaluating the vote.
    - Peers evaluate the solicitation against log completeness and single-vote-per-term constraints. If the candidate's log is up-to-date and the voter has not voted for another candidate in $T_{\text{target}}$, the voter persists
      `votedFor = candidateId` and returns an explicit `Vote` at $T_{\text{target}}$.
    - **Local Voter Evaluation Boundary**: Voters evaluate vote solicitations strictly against local log completeness and the single-vote-per-term invariant without dispatching discovery queries (`HowAreYou`) to third-party peers. This
      prevents $O (N^2)$ query cascades and ensures that an unreachable or partitioned minority cannot deadlock majority quorum formation.
    - **Epoch Fencing Effect**: Advancing and persisting $T_{\text{target}}$ permanently fences the voter against all prior terms ($\le T_{\text{target}} - 1$). Any delayed append requests from an old deposed leader at prior terms are
      unconditionally rejected with `StaleTerm`.

5. **Quorum & Role Inauguration**:
    - **Stable Configuration Quorum**: A candidate must receive votes for $T_{\text{target}}$ from a strict majority (`> N / 2`) of active configuration participants.
    - **Transitional Configuration Quorum (Joint Consensus)**: A candidate must receive a strict majority in **both** the old configuration set (`Cold`) and the new configuration set (`Cnew`).
    - If the candidate obtains the required quorum, it transitions through `Promoting` directly into `Leader(term = targetTerm)`. Because the term was already persisted prior to voting, no further term bumping occurs during promotion. If
      quorum is not attained, the participant transitions to `Isolated` to retry with an advanced ballot.

### II. Ballot Mechanics & Single-Vote-Per-Term Invariant

- **Ballot Monotonicity**: Every participant maintains a local monotonic ballot counter (`currentBallot`). The ballot counter distinguishes distinct election rounds within the participant's lifecycle.
- **Ballot Invalidation on Disruption**: The ballot counter is bumped whenever an election round fails to establish a leader, whenever higher ballots are observed in peer RPC responses, or when client retries incite new election rounds.
  Advancing the ballot immediately purges all cached peer state information.
- **One Vote per Ballot & Term Invariant**: A participant may cast at most one vote per ballot round and at most one vote per term. When a vote is granted, the candidate's identity is recorded in persistent storage (`Workspace.votedFor`)
  alongside the term. Advancing to a higher term resets the recorded vote.
- **Split-Brain Prevention**: Because voters persist their term and granted vote before responding to `ChooseALeader`, intersecting majorities across terms are prevented from acknowledging divergent logs, strictly preserving Raft's Leader
  Completeness and State Machine Safety.

### III. Promotion Phase (`Promoting`) & Term Bumping Invariants

- **Hidden Transitional Substate**: `Promoting` is a transient, internal role entered immediately upon securing an election quorum. Outside participants never observe a node in the `Promoting` state because inbound RPCs are held pending
  during this interval.
- **Pre-Election Epoch Bumping**: Unlike designs that defer term bumping until after quorum acquisition, the term is bumped and persisted *prior* to Phase 2 vote collection. `Promoting` serves to finalize asynchronous pipeline
  initialization and causally anchor active configuration state before client command processing commences.
- **Strict Role-Exit Cleanliness**: All active replication waves and retry tokens are canceled upon role transitions. Role references are updated before invoking exit handlers to ensure synchronous cancellation continuations immediately
  observe that the node is no longer in the previous role.
- **Leadership Inauguration**: Once causal state anchoring completes, the participant instantiates and transitions to `Leader(term = targetTerm)`.

### IV. Out-of-Band Commit Index Absorption Invariant (Log Matching Safety)

- **Deadlock Vulnerability with Ineligible Candidates**:
    - Because log completeness strictly dominates candidate rank (`completeness > isCandidate`), an active candidate in Phase 1 discovery will lose the election to a retiring (`ER_RETIREE`) or joining (`ER_JOINER`) participant whenever that
      peer possesses a higher commit index or a more complete log tail.
    - Retiring and joining participants are structurally ineligible for leadership (`rank != ER_LEADING` and cannot transition to `Leader`). Consequently, they will never broadcast `AppendEntries` to advance followers' commit indices.
    - If candidates were restricted to standard Raft leader-driven commit index advancement, candidates would repeatedly yield blank votes or select ineligible peers, causing the cluster to deadlock indefinitely.
- **Out-of-Band Advancement via Raft's Log Matching Property**:
    - To break election deadlocks without requiring heartbeats or active leaders, active participants evaluate out-of-band commit index absorption (`absorbHigherCommitIndexFromPeers`).
    - By Raft's Log Matching Property: *If two entries in different logs have the same index and term, the logs are identical in all preceding entries.*
    - An active participant safely advances its local `commitIndex` out-of-band to a peer's `peerCommitIndex` without leader intervention if and only if:
      $$\text{firstEmptyRecordIndex} > \text{peerCommitIndex} \quad \text{and} \quad \text{getRecordTermAt} (\text{peerCommitIndex}) == \text{peerStateInfo.termAtCommitIndex}$$
    - Matching the term at `peerCommitIndex` provides mathematical proof that all entries up to `peerCommitIndex` in the local log are identical to the committed entries on the peer.
- **Post-Absorption Dynamic Reconfiguration**:
    - Once `commitIndex` is advanced, the node triggers local application of newly committed commands and configuration entries.
    - When the absorbed commit index covers the `StableConfigChange` that finalized a retiree's exclusion, the candidate updates its active configuration, removes the retired peer from consideration, and becomes eligible to secure majority
      quorum and inaugurate leadership in the subsequent election round.

---

## 9. Configuration Change Response & Ballot Propagation Invariants

- **Terminal vs. Non-Terminal Response Partitioning**: Configuration change responses are strictly partitioned into terminal and non-terminal variants:
    - **Terminal Responses**: Represent final consensus outcomes (completed or already matching configuration changes) that definitively satisfy the requester and terminate retry loops. Terminal responses do not carry election ballot
      metadata.
    - **Non-Terminal Responses**: Represent intermediate rejections, redirections, or lost tracking states. Non-terminal responses carry the highest observed election ballot to propagate election round counters across sequential node
      interactions during client discovery.
- **Ballot Propagation Independence**: The receipt of an observed ballot via prior responses allows subsequent nodes in a discovery loop to fast-forward local election ballots and purge obsolete peer cache entries without triggering
  unprovoked ballot increments.
- **Reconfiguration Quiescence Decoupling**: Successful replication and commitment of a stable configuration change that excludes the current leader drives participant retirement and persistent workspace release. The terminal response is
  returned upon majority consensus commitment without requiring post-commit causal anchoring against persistent state.

## 10. Decoupled Mutation Contract & Causal State Synchronization

To guarantee strictly sequential state transitions without the stalling overhead of Thread/Actor-blocking or the race-condition vulnerabilities of Mutexes (lock-and-release), the consensus participant wraps its PrimaryState inside a
CausalFence. This acts as a non-blocking promise queue dedicated solely to state synchronization.

### The Game-Changing Invariant & Temporal Window of Causal Safety

By attaching continuations synchronously to the Capture returned by advance or causalAnchor, the algorithm guarantees execution ordering. Continuations inherently execute in the exact causal timeline slot following the anchored mutation,
ensuring they receive the perfectly synchronized, fresh state.

Crucially, this guarantee is strictly bounded by the **Temporal Window of Causal Safety**:

- The causal guarantee holds only during the synchronous execution of a consumer synchronously subscribed to the returned `Capture`.
- Once execution yields, crosses an asynchronous completion (such as `waitRecordBecomesCommitted`), or defers via `Capture_defer`, the captured `PrimaryState` reference is obsolete and outside the causal window.
- Any subsequent derivation of configuration, role synchronization, or invariant verification that inspects `PrimaryState` after an asynchronous boundary or deferred dispatch MUST acquire a fresh anchor via
  `primaryStateFence.causalAnchor()` and perform derivations strictly within that anchor's synchronous continuation.

### The Decoupled Mutation Contract (An Asymmetric Performance Decision)

While `CausalFence` enforces the sequential timeline, the consensus algorithm specifically enforces an asymmetric **Decoupled Mutation Contract** across all internal reactive notification boundaries (such as `CommitIndexAwaiter` fulfillment
and state event observers).

This contract strictly mandates that observers and continuation callbacks MUST NOT synchronously mutate the protected `PrimaryState` (`CausalFence`) within their notification context. Any consequential state mutation or side-effect
resulting from a notification (such as appending records, transitioning configuration phases, or initiating log compaction) must be decoupled from the notification stack—either by self-deferring via `sequencer.Capture_defer` or dispatching
via `sequencer.run`.

#### 1. Rationale: The Hot-Path Performance Asymmetry

The Decoupled Mutation Contract is not an accidental restriction; it is an intentional architectural tradeoff designed to optimize the primary throughput path of the consensus engine:

- **Zero-Cost Happy Path for Client Commands**: In a production consensus cluster, over 99.999% of commit index advancements represent client command replication. On this hot path, when a command commits, the leader applies the command to
  the state machine, notifies client awaiters, and returns the response. Applying committed records to the state machine does not mutate the `PrimaryState` or touch the `CausalFence`. Because the contract guarantees that awaiters do not
  mutate the fence, `recalculateCommitIndex` can notify awaiters synchronously on the current thread stack. This eliminates task-queue allocations, trampoline scheduling, and dispatch latency, allowing the happy path to execute with
  absolute minimum overhead.
- **Asymmetric Caller Burden**: In exchange for zero-overhead execution on the dominant hot path, callers that execute state-mutating logic bear the architectural burden of explicit self-deferral. Seldom-traveled paths—such as the fallback
  branches of command replication (appending no-op records) and configuration change completions (advancing from transitional to stable configurations)—must explicitly wrap their continuations in `Capture_defer`. This trades caller
  simplicity on rare paths to maximize performance on the critical path.

#### 2. Rejection of Alternative 1: Synchronous Inline Re-Anchoring

A naive alternative to the contract would be to permit observers to mutate the state synchronously, under the assumption that the notifier could simply re-anchor after calling each observer. This alternative is structurally impossible:

- **The Asynchronous Promise Reality**: `CausalFence.causalAnchor()` is fundamentally an asynchronous primitive that returns a `Capture[PrimaryState]`, not a synchronous value. The moment an advance is enqueued onto the fence (such as an
  observer appending an entry to disk), the new state does not exist synchronously; it is an in-flight operation awaiting persistence or pipeline sequencing.
- **Inability to Resume Synchronously**: A synchronous loop (such as `recalculateCommitIndex` iterating over fulfilled awaiters) cannot block or inline-await an asynchronous promise. To pass a freshly anchored state to subsequent awaiters,
  the entire remainder of the loop would have to be severed and converted into an asynchronous callback chain (`awaiter.seizeWith(causalAnchor())`), destroying synchronous stack execution.

#### 3. Rejection of Alternative 2: Dynamic Mutation Detection

Another tempting design is dynamic fallback detection: having `recalculateCommitIndex` notify synchronously by default, but dynamically check if the fence was modified
(`!primaryStateFence.committedState.is(primaryState0) || !primaryStateFence.isEmpty`), re-anchoring remaining awaiters only when a mutation occurs. This alternative was formally rejected due to two fatal concurrency hazards:

- **Intra-Batch Turn Splitting (Execution Reordering)**: If a commit index advancement satisfies multiple awaiters simultaneously (e.g., a configuration change and subsequent client commands), and an early awaiter mutates the fence,
  dynamically re-anchoring subsequent awaiters via `seizeWith(causalAnchor())` defers them to future sequencer turns. Consequently, awaiters that achieved consensus within the exact same batch are arbitrarily split across different
  execution frames, violating linear dispatch expectations.
- **Stack Re-Entrancy and State Invalidation**: Allowing an observer to mutate the fence synchronously executes arbitrary state mutation logic while `recalculateCommitIndex` is actively running on the call stack. The observer's callback
  could alter roles (triggering abdication or follower transitions), schedule pipeline drives, or mutate the pending awaiter collection while the outer loop is still traversing it.

#### 4. The Resulting Purity Invariant

By enforcing the Decoupled Mutation Contract, `recalculateCommitIndex` maintains the strict assertion:
`assert(primaryStateFence.committedState.is(primaryState0))`
This mathematical invariant proves that commit index calculation and notification is strictly pure with respect to the causal state. It guarantees that a valid `PrimaryState` reference remains pristine and temporally safe from the beginning
of the notification loop to the end, eliminating re-entrancy bugs, stale-state references, and batch turn-splitting.

## 11. Diagnostic Inspection & Causally Consistent Observation

- **Quiescent Diagnostic State Boundary**: Diagnostic observation of internal participant and role state (such as peer learner replication progress and configuration tracking) must not bypass single-threaded sequencer confinement or observe
  partially applied causal mutations.
- **Inter-Step Temporal Consistency**: In discrete-event simulation, test harnesses, and external diagnostics, inspecting the active role's diagnostic snapshot is causally consistent at step boundaries (whenever a discrete sequencer step
  completes or when the runnable queue is empty). At these boundaries, all synchronous causal chain derivations (including active configuration changes and peer replication metrics) are guaranteed to be in identical lockstep.

## 12. Host Bridge RPC Completion & Transport Termination Invariants

- **Asynchronous Completion Guarantee**: The consensus engine delegates all inter-participant communication to the host-provided transport bridge (`ClusterParticipant`). All outbound RPC methods (`howAreYou`, `chooseALeader`,
  `appendRecords`) return an asynchronous completion handle (`Capture[R]`) that contractually must resolve to either `Success` or `Failure`. The consensus state machine does not maintain internal per-request timeout watchdogs, relying
  strictly on the host layer to bound request lifecycles.
- **Aggregation vs. Pipelined Quorum Resilience**: While pipelined log replication makes forward progress upon reaching quorum without waiting for lagging peers, discovery and election phases aggregate responses across candidate peers
  (e.g., collecting state reports across the full configuration). If an outbound query is dropped without notifying the caller's completion observer of a transport failure, the aggregation pipeline awaits completion indefinitely, stalling
  the single-threaded participant actor.
- **Simulation Harness Equivalence**: In discrete-event simulation and test environments, dropping an in-flight network packet must resolve the sender's pending completion observer with a transport failure to emulate transport timeouts or
  connection termination. Silent removal of packets without failure resolution creates an artificial permanent deadlock that violates the reactive completion contract.
- **Uniform Transport Failure Handling**: The consensus protocol is fail-silent and retry-driven with respect to transport errors. All network-level failures are handled uniformly regardless of the underlying exception class; specific
  exception discrimination provides no algorithmic differentiation within the consensus engine.
