---
type: "Concept"
title: "Consensus Simulation & Test Framework"
description: "Architecture, three-tier boundary, modular subsystems, hierarchical process scheduling, decoupled virtual clocks, and deterministic replay journal of the consensus test framework."
tags: [ "user-guide", "consensus", "testing" ]
timestamp: "2026-10-22T19:00:00Z"
---

# Consensus Simulation & Test Framework

This document specifies the architecture, three-tier separation of concerns, modular subsystem decomposition, hierarchical process scheduling model, decoupled virtual clock mechanics, and deterministic replay journal of the test framework
used to verify [Lazy Multi-Raft Consensus Architecture](lazy-multi-raft-consensus.md).

## 1. Concurrency Verification Objectives & Deterministic Simulation

Concurrent consensus protocols exhibit edge-case races arising from unpredictable message delays, reordering, asymmetric partitions, and uncoordinated node execution. Verifying these invariants through asynchronous multi-threaded runtimes
introduces non-determinism, irreproducible failures, and flaky wall-clock timeouts.

The consensus test framework replaces multi-threaded runtimes and asynchronous networks with a deterministic, single-threaded pseudo-random simulation harness. Guided by an explicit pseudorandom seed, the framework explores the full space
of parallel interleavings while guaranteeing strict bit-for-bit reproducibility across property-based testing runs, regression suites, and interactive debugging sessions.

## 2. The Three-Tier Architectural Boundary

To prevent verification oracles and administrative coordination from contaminating protocol invariants or granting consensus state machines omniscient cluster knowledge, the test framework enforces a strict three-tier architectural
boundary:

### Tier 1: Physical Environment & Network Substrate

Tier 1 encapsulates the simulated physical reality. It implements point-to-point communication channels, integer virtual time progression, asynchronous storage queues, and single-threaded node execution stepping. Components in Tier 1 expose
low-level physical primitives: delivering or dropping packets, advancing clock ticks, completing or failing storage operations, and stepping a node's single-threaded sequencer. Tier 1 components enforce no consensus protocol logic, client
coordination policies, or scheduling heuristics.

### Tier 2: External Client & Administrative Control Plane Gateway

Tier 2 models external actors interacting with the cluster with zero visibility into node internals. It mediates client command submissions, tracks in-flight reconfiguration proposals, and manages administrative cluster topology. The
administrative control plane maintains external peer availability views (`memorizedOtherProbableParticipants`) strictly through protocol-visible client responses (`SuccessfullyChanged`, `AlreadyChanged`, `AnotherChangeInProgress`,
`RedirectTo`) and outbound lifecycle events (`onNodeQuiesced`, `onActiveElectorateChanged`). External coordinators never inspect internal node state machines or query oracle data structures to make routing or failover decisions.

### Tier 3: Omniscient Invariant Oracles

Tier 3 consists of quarantined verification components reserved exclusively for test safety assertions. The invariant oracle maintains causally consistent observation across all participants to verify fundamental safety properties (Leader
Uniqueness, Log Matching, State Machine Safety, and Highest Committed Record Index). Participant state machines, host cluster bridges, and Tier 2 administrative coordinators are strictly forbidden from reading or depending on Tier 3 oracle
state.

## 3. Modular Subsystem Decomposition

The simulation infrastructure is decomposed into dedicated, single-responsibility subsystems under the `readren.consensus.env` package:

- **`SimulatedDomain`**: Defines strongly typed domain identifiers (`NodeId`, `NodeRef`, `ClientRef`, `VirtualTime`, `PacketId`, `StorageOpId`, `WakeUpTokenId`), consensus RPC messages, packet hierarchies (`RequestPacket`,
  `ResponsePacket`), delivery outcomes (`Delivered`, `Dropped`, `ChannelEmpty`), drop reasons, and client/reconfiguration status algebraic data types.
- **`SimulatedNetwork`**: Manages point-to-point directional FIFO channel buffers (`mutable.Map[(NodeId, NodeId), mutable.ArrayDeque[Packet]]`), per-channel packet delivery and drop mechanics, channel traffic statistics, and RPC lifecycle
  interceptors.
- **`VirtualClock`**: Implements discrete integer virtual time progression (`virtualTime`, `ticksPerMilli`), wake-up token registration (`pendingWakeUpsMap`), time advancement (`advanceTime`), and timer callback execution (`triggerWakeUp`).
- **`SimulatedStorageSubsystem`**: Simulates asynchronous persistence layers with pending operation queues (`pendingPersistenceMap`), manual and automatic save completion (`completeStorageSave`, `failStorageSave`), and auto-succeed toggles.
- **`EnvironmentNode`**: Encapsulates the runtime harness for an individual consensus participant, managing its single-threaded [Sequencer Core](sequencer-core.md) (`StepDoer`), lifecycle state (`isDown`, `isAdministrativeTarget`), and
  internal subsystem adapters (`TestStateMachine`, `TestWorkspace`, `TestStorage`, and `TestClusterParticipant`).
- **`SimulatedControlPlane`**: Implements the Tier 2 administrative control plane, orchestrating dynamic offline node bootstrap (`startDynamicNode`), terminal quiescence notifications (`onNodeQuiesced`), and surviving peer view updates
  without omniscient cross-node inspection.
- **`EnvironmentJournal`**: Provides a deterministic operation recording and replay engine supporting transactional operation boundaries (`recordOrExecute`), checkpoint tagging, and reversible timeline navigation (`undo`, `redo`, `reset`,
  `restoreTag`).
- **`ConsensusOracle`**: Implements the quarantined Tier 3 verification oracle, tracking global leadership terms, committed records, applied state machine commands, and executing cross-node log matching validations.
- **`ConsensusEnvironment`**: Serves as the composition facade coordinating all subsystems (`network`, `clock`, `storage`, `nodes`, `controlPlane`, `journal`, `oracle`) while exposing top-level convenience delegates for test suites.

## 4. Two-Tier Hierarchical Process Stepper

A flat discrete-event scheduler that selects uniformly across all active entities suffers from domain dilution in distributed environments. In a cluster of $N$ nodes, there exist up to $N (N-1)$ directional communication channels. During
message broadcast phases, active network channels outnumber active nodes by an order of magnitude. Under a flat selection policy, node execution frequency drops proportionally to the number of traveling packets, artificially throttling
compute progression and distorting the relative speed of nodes versus network transport.

To provide complete processing speed independence for each parallel entity while maximizing interleaving variance, the scheduler implements a Two-Tier Hierarchical Process Scheduler:

### Tier 1: Domain Arbitration

At each simulation step, the scheduler identifies the active domains:

- **Compute Domain**: Active when at least one node has pending tasks in its [Sequencer Core](sequencer-core.md) queue or registered active timers.
- **Network Domain**: Active when at least one directional channel contains in-flight packets.

When both domains are active, the scheduler arbitrates between them using a configurable probability parameter $P_{\text{compute}}$:

$$\text{Domain} = \begin{cases} \text{Compute Domain} & \text{with probability } P_{\text{compute}} \\ \text{Network Domain} & \text{with probability } 1 - P_{\text{compute}} \end{cases}$$

If only one domain contains active entities, that domain is chosen unconditionally. This arbitration decouples computational throughput from network density, preventing packet bursts from starving node computation.

### Tier 2: Entity Selection & Execution

Once a domain is selected, the scheduler selects uniformly among all active entities within that domain:

- **Compute Domain Execution**: The scheduler selects uniformly among all active nodes ($P (n_i) = \frac{1}{|ActiveNodes|}$). The selected node executes one discrete "CPU cycle":
  - **Sequencer Progress**: If the node has a runnable task enqueued in its single-threaded sequencer, it executes exactly one runnable.
  - **Local Clock Progression**: Any active retry timer registered by this node decrements its remaining tick counter by a normalized cycle quantum ($\delta_{\text{node}}$).
  - **Timer Maturity**: If an active timer's remaining tick counter reaches zero, its registered callback is enqueued into the node's sequencer queue for subsequent execution.
- **Network Domain Execution**: The scheduler selects uniformly among all non-empty directional channels ($P (c_{i \to j}) = \frac{1}{|ActiveChannels|}$). The selected channel consumes strictly its head packet, enforcing the per-channel
  FIFO invariant:
  - **Packet Drop Evaluation**: The head packet is evaluated against drop probabilities (differentiating request drops from response drops). If dropped, the packet is removed from the channel without delivery.
  - **Packet Delivery**: If not dropped, the packet is delivered directly into the recipient node's sequencer queue.

Because each node and channel represents an independent slot within its respective domain, processing speeds across nodes are completely decoupled: one node can execute multiple cycles while a peer remains idle, and channels deliver
messages at varying rates without synthetic global synchronization.

## 5. Decoupled Local Virtual Clock & Local Timer Mechanics

Distributed consensus nodes in the lazy multi-raft architecture contain no internal concepts of wall-clock time, system timestamps, or monotonic hardware clocks; state machines interact with time exclusively through an abstract wake-up
registration contract (`requestWakeUp`).

### Decoupled Node-Relative Time Progression

Alternative approaches to virtual time introduce critical flaws in property-based testing:

- **Distributed Lamport Clock Graphs**: Tagging packets with departure timestamps and maintaining causal clock hops across all nodes constructs complex, stateful timestamp tracking across the entire network to govern only a small number of
  leader retry callbacks.
- **Queue-Depth Ratio Scaling**: Heuristically scaling timeout durations based on instantaneous queue depths causes severe ratio fluctuations. When network queues swell, timers scale down drastically and expire before RPC round-trips can
  complete, rapidly exhausting retry limits and generating false-positive test failures on valid code. Conversely, when activity subsides, timers scale up to tens of thousands of skips, resulting in step-limit starvation hangs.

The hierarchical stepper resolves these issues by anchoring timer progression strictly to node execution cycles. A node's timers advance if and only if that node is allocated compute cycles. If a node is scheduled frequently, its timers
mature quickly; if a node is neglected, its timers progress slowly.

### Zero Head-of-Line Blocking

Active timers are tracked independently in a dedicated per-node timer registry and do not reside within the node's primary sequencer mailbox. Immediate sequencer tasks execute in strict FIFO order without being obstructed by unexpired
timers. Timers mature independently during node CPU cycles and enqueue their callbacks into the sequencer only when their tick counters reach zero.

### $O (1)$ Quiescence Fast-Forwarding

When all directional network channels are empty and no node possesses pending sequencer tasks, but active timers remain registered across the cluster, the system is quiescent and waiting for timeout expiration.

Rather than stepping idle compute cycles, the scheduler evaluates the global timer state:

$$\Delta = \min_{n \in Nodes, t \in Timers_n} t.\text{remainingTicks}$$

The scheduler advances cluster virtual time by $\Delta$ in $O (1)$ time, subtracts $\Delta$ from all active timers across all nodes, and triggers all timers that reach zero into their respective node sequencers. This breaks quiescence
instantaneously without wasted simulation cycles while preserving exact causal event order.

## 6. Deterministic Journal Replay & Reset Completeness Invariant

The test framework includes an operation journal (`EnvironmentJournal`) supporting timeline rollback (`undo`), forward re-execution (`redo`), and checkpoint tag restoration (`restoreTag`) for interactive debugging and failure analysis.

- **Transactional Recording Boundary**: All mutating environment operations pass through `recordOrExecute`, assigning monotonic sequence identifiers and maintaining applied and undone operation stacks.
- **Deterministic Journal Replay & Reset Completeness Invariant**: For replay to be strictly isomorphic to the original execution, environment reset must purge all subsystem state across all three tiers—including physical network channels,
  virtual clock timers, suspended persistence queues, client session counters, in-flight reconfiguration tracking tables, and oracle history buffers—and execute baseline initialization with journal recording suppressed so that replayed
  operations never accumulate duplicate journal entries or observe residual state from rolled-back branches.

## 7. Dynamic Node Lifecycle & Continuous Invariant Verification

The framework supports dynamic cluster reconfiguration and rigorous safety verification:

- **Randomized Seed Masks & Dynamic Joins**: Tests generate randomized boolean masks for initial node configurations. Nodes marked offline remain unstarted (`isDown = true`, `_participant = null`) at simulation launch. When dynamic
  reconfiguration proposals incorporate offline nodes, the administrative control plane bootstraps them dynamically (`startDynamicNode`) and broadcasts updated peer views to surviving participants.
- **Terminal Quiescence Irreversibility**: Quiescence is terminal and irreversible (`isDown = true`, `_participant = null`). Quiesced nodes are never automatically restarted; their release events notify `controlPlane.onNodeQuiesced` to
  update surviving peer views.
- **Continuous Per-Step Invariant Verification**: Core consensus invariants—including Leader Uniqueness (at most one leader per term), State Machine Safety (identical committed log entries produce identical state machine states), and Log
  Matching (identical index and term imply identical prefix history)—are asserted after every single discrete simulation step. When an invariant is violated, the simulation halts immediately at the exact causal step that caused the
  divergence.
