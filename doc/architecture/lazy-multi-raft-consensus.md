---
type: "Concept"
title: "Lazy Raft Consensus Protocol Specification"
description: "Specification of the reactive, heartbeat-less single-group consensus state machine, covering role lifecycles, on-demand elections, dynamic membership, log compaction, and causal state synchronization."
tags: [ "user-guide", "consensus", "sequencer" ]
timestamp: "2026-10-31T20:00:00Z"
---

# Lazy Raft Consensus Protocol Specification

This document provides the formal architectural specification for the reactive, single-group **Lazy Raft** consensus protocol engine implemented in `consensus/core/src/main/scala/ConsensusParticipantSdm.scala`. For cluster-wide topology,
control/data plane separation, and multi-raft sharding architecture, see [Multi-Raft System Architecture](system-architecture.md). For verification harnesses and simulation models,
see [Consensus Simulation & Test Framework](consensus-test-framework.md).

---

## 1. Participant Exclusion, Retirement Dynamics & Quiescence Protocol

When cluster membership changes, nodes removed from the active cluster topology must be safely transitioned from active consensus participation to complete network quiescence without violating linearizability or causing election deadlocks.

### I. Participant Role Lifecycle, Unanchored Joining & Electorate Transition Overview

A participant node in the consensus engine moves through distinct operational roles during its lifecycle:

```
[ Starting ]
     |
     +---> (Included in storage config) ---> [ Isolated ] ---> [ Candidate / Follower / Leader ]
     |                                                                      |
     +---> (Unanchored / not in config) ---> [ Joining ]                  | (Excluded by Sole Electorate Change)
                                                  |                         v
                                                  | (Included via Append) [ Retiring ]
                                                  v                         |
                                             [ Follower ]                   v
                                                                       [ Quiesced ] (Zero Network Load)
```

- **Active Roles (Leader, Follower, Candidate):** Process client commands, participate in leader elections, and replicate log entries.
- **Starting Role:** Initializes consensus state from local storage without requiring external knowledge of configuration indices. If the node's identifier is included in its latest recovered configuration, it transitions to `Isolated`. If
  unanchored (not included in local storage configuration), it transitions directly to `Joining`.
- **Joining Role (Unanchored Learner):** Operates as a passive, non-voting learner awaiting log replication. It does not solicit votes or participate in leader elections. Upon receiving replicated log records or a snapshot containing the
  electorate change that includes it, the joiner fuses the records into its local log, evaluates inclusion, and transitions directly to `Follower`.
- **Retiring Role:** Entered when a sole electorate change excludes the participant from the cluster. The participant stops proposing client commands and cannot win elections for subsequent terms. It remains active solely to catch up its
  local log to the excluding electorate entry.
- **Quiesced Role:** Terminal resting state where the participant halts all background network traffic, timers, and RPC handling.

---

### II. Retirement Replication Driver Architecture

When an electorate transition excludes one or more participants (moving from a joint electorate to a sole electorate), the leader initiates targeted replication processes (pipelines) to push records to the excluded participants until they
reach the retirement boundary (the SoleElectorateChange that excluded them).

- **Leader Push over Follower Pull:** The cluster leader actively drives replication to non-active participants. This eliminates the need for a pull-based `RetirementDriver` on the follower. Because the new leader relies on the cluster
  transport layer (via `ClusterBridge.getOtherProbableParticipants`) to discover all live nodes, it can spin up pipelines for any non-active nodes (including those excluded by past leaders). Consequently, an abdicating leader can safely
  tear down its own pipelines, knowing the new leader will pick up the slack.
- **Initial Retirement Driving on Takeover:** When a newly crowned leader takes over, its registry of active retirement drivers is initially empty. If the leader takes over under an already-committed configuration, it immediately initiates
  replication pipelines for all historical non-active nodes (`Universe \ Cnew`). If the leader assumes office during an uncommitted configuration transition (an uncommitted joint or sole electorate change), it must defer initializing
  retirement pipelines until that transition's sole electorate change is committed; upon that commitment, it must drive the retirement of all historical non-active nodes (`Universe \ Cnew`), rather than treating itself as an incumbent
  leader that only drives newly excluded peers (`Cold \ Cnew`). Subsequent configuration transitions occurring continuously within that leader's tenure only need to initiate pipelines for newly excluded peers (`Cold \ Cnew`), as earlier
  unquiesced retirees remain tracked in its registry.
- **Strict Upper Boundary (TargetIndexBound):** The pipeline restricts the logs sent to the participant strictly up to the log index of the excluding electorate change (`secIndex`).
- **LeaderCommit Clamping & Ghost Records:** When sending `AppendRecords`, the `leaderCommit` sent to the excluded participant MUST NEVER exceed the `secIndex`. If the retiring participant has unverified, trailing "ghost" records in its log
  (e.g., from a past term where it was active), an empty `AppendRecords` (bounded by `secIndex`) will not truncate them. If the leader sent a `leaderCommit > secIndex`, the participant would execute
  `commitIndex = min(leaderCommit, indexOfLastNewEntry)` and falsely commit its ghost records, corrupting its state machine. Therefore, `requestLeaderCommit` is tightly clamped.
- **Snapshot Fallback (Wiping Ghost Records):** If the leader's log has been compacted past the `secIndex`, it cannot send a `requestLeaderCommit <= secIndex` (as `getRecordTermAt` would throw an `IndexOutOfBoundsException`). Instead, the
  pipeline falls back to sending an `InstallSnapshot`. This safely resolves the dilemma: because the snapshot fully replaces the follower's log, all unverified trailing ghost records are wiped out. The follower safely adopts the verified
  state (which includes its own exclusion) and its `commitIndex` safely advances past `secIndex` without risking corruption.
- **Stateless Retiring Role:** The follower only transitions to the `Retiring` role *after* it has successfully committed the excluding electorate change. Because it has already committed its own exclusion, it has no need to process further
  records. Thus, the `Retiring` role is appropriately stateless and rejects all incoming `AppendRecords` or `InstallSnapshot` requests (unless they contain a new electorate change that re-includes it). This rejection signals the leader's
  pipeline to terminate.
- **Non-Fatal Pipeline Aborts (`GracefullyReleased`):** Background pipelines rely on `CausalFence` operations (like `causalAnchor()`). These operations can fail non-fatally with a `GracefullyReleased` exception if the role loses
  statefulness (e.g., the leader demotes). When this happens, the capture fails gracefully, and the replication pipeline silently and safely halts, recognizing that the leader role is shutting down.
- **Driver Lifecycle & Registry Cleanup:** The leader maintains an active tracking registry of all running retirement drivers. The pipeline is terminated and the participant is removed from tracking once it acknowledges that it has
  committed the excluding configuration change (e.g., by responding with an `AppendResult_Rejected` citing the `RETIRING` role).

---

### III. Quiescence Protocol & Convergence Preconditions

Quiescence authorization ensures that a retiring participant does not shut down prematurely before its peers have recognized its exclusion and authorized its departure.

- **Quiescence Authorization Flow:**
    1. Upon committing an excluding electorate change, the leader broadcasts quiescence authorization permissions (`PermitQuiesce`) to all excluded participants that have not been decommissioned due to unreachable retry exhaustion.
    2. Retiring participants acknowledge the permission and record the authorized electorate change index.
    3. If initial permission requests fail due to transient network drops, the leader retries permission delivery up to a configured retry limit before clearing remaining unacknowledged permissions.
- **Convergence Preconditions for Quiescence:** A participant in the `Retiring` role can transition to `Quiesced` if and only if the following conditions converge:
    1. **Role Eligibility:** The participant's current role is `Retiring`.
    2. **Outbound Pipeline & Authorization Encapsulation:** Active retirement log replication pipelines and outbound quiescence authorization tracking are strictly encapsulated within the `Leader` role lifecycle; because ghost leaders
       remain in office until all outbound quiescence permissions are acknowledged or retry limits are exhausted, all pipelines, wake-up retry tokens, permission tracking maps, and acknowledgment subscriptions terminate unconditionally upon
       leaving `Leader`, ensuring retiring participants are stateless and never harbor active or leaked drivers or unacknowledged permission tracking.
    3. **Permission Grant:** Explicit quiescence authorization has been granted by the cluster (received from an active leader, or self-authorized by a vanishing ghost leader once all peers have acknowledged or retries are exhausted).
    4. **Index Verification:** The authorized electorate change index is greater than or equal to the participant's excluding electorate index.
    5. **Ghost Leader Term Promotion & Non-Abdication Invariant Under Vanishing Electorates:** When finalizing an empty electorate configuration change ($C_{\text{new}} = \emptyset$), the ghost leader is the sole terminal coordinator of
       cluster shutdown; crowning a successor leader is structurally impossible. Consequently, the ghost leader must ignore higher terms observed in outbound replication responses (such as `AppendResult_Rejected`) or uncoordinated discovery
       queries (`HowAreYou`), refusing to abdicate. To resolve term disparity against lagging peers whose persistent terms were speculatively inflated by aborted candidate inquiries, the ghost leader promotes its own tenure term to match
       the highest observed peer term ($T_{\text{leader}} \leftarrow T_{\text{peer}}$) without stepping down or triggering an election. This term promotion satisfies the recipient's term monotonicity precondition
       ($T_{\text{inquirer}} \ge T_{\text{local}}$), enabling lagging peers to ingest the committed configuration, transition to `Retiring`, and receive quiescence authorizations. The ghost leader only abdicates if it receives an
       authoritative inbound append from a higher-term leader.

---

### IV. Retiring Participant Election StateInfo Invariant

- **Excluding-Electorate Index Bounding in StateInfo:** To prevent election deadlock during cluster electorate transitions, a participant in the retiring state must report the term and index of the excluding electorate change as its
  committed state information (`StateInfo`), even if its local log contains committed records at higher indices.
- **Election Deadlock Prevention:** Reporting post-exclusion log indices during election coordination would allow an excluded node with higher log indices to block active cluster members from achieving a decisive election outcome. Bounding
  the exposed state to the excluding electorate change ensures active participants can elect a valid leader without being blocked by retiring nodes.

---

### V. Per-Peer In-Flight Append Backpressure & Domain Result ADT

- **Monotonic Serial Generation:** Every append RPC emitted to a learner is tagged with a monotonically increasing serial number per peer.
- **Self-Healing Response Acknowledgement:** The leader tracks the highest acknowledged append serial number per peer. Processing a higher serial number automatically acknowledges all preceding in-flight requests for that peer.
- **In-Flight Bound Enforcement:** The difference between the last emitted append serial number and the last acknowledged append serial number defines the in-flight count. When this count reaches the configured threshold
  (`maxInFlightAppendsPerPeer`), further emissions to that peer are deferred.
- **Domain Outcome Safety:** Append RPC outcomes are explicitly typed as ADT variants (`Accepted`, `Rejected`, `SkippedDueToBackpressure`, `SkippedOutOfConfiguration`, `Failed`) eliminating magic-number status flags and heap-allocated error
  wrappers.
- **Backpressure Deferral Invariant:** Deferring an append request due to in-flight backpressure emits a `SkippedDueToBackpressure` domain result. Processing `SkippedDueToBackpressure` is a strict no-op on peer progress state, leaving
  pessimistic and optimistic replication indices completely unchanged.

---

## 2. Commitment, Universal Log Fusion & Log Compaction Safety Invariants

### I. Strict Commitment Verification Before State Machine Application

- **Uncommitted Entry Barrier:** A leader must never apply a command record to its state machine, respond success to a client, or trigger log compaction based solely on peer RPC transport success.
- **Raft §5.4.2 Transitive Commitment Constraint:** Log entries from previous terms cannot be committed directly by majority acknowledgment alone. They can only be committed indirectly by committing a log entry from the leader's current
  term.
- **Upfront Current-Term Entry Insertion for Prior-Term Records:** Under Raft §5.4.2, a leader cannot advance its commit index directly on an uncommitted entry appended in a previous term. If an uncommitted entry (such as a recovered sole
  electorate change) has a term strictly lower than the leader's active term and no subsequent entry in the active term exists in the log, the leader must append a current-term no-op transition entry upfront before awaiting commitment.
  Awaiting commitment directly on a previous-term entry without a current-term entry in the log deadlocks, as commitment verification rules permanently refuse to advance the commit index.
- **In-Flight Electorate Transition Pipeline Serializability:** Upon assuming leadership with an uncommitted electorate change in the persistent log, the replication pipeline driving that electorate change to commitment must be linked to
  the leader's in-flight electorate change completion handle. Subsequent electorate change requests must serialize behind this recovery pipeline. Permitting subsequent electorate changes to execute before the recovered electorate change
  reaches the commit index causes false transitional-phase rejections, as the cluster electorate remains derived from the uncommitted joint state.
- **Commit Watermark Barrier Invariant:** Awaiting record commitment functions strictly as a monotonic watermark barrier, never an RPC-style request-reply operation. The barrier never resolves with an uncommitted or failure status while the
  leader remains in office. When the barrier resolves for an active leader, the target record index is mathematically guaranteed to be committed. In the event of quorum loss or network partition, the barrier remains suspended indefinitely
  until quorum is restored or the leader is deposed and exits office, eliminating phantom retry loops in post-barrier continuations.
- **Peer vs. Retiree Replication Pipeline Disjunction:** An active peer participating in the current cluster electorate cannot simultaneously be tracked as a retiring participant. When the active electorate changes to include a participant,
  any pending retirement pipeline and quiescence authorization for that participant must be cancelled and removed. Furthermore, if an active peer responds with a role of `RETIRING` (because it has not yet received the electorate change that
  includes it), the leader must treat the rejection as needing earlier records rather than abandoning replication. The leader must rewind replication to include the electorate change entry that brings the follower out of retirement,
  preventing deadlock.
- **Retiree Replication Invariant Under Quorum Authority:** Active leaders and ghost leaders governing a non-empty active electorate must persistently replicate log records to excluded participants without abandonment, as cluster liveness
  is governed by the surviving voting configuration. Decommissioning unreachable retirees upon retry exhaustion is strictly confined to the terminal cluster shutdown state where the active electorate is empty and no successor leadership can
  arise. Decommissioned retirees are omitted from subsequent quiescence authorization broadcasts, as their commit watermarks were never advanced to the excluding configuration change.
- **Independent Exclusion Commitment Invariant for Retiring Participants:** When a ghost leader evaluates learner progress to determine eligibility for abdication, commitment of retiring learners must be verified against each participant's
  individual excluding configuration change index rather than the ghost leader's own exclusion index, ensuring participants retired in prior configuration changes complete retirement without requiring log records from subsequent tenures.
- **Quiescence Permission Deduplication Across Successive Transitions:** Participants that have acknowledged quiescence authorization from the active leader are omitted from subsequent quiescence authorization broadcasts within that
  leadership tenure, preventing redundant RPC retry loops targeting already-quiesced nodes. If an excluded participant is subsequently re-included in a succeeding active electorate, its acknowledged quiescence state is cleared so that any
  future exclusion generates a fresh authorization for the new configuration change.

---

### II. Universal Log Fusion & Obsolete Suffix Truncation Invariants

- **Universal Scope:** Log matching and fusion invariants apply to all participants processing replication RPCs (`AppendRecords`), regardless of their current role (`Follower`, `Joining`, `Isolated`, or `Retiring`).
- **Conflict Truncation:** During log fusion, if an existing local record at index $i$ differs in term from the corresponding record in the incoming batch, the local log is truncated from index $i$ onwards before appending the remaining
  batch entries.
- **Obsolete Trailing Suffix Truncation:** When all entries in an incoming batch match the local log without term conflicts, but the local log holds uncommitted entries extending beyond the applied batch:
    - The evaluation boundary advances to the first uncommitted entry, calculated as $\max (\text{appliedIndex} + 1, \text{commitIndex} + 1)$.
    - If the entry at this uncommitted boundary was written under a term strictly lower than the active leader's term, that entry and all subsequent records represent uncommitted remnants from a superseded term.
    - The participant must truncate its log starting at this uncommitted boundary.
- **Committed Data Invariant:** Suffix truncation is strictly bounded by the participant's verified commit baseline (`commitIndex + 1`), ensuring committed records are never mutated or truncated.
- **Current-Term Invariant:** If uncommitted trailing records have terms matching the active leader's term, they are preserved as valid extensions established under the current leadership tenure.

---

### III. Log Compaction Boundaries

- **Committed-Only Compaction Invariant:** Log compaction and log truncation must operate strictly within the bounds of committed log entries. A node must never truncate log entries beyond its verified commit index.
- **Compaction Truncation Bounds:** Log compaction truncation index must never fall below the in-memory log buffer offset. The retained log record count is bounded by the compaction threshold to guarantee strictly positive buffer bounds.
- **Recovered Commit Floor Invariant:** Commands applied to the state machine or embedded in a persistent snapshot are mathematically guaranteed to have been committed by quorum. Upon startup or crash recovery, a participant must advance
  its volatile commit index to reflect the recovered applied command index and snapshot boundary, ensuring candidate state information accurately advertises the verified committed baseline during leader elections.
- **State Machine Synchronization:** State machine application index and log compaction truncation point must never exceed the verified commit index.
- **Snapshot-Superseded Log Record Fusion Invariant:** When a follower receives an `AppendRecords` RPC containing log records whose indices fall strictly below or at the follower's locally committed snapshot boundary, the batch is evaluated
  as consistent without requiring previous log index matching against compacted history, because entries prior to a verified snapshot are mathematically guaranteed to have achieved quorum commitment.

---

### IV. Client Response Contract & Commit Watermark Invariant

- **Commit Watermark Invariant:** Whenever a leader successfully commits and applies a client command to its state machine, the positive acknowledgement (`Processed`) must expose both the resulting `RecordIndex` of the committed command in
  the distributed log and the application's `StateMachineResponse`.
- **Decoupled Session Consistency:** Exposing `RecordIndex` directly in the consensus response contract allows client runtimes, gateway routers, and caching layers to track monotonically increasing commit watermarks without deserializing or
  coupling to application-specific `StateMachineResponse` envelopes, enabling standardized read-your-writes and session-level linearizable consistency across cluster participants.

---

### V. Read Consistency Architecture & Omission of Quorum-Verified Reads (ReadIndex)

- **Omission of Quorum-Verified Reads (`ReadIndex`):** The framework deliberately omits lease-free linearizable read protocols (such as Raft's `ReadIndex`) as well as physical clock-based leader leases. In an idle Lazy Multi-Raft cluster
  with thousands or millions of quiescent shards, executing an on-demand quorum exchange for read operations destroys network quiescence, introduces distributed round-trip network latency, and turns read paths into consensus bottlenecks.
- **Session Consistency via Applied Watermark Barrier:** Read operations are served directly from co-located state machines or embedded databases by gating queries on the monotonic applied watermark
  (`highestAppliedCommandIndex >= targetIndex`). Clients, gateways, or caching layers propagate the `RecordIndex` returned by prior write operations as their `targetIndex`, ensuring monotonic reads and read-your-writes guarantees without
  distributed consensus overhead.
- **Applied Boundary vs. Commit Boundary:** Read barriers must synchronize strictly against the state machine's application index (`highestAppliedCommandIndex`), never the consensus `commitIndex`. While reaching `commitIndex` guarantees log
  durability across a quorum, the co-located database only reflects state mutations once the entry is sequentially applied via `applyClientCommand`. Reading when `commitIndex >= targetIndex` before state machine application completes yields
  stale or unapplied state.
- **In-Memory Sequencer Awaiters vs. External Database Triggers:** Synchronization against a pending `targetIndex` is performed strictly in memory within the single-threaded sequencer (`Doer`) using non-blocking reactive awaiters attached
  to the state machine applier. The co-located database functions as an embedded storage sink driven sequentially by the consensus participant; it requires no external database triggers, polling, or storage-level notification mechanisms to
  notify readers when a watermark is reached.

---

## 3. Ghost Leader Electorate Transition & Learner Convergence Dynamics

### I. Ghost Leader Electorate Transition Invariant

- **Ghost Leader Definition:** When a leader commits a sole electorate change that excludes itself from the target electorate, it enters a transitional ghost leader state. The ghost leader remains responsible for driving followers to commit
  the excluding electorate change, but cannot initiate or lead subsequent electorate transitions.
- **Ghost Leader Abdication Convergence Preconditions:** A ghost leader relinquishes office to the `Retiring` role if and only if two conditions converge: (1) universal exclusion commitment—all active included learners and retiring learners
  have committed their corresponding excluding configuration changes; and (2) complete quiescence permission acknowledgment—all outbound quiescence authorization permissions granted by this leader have been acknowledged by the recipients
  (or permission retry limits have been exhausted and unacknowledged entries cleared). Surviving members in a non-empty sole electorate form election quorums independently without consulting the excluded ghost leader, guaranteeing that
  prolonging ghost leadership until outbound quiescence permissions are acknowledged cannot deadlock succeeding elections.
- **Electorate Validation Scope:** A ghost leader evaluates its universal commitment condition strictly against the current active electorate and active retiring learners. Historical tracking capacities or learners excluded by prior
  electorates must not influence this eligibility check, preventing stale tracking data from stalling the leader's retirement.
- **Exclusion Rejection Barrier:** If a ghost leader receives a request for a new electorate change from which it is also excluded:
    - If all abdication convergence preconditions are met, the ghost leader transitions immediately to `Retiring` and delegates request handling to the new role.
    - If one or more active learners have not yet committed the excluding electorate change or quiescence permissions remain unacknowledged, the ghost leader cannot transition to `Retiring` and must reject the request with a status
      instructing the caller to wait for the ghost leader to be demoted (`WaitGhostLeaderIsDemoted`), rather than synchronously re-triggering replication loops.

---

### II. Asynchronous Learner Convergence vs. Quorum Progress Decoupling

- **Quorum vs. Universal Convergence Decoupling:** Replication waves complete successfully as soon as a majority quorum of active peers acknowledges append RPCs. Conversely, leader retirement transitions require universal (100%)
  acknowledgment across all active learners.
- **Asynchronous Retry Decoupling:** When a replication wave completes via majority quorum while lagging or unreachable learners remain pending, retries targeting unreachable learners must be scheduled asynchronously on dedicated timers.
  Ghost leaders must never execute re-entrant, synchronous replication loops during electorate change request handling.
- **Commit Watermark Broadcast Invariant:** When the leader's commit index advances upon receiving an append acknowledgment from any peer, replication must be triggered across all active and retiring peer pipelines. In a heartbeat-free lazy
  consensus architecture, idle peer pipelines (whose logs are caught up but whose knowledge of the commit index is stale) only learn of newly committed records when the leader drives replication. Failure to broadcast commit index
  advancements leaves idle followers unaware of commitments, preventing excluded ghost leaders from observing universal learner acknowledgment and retiring.

---

## 4. On-Demand Election Protocol, Single-Vote-Per-Term Invariant & Promotion Dynamics

### I. Multi-Phase Reactive Election Architecture

Unlike monolithic consensus systems with background heartbeat timeouts, leader elections are executed on-demand in coordinated phases:

1. **State Discovery Phase (`HowAreYou` / Pre-Vote):**
    - An uncoordinated participant (`Isolated`) encountering client commands or leadership loss broadcasts state discovery queries (`HowAreYou`) to all reachable peers in the active electorate.
    - This phase is term-neutral and acts as a reactive Pre-Vote: querying nodes do not advance their terms, ensuring active leaders are never disrupted by exploratory queries from partitioned or restarting nodes.
   - Responding peers expose a snapshot of their local consensus state (`StateInfo`), containing their current term, election rank, commit index, and log tail metadata.
2. **Deterministic Candidate Nomination (`decideMyNomination`):**
    - Each participant evaluates contenders using a deterministic, total-order comparison function over `(currentTerm, isLeading, lastRecordTerm, lastRecordIndex, isCandidate, membershipTier, participantId)`.
    - **Precedence Order & Safety Invariants:**
        - `currentTerm`: Higher terms strictly dominate lower terms.
        - `isLeading` over Log Completeness: An active leader (`ER_LEADING`) within the same term takes precedence over other participants regardless of log tail comparisons. Assuming term invariants hold, this avoids unnecessary leadership
          churn, abdications, and re-elections during exploratory discovery.
        - Log Completeness over `isCandidate`: Log completeness (`lastRecordTerm`, then `lastRecordIndex`) strictly dominates candidate status (`ER_CANDIDATE`). Lagging candidates cannot bypass more complete logs held by retiring
          (`ER_RETIREE`) or joining (`ER_JOINER`) participants. If a non-candidate peer holds newer committed entries, the comparison prioritizes that peer's state, preventing the candidate from voting for itself or achieving nomination
          until it absorbs those committed records.
        - `isCandidate` over Non-Candidate: Among participants with equal terms and identical log completeness, active candidates take precedence over passive participants (followers, retirees, joiners).
        - 3-Tier Electorate Membership Hierarchy (Surviving > Retiring > Joining): Among contenders with identical log completeness and candidate status during joint consensus transitions, candidates are ranked according to a deterministic
          3-tier membership hierarchy: Surviving ($C_{\text{old}} \cap C_{\text{new}}$, Tier 2) > Retiring ($C_{\text{old}} \setminus C_{\text{new}}$, Tier 1) > Joining / New-Only ($C_{\text{new}} \setminus C_{\text{old}}$, Tier 0). This
          ranking satisfies three fundamental invariants:
            - Election Liveness Invariant (Old-Set Precedence: Tiers 2 and 1 over Tier 0): In joint consensus transitions, nodes transition to joint consensus immediately upon appending a transitional joint configuration record
              (`JointElectorateChange`), prior to commitment. If a leader crashes while this joint record is uncommitted, lagging nodes operating under the prior stable configuration ($SoleElectorate (C_{\text{old}})$) discover and vote
              exclusively for peers within $C_{\text{old}}$. If nodes operating under joint consensus were to elect a new-only peer ($C_{\text{new}} \setminus C_{\text{old}}$), votes would deterministically split: $C_{\text{old}}$ nodes
              would vote for an old node, while joint nodes would vote for the new-only node. Because the joint configuration entry is uncommitted, it cannot be committed out-of-band via log matching; only a newly elected leader can
              replicate uncommitted records via append RPCs. Because joint consensus requires independent majorities in both $C_{\text{old}}$ and $C_{\text{new}}$, a split vote permanently deadlocks election quorums. Prioritizing candidates
              belonging to $C_{\text{old}}$ (Tiers 2 and 1) strictly over new-only candidates (Tier 0) guarantees election convergence and eliminates livelock.
            - Ghost Leader Elimination Invariant (Surviving Precedence: Tier 2 over Tier 1): When both a surviving node ($C_{\text{old}} \cap C_{\text{new}}$) and a retiring node ($C_{\text{old}} \setminus C_{\text{new}}$) hold the
              uncommitted joint entry with identical log completeness and candidate rank, treating them symmetrically causes tie-breaking to arbitrate by identifier. If the retiring node has a lower identifier, it wins the election, commits
              the joint configuration, and appends the subsequent stable configuration ($SoleElectorateChange (C_{\text{new}})$) from which it is excluded. Upon committing this sole configuration, the retiring leader transitions to a Ghost
              Leader and must step down, triggering a secondary leader election. Prioritizing surviving nodes ($C_{\text{old}} \cap C_{\text{new}}$, Tier 2) over retiring nodes ($C_{\text{old}} \setminus C_{\text{new}}$, Tier 1) eliminates
              this redundant handover, establishing stable leadership in a single election round.
            - Disjoint Cluster Migration Invariant ($C_{\text{old}} \cap C_{\text{new}} = \emptyset$): In complete cluster migrations where Tier 2 is empty, the hierarchy degrades gracefully to Tier 1
              ($C_{\text{old}} \setminus C_{\text{new}}$) over Tier 0 ($C_{\text{new}} \setminus C_{\text{old}}$). An old node is elected, commits the new configuration, and executes a planned handover to $C_{\text{new}}$, preserving
              liveness even with zero surviving nodes.
        - `participantId`: Deterministic tie-breaker ensuring that all participants with identical peer views select the exact same candidate.
3. **Candidate Elevation & Preemptive Term Bumping:**
    - When a participant deterministically selects itself and has observed responses from a majority of active participants, it initiates Phase 2.
    - Before issuing vote solicitations, the candidate advances its term to $T_{\text{target}} = \max (T_{\text{observed}}) + 1$, records its vote for itself, and persists $(T_{\text{target}}, \text{votedFor} = \text{self})$ in stable
      storage via the primary state causal fence.
4. **Voting Phase (`RequestBallot`) & Voter Epoch Fencing:**
    - The candidate solicits explicit ballots via `RequestBallot` carrying $T_{\text{target}}$ and its updated `StateInfo`.
    - Responding peers observing $T_{\text{target}} > T_{\text{local}}$ atomically advance their local term to $T_{\text{target}}$ and clear prior votes in persistent storage before evaluating the solicitation.
    - Peers evaluate the solicitation against log completeness and single-vote-per-term constraints. If the candidate's log is up-to-date and the voter has not voted for another candidate in $T_{\text{target}}$, the voter persists
      `votedFor = candidateId` and returns an affirmative `Ballot(term = targetTerm, granted = true, voterRank)` exposing its own role rank. If the vote is denied or the voter is ineligible, it returns a non-granted
      `Ballot(term, granted = false, voterRank)`.
    - Local Voter Evaluation Boundary: Voters evaluate vote solicitations strictly against local log completeness and the single-vote-per-term invariant without dispatching discovery queries (`HowAreYou`) to third-party peers. This
      prevents $O (N^2)$ query cascades and ensures that an unreachable or partitioned minority cannot deadlock majority quorum formation.
    - Epoch Fencing Effect: Advancing and persisting $T_{\text{target}}$ permanently fences the voter against all prior terms ($\le T_{\text{target}} - 1$). Any delayed append requests from an old deposed leader at prior terms are
      unconditionally rejected with `StaleTerm`.
5. **Quorum & Role Inauguration:**
    - Sole Electorate Quorum: Evaluated by aggregating returned ballots. The accumulator seeds its matching count with the candidate's own granted vote, counting affirmative `Ballot(granted = true)` responses across the electorate without
      requiring candidate ID comparison. A candidate must receive affirmative ballots for $T_{\text{target}}$ from a strict majority (`> N / 2`) of active electorate participants.
    - Joint Electorate Quorum (Joint Consensus): A candidate must receive a strict majority of affirmative ballots in **both** the old electorate set ($E_{\text{old}}$) and the new electorate set ($E_{\text{new}}$).
    - If the candidate obtains the required quorum, it transitions directly into `Leader(term = targetTerm)` using the freshly anchored primary state and active electorate. Because the term and self-vote were committed to persistent storage
      prior to vote collection, no intermediate promoting role or post-election persistence barrier is required. If quorum is not attained, the participant transitions to `Isolated` to await subsequent triggers or client commands.

---

### II. Canonical Raft Term Invariants & Single-Vote-Per-Term Dynamics

- **Monotonic Term Progression:** Every participant maintains a durable monotonic term counter (`currentTerm`). Monotonic term bumping guarantees that distinct election epochs are strictly partitioned across time.
- **Preemptive Candidate Advancement:** A candidate bumping its term to $T_{\text{target}} = \max (T_{\text{observed}}) + 1$ persists $(T_{\text{target}}, \text{votedFor} = \text{self})$ atomically before soliciting votes.
- **One Vote per Term Invariant:** A participant may cast at most one vote per term. When a vote is granted, the candidate's identity is recorded in persistent storage (`Workspace.votedFor`) alongside the term. Advancing to a higher term
  clears the recorded vote.
- **Split-Brain Prevention:** Because voters persist their term and granted vote before responding to `RequestBallot`, intersecting majorities across terms are prevented from acknowledging divergent logs, strictly preserving Raft's Leader
  Completeness and State Machine Safety without requiring secondary ballot counters.

---

### III. Direct Leadership Inauguration & In-Flight Cleanup Invariants

- **Pre-Election Persistence & Direct Inauguration:** Unlike legacy designs that defer term bumping until after quorum acquisition, the term is advanced and persisted alongside the self-vote *prior* to Phase 2 vote collection. Consequently,
  securing an election quorum allows the node to immediately and synchronously inaugurate `Leader(term = targetTerm)` without entering an intermediate transitional state or gating inbound RPC traffic behind promise covenants.
- **Strict Role-Exit Cleanliness:** All active replication waves and retry tokens are canceled upon role transitions. Role references are updated before invoking exit handlers to ensure synchronous cancellation continuations immediately
  observe that the node is no longer in the previous role.
- **Leadership Inauguration:** Upon entering leadership, the leader initializes learner replication progress baselines from the first uncommitted record index and immediately drives replication to active peers.

---

### IV. Out-of-Band Commit Index Absorption Invariant (Log Matching Safety)

- **Deadlock Vulnerability with Ineligible Candidates:** Because log completeness strictly dominates candidate rank (`completeness > isCandidate`), an active candidate in Phase 1 discovery will lose the election to a retiring (`ER_RETIREE`)
  or joining (`ER_JOINER`) participant whenever that peer possesses a higher commit index or a more complete log tail. Retiring and joining participants are structurally ineligible for leadership (`rank != ER_LEADING` and cannot transition
  to `Leader`). Consequently, they will never broadcast `AppendRecords` to advance followers' commit indices. If candidates were restricted to standard Raft leader-driven commit index advancement, candidates would repeatedly yield blank
  votes or select ineligible peers, causing the cluster to deadlock indefinitely.
- **Out-of-Band Advancement via Raft's Log Matching Property:** To break election deadlocks without requiring heartbeats or active leaders, active participants evaluate out-of-band commit index absorption
  (`absorbHigherCommitIndexFromPeers`). By Raft's Log Matching Property: *If two entries in different logs have the same index and term, the logs are identical in all preceding entries.* An active participant safely advances its local
  `commitIndex` out-of-band to a peer's `peerCommitIndex` without leader intervention if and only
  if: $$\text{firstEmptyRecordIndex} > \text{peerCommitIndex} \quad \text{and} \quad \text{getRecordTermAt} (\text{peerCommitIndex}) == \text{peerStateInfo.termAtCommitIndex}$$ Matching the term at `peerCommitIndex` provides mathematical
  proof that all entries up to `peerCommitIndex` in the local log are identical to the committed entries on the peer.
- **Post-Absorption Dynamic Electorate Transition:** Once `commitIndex` is advanced, the node triggers local application of newly committed commands and electorate entries. When the absorbed commit index covers the `SoleElectorateChange`
  that finalized a retiree's exclusion, the candidate updates its active electorate, removes the retired peer from consideration, and becomes eligible to secure majority quorum and inaugurate leadership in the subsequent election round.

---

### V. Early-Terminating Quorum Accumulation & Strict Phase Isolation

- **Reactive Incremental Evaluation:** Rather than awaiting all in-flight peer responses across the entire electorate, election phases evaluate decisive quorum conditions reactively upon each individual reply. In a sole electorate,
  solicitations achieve decisive resolution early as soon as a strict majority ($> N / 2$) of affirmative responses are collected, or as soon as accumulated rejections and failures prove that obtaining a majority is mathematically
  impossible ($N - \text{failures} \le N / 2$). In joint electorates ($E_{\text{old}} \to E_{\text{new}}$), independent majorities in both the old and new participant sets are strictly required; accumulation short-circuits to success when
  both subsets reach majority, or short-circuits to failure immediately if failures in either subset preclude reaching a majority.
- **Immediate Higher Term and Active Leader Short-Circuiting:** In both Phase 1 discovery and Phase 2 voting, receiving an RPC response from a peer carrying a term higher than the local term ($T_{\text{peer}} > T_{\text{local}}$)
  immediately short-circuits the accumulator with a stale outcome, aborting election progression without awaiting pending peer replies. In Phase 1 discovery, observing an active leader ($T_{\text{peer}} == T_{\text{local}}$ with rank
  `ER_LEADING`) immediately terminates state discovery and transitions the participant to `Follower` under that leader.
- **Strict Phase Isolation & Disposal of Trailing Queries:** The instant an accumulator reaches a decisive outcome, it unconditionally invokes an unsubscription callback to cancel and discard all remaining in-flight peer inquiries. Trailing
  discovery queries are strictly prohibited from resolving or preempting state across role transitions into Phase 2 voting. Because Phase 2 voting is mandatory and enforces epoch fencing alongside log completeness verification, trailing
  Phase 1 discovery replies are mathematically redundant for consensus safety. Eliminating cross-phase preemption prevents causal timeline desynchronization and ensures clean, decoupled actor role transitions. Aborted inquiries are resolved
  with cancellation failures to release memory without triggering error recovery routines.
- **Command-Query Separation in State Discovery:** State discovery evaluation operates strictly as a side-effect-free query that collects peer metadata without mutating local log state, commit indices, or terms. Upon discovery completion, a
  dedicated reconciliation command phase ingests peer replies, updates observed terms, absorbs eligible commit indices via Log Matching, and adjusts peer caches. Once state is reconciled, a synchronous query evaluates candidate completeness
  to compute the candidate nomination (`CandidateNomination`). When an active leader of the current term is observed during early-terminating discovery, the evaluation returns a nomination pointing to that leader rather than executing a
  role mutation directly or returning an empty nomination. The subsequent command phase executes the role transition to follower unconditionally upon evaluating an active leader nomination, bypassing numerical discovery quorum requirements
  because the existence of an active leader in the current term constitutes decisive mathematical proof of quorum validity.

---

## 5. Electorate Change Protocol & Response Semantics

- **Client Interaction & Terminality Classification:** Electorate change requests adhere to a strict interaction contract partitioned by terminality:
    - **Terminal Responses:** `SuccessfullyChanged` and `AlreadyChanged` represent definitive consensus outcomes (a newly committed or already matching configuration) that definitively satisfy the requester and terminate client retry loops.
    - **Non-Terminal Responses:** Represent transient rejections, busy states, redirections, or interrupted request tracking (`WaitGhostLeaderIsDemoted`, `AnotherChangeInProgress`, `AskTheLeader`, `CatchingUp`, `Excluded`, `Secluded`,
      `Stopped`, and lost tracking outcomes). Non-terminal responses instruct the client coordinator on how and where to retry without implying request failure.
- **Rejection of Concurrent Configuration Changes & Coordinator Synchronization:** The leader enforces that at most one configuration transition is active at any time. When an electorate change request is submitted while another transition
  is actively in progress (undergoing joint or sole consensus replication and commitment), the leader does not queue or chain the proposal behind the active transition. Instead, it rejects the request non-terminally with
  `AnotherChangeInProgress`, communicating the identifier of the active request, its target configuration membership, and active peer routing hints. Rejecting concurrent proposals prevents unbounded proposal queueing on the leader, which
  would otherwise execute stale reconfigurations against nodes that have already been excluded, decommissioned, and permanently shut down by intervening configuration changes. When an external coordinator receives `AnotherChangeInProgress`
  whose reported in-progress target configuration matches the coordinator's own desired configuration, the coordinator must continue retrying and polling that transition until a terminal outcome (`SuccessfullyChanged` or `AlreadyChanged`)
  is observed, thereby keeping coordinator in-flight tracking synchronized with the active cluster transition and preventing premature termination of tracking or candidate pool corruption.
- **Leader Redirection Semantics:** When a client command or electorate change request is submitted to a follower, the response provides an explicit redirection (`RedirectTo` or `AskTheLeader`) carrying an ordered `ListSet` of candidate
  targets with the currently followed leader (`followeeId`) positioned as the first element, followed by fallback electorate candidates. This allows callers to retarget the active consensus leader directly on the first retry attempt and
  seamlessly fall back to remaining electorate members if the leader has become unreachable.
- **Strongly-Typed Participant Identifiers and Probability-Ordered Candidate Routing Hints:** Electorate change response contracts (`ElectorateChangeResponse[+P]`) and non-terminal client command responses (`RedirectTo` and `Unable`) carry
  candidate routing suggestions as an ordered `ListSet` (`suggestedParticipants: ListSet[? <: P]` for electorate change responses and `otherParticipants: ListSet[ParticipantId]` for client command responses) ordered in descending
  probability that each participant is the next appropriate target to contact, even when the response variant implies that the responder itself is the primary target. Rather than over-complicating set construction with full multi-key
  sorting, ordering logic prioritizes placing the highest-probability candidate at the head of the `ListSet`: followers place their followed leader (`followeeId`) first followed by active electorate peers; isolated participants place their
  most recently nominated election candidate first (when available) followed by active peers and self; retiring participants place the quiescence-authorizing leader first (when included in the excluding configuration) followed by the
  excluding configuration members; leaders rejecting concurrent configuration proposals (`AnotherChangeInProgress`) or reporting lost tracking place themselves first when included in the active target configuration, or place a surviving
  peer ($C_{\text{old}} \cap C_{\text{new}}$) first when self-excluding; and joint consensus configurations place a surviving peer ($C_{\text{old}} \cap C_{\text{new}}$) ahead of retiring-only ($C_{\text{old}} \setminus C_{\text{new}}$) and
  joining-only ($C_{\text{new}} \setminus C_{\text{old}}$) peers. While client command rejections (`Unable`) exclude the responding node from `otherParticipants` because the responder cannot execute the command in its current role,
  electorate change responses (`suggestedParticipants`) include the responder whenever it remains a valid candidate for subsequent configuration coordination. Roles lacking consensus-derived membership knowledge (such as `Stopped` or
  `Quiesced`) return an empty `ListSet`, and terminal responses (`SuccessfullyChanged` and `AlreadyChanged`) remain static allocation-free singletons extending `ElectorateChangeResponse[Nothing]` with an empty `ListSet`.
- **Electorate Transition Quiescence Decoupling:** Successful replication and commitment of an electorate change across a majority quorum immediately yields a terminal response to the client. Terminality is strictly decoupled from the
  subsequent, asynchronous retirement and quiescence of excluded participants; the coordinator is unblocked the moment the configuration is durably committed in the Raft log.
- **Ghost Leader Quiescence & Commit Watermark Fulfillment Order:** When a leader is excluded by an active configuration change (operating as a ghost leader to conclude membership transitions) and all active and retiring participants have
  committed that excluding configuration change, the leader must not relinquish office or initiate quiescence before advancing the commit watermark to include all eligible entries in its term and resolving completion handlers. Advancing the
  commit watermark prior to role exit guarantees that in-flight client commands and the configuration change itself are durably committed and reported to callers while the leader remains authoritative, ensuring second-phase membership
  replication concludes with a terminal confirmation rather than aborted request tracking, and that participant retirement and quiescence authorization proceed strictly after all log entries committable in that term are finalized.
- **Commit Watermark Lower-Bound Monotonicity:** Commit watermark evaluation across both sole and joint consensus configurations is monotonically non-decreasing and bounded below by the existing commit index. When no uncommitted entries in
  the current term have achieved majority replication, the evaluation unconditionally preserves the existing watermark, ensuring that commit indices never regress.
- **Ingress Gating Precondition for Graceful Decommissioning:** When executing graceful cluster decommissioning by submitting an electorate change to the empty set (`Set.empty`), the host integration layer, ingress gateway, or
  `ClusterBridge` is architecturally required to gate, reject, or drain inbound client command traffic prior to or concurrently with issuing the empty electorate change request. Once the empty configuration is committed to the Raft log, all
  cluster participants transition to `RETIRING` and `QUIESCED` roles, in which inbound client commands are permanently rejected with `Unable` and cannot be committed due to the absence of voting members. Failing to gate client traffic at
  the ingress tier results in persistent client-side retry exhaustion and spurious communication errors against a decommissioned cluster.
- **Active Electorate Replication Resilience over Quiescence:** When an active voting member of the current electorate (including newly introduced participants in transitional joint consensus) returns a quiesced status in response to log
  replication—such as when processing a delayed quiescence authorization from an earlier configuration change—the leader must strictly distinguish between retiring followers and active cluster peers. While retiring followers terminate
  replication and decommission progress tracking upon reaching quiescence, active cluster peers must not be decommissioned. The leader treats quiescence from an active peer as transient unreachability: optimistic send indices are rolled
  back to unacknowledged baselines to preserve uncommitted entries, and exponential backoff retry timers are scheduled to resume replication as soon as the participant re-initializes into a joining or follower role, preventing permanent
  joint quorum stall.
- **Client Eviction on Excluded Response:** An `Excluded` response explicitly informs the client coordinator that the targeted participant is no longer a voting member of the cluster configuration. Client-side request routing and target
  rotation must immediately evict the excluded node from active target pools rather than treating the response as transient seclusion subject to retry backoffs.
- **Active-Pool Target Prioritization during Administrative Coordination:** Client coordination of cluster administration operations (such as graceful shutdown or sequential membership changes) must prioritize known active voting
  participants over historical or retiring nodes during candidate selection and round-robin target rotation. Routing administration requests against retiring nodes wastes retry budgets and triggers unnecessary backoff delays while active
  nodes await election and replication.
- **Joint Consensus Retirement Recognition for Excluded Followers:** During two-phase joint consensus ($C_{\text{old}, \text{new}}$), participants belonging solely to the prior configuration ($C_{\text{old}} \setminus C_{\text{new}}$) are
  excluded from the target stable configuration ($C_{\text{new}}$). When an excluded follower commits the excluding sole configuration change and enters role `RETIRING`, its log is permanently bounded by its excluding configuration index.
  The leader must recognize `RETIRING` responses from excluded peers as terminal retirement synchronization, decommissioning further replication to them and evaluating commit index advancement. Because `JointElectorate` quorums recognize
  retirement responses from old-only peers as satisfying old-configuration replication requirements, this enables the leader to satisfy old-configuration quorum requirements, commit current-term anchor entries, and finalize the transition
  to the stable configuration.
- **Obsolete Leader Immediate Abdication and Electorate Isolation Invariant:** When a leader receives a valid replication payload (such as log record append or snapshot installation) presenting a strictly higher term from a successor
  leader, the node is definitively obsolete. The participant must immediately abdicate and decommission its leadership state prior to advancing commit watermarks or deriving the active configuration state. Because configuration derivation
  updates active electorate state and triggers role-specific configuration hooks, retaining an obsolete leader instance during electorate derivation causes legacy replication pipelines and retirement drivers to execute against a truncated
  or snapshot-replaced primary state log, attempting to inspect purged log indices. Immediate abdication guarantees that leadership replication mechanisms are synchronously dismantled, preserves truthful lifecycle notification ordering, and
  enables clean subsequent resolution to a retiring role (if excluded by the new configuration), a follower role (if following an active peer), or an isolated role.
- **Transitional Joint Consensus Quorum-Buffering for Re-included Retirees:** During two-phase joint consensus ($C_{\text{old}, \text{new}}$), a newcomer belonging to the target configuration ($C_{\text{new}} \setminus C_{\text{old}}$) may
  be a participant excluded by a prior committed configuration change that is currently in the `RETIRING` role awaiting quiescence authorization or catch-up. Because such participants have not yet received the transitional configuration
  change, they cannot participate as active voters and cast blank ballots. Just as joining participants (`ER_JOINER`) buffer the quorum of $C_{\text{new}}$ while catching up, retiring participants belonging
  to $C_{\text{new}} \setminus C_{\text{old}}$ (`ER_RETIREE`) contribute toward $C_{\text{new}}$'s quorum threshold. Recognizing both `ER_JOINER` and re-included `ER_RETIREE` as non-voting quorum buffers preserves quorum evaluation liveness
  for $C_{\text{new}}$, allowing surviving members of $C_{\text{old}}$ to elect a leader that replicates the transitional configuration and returns the retiree to active participation.
- **Stateful Role Immediate Retirement on Exclusion Invariant:** When a participant in a stateful role absorbs a higher commit index that advances the commit watermark past an excluding configuration change, the participant is excluded from
  the active electorate. The node must immediately transition to the `RETIRING` role upon processing the interaction, ensuring subsequent ballots and state exchanges truthfully report the retiree rank during election and discovery
  interactions.
- **Retiring Participant Re-inclusion Invariant:** When a participant in the `RETIRING` role receives an authoritative replication payload (delivered via log record batches or state machine snapshots), it evaluates re-inclusion against any
  configuration change whose log index exceeds its excluding configuration index. Because committed configurations are preserved as either transitional joint configuration records (`JointElectorateChange`) or stable configuration records
  (`SoleElectorateChange`), any subsequent configuration change that includes the participant in its target membership ($C_{\text{new}}$) constitutes authoritative proof of re-inclusion. Upon observing such a configuration change in
  uncompacted log entries or snapshot metadata, the participant immediately transitions from retirement to initialization (`STARTING`) to reload persistent state, synchronize with the active leader, and resume active cluster participation.

---

## 6. Decoupled Mutation Contract & Causal State Synchronization

To guarantee strictly sequential state transitions without the stalling overhead of Thread/Actor-blocking or the race-condition vulnerabilities of Mutexes (lock-and-release), the consensus participant wraps its `PrimaryState` inside a
`CausalFence`. This acts as a non-blocking promise queue dedicated solely to state synchronization (see [CausalFence and the Decoupled Mutation Contract](adr-causal-fence-state-synchronization.md)).

### The Game-Changing Invariant & Temporal Window of Causal Safety

By attaching continuations synchronously to the `Capture` returned by `advance` or `causalAnchor`, the algorithm guarantees execution ordering. Continuations inherently execute in the exact causal timeline slot following the anchored
mutation, ensuring they receive the perfectly synchronized, fresh state.

Crucially, this guarantee is strictly bounded by the **Temporal Window of Causal Safety**:
- The causal guarantee holds only during the synchronous execution of a consumer synchronously subscribed to the returned `Capture`.
- Once execution yields, crosses an asynchronous completion (such as `waitRecordBecomesCommitted`), or defers via `Capture_defer`, the captured `PrimaryState` reference is obsolete and outside the causal window.
- Any subsequent derivation of electorate, role synchronization, or invariant verification that inspects `PrimaryState` after an asynchronous boundary or deferred dispatch MUST acquire a fresh anchor via `primaryStateFence.causalAnchor()`
  and perform derivations strictly within that anchor's synchronous continuation.

### The Decoupled Mutation Contract (An Asymmetric Performance Decision)

While `CausalFence` enforces the sequential timeline, the consensus algorithm specifically enforces an asymmetric **Decoupled Mutation Contract** across all internal reactive notification boundaries (such as commit watermark awaiter
fulfillment
and state event observers).

This contract strictly mandates that observers and continuation callbacks MUST NOT synchronously mutate the protected `PrimaryState` (`CausalFence`) within their notification context. Any consequential state mutation or side-effect
resulting from a notification (such as appending records, transitioning electorate phases, or initiating log compaction) must be decoupled from the notification stack—either by self-deferring via `sequencer.Capture_defer` or dispatching via
`sequencer.run`.

#### 1. Rationale: The Hot-Path Performance Asymmetry

The Decoupled Mutation Contract is an intentional architectural tradeoff designed to optimize the primary throughput path of the consensus engine:

- **Zero-Cost Happy Path for Client Commands:** In a production consensus cluster, over 99.999% of commit index advancements represent client command replication. On this hot path, when a command commits, the leader applies the command to
  the state machine, notifies client awaiters, and returns the response. Applying committed records to the state machine does not mutate the `PrimaryState` or touch the `CausalFence`. Because the contract guarantees that awaiters do not
  mutate the fence, commit index advancement can notify awaiters synchronously on the current thread stack. This eliminates task-queue allocations, trampoline scheduling, and dispatch latency, allowing the happy path to execute with
  absolute minimum overhead.
- **Asymmetric Caller Burden:** In exchange for zero-overhead execution on the dominant hot path, callers that execute state-mutating logic bear the architectural burden of explicit self-deferral. Seldom-traveled paths—such as the fallback
  branches of command replication (appending no-op records), electorate transition completions (advancing from joint to sole electorates), and sequential electorate transition chaining—must explicitly wrap their continuations in
  `Capture_defer`.
- **Role Exit Decoupling & Re-Entrancy Prevention:** During role transitions (such as leader abdication), in-flight watermark awaiters must be resolved in a decoupled manner (e.g., via `sequencer.run`). Because observers react to abdication
  by delegating vacated in-flight commands to the incoming role—which synchronously triggers role evaluations and state fence mutations—resolving awaiters synchronously inside `handleExit` creates re-entrant state mutations on the caller's
  call stack. Decoupling awaiter seizure ensures that role transitions and enclosing RPC turns complete atomically before vacated operations are processed.

#### 2. Rejection of Alternative 1: Synchronous Inline Re-Anchoring

A naive alternative to the contract would be to permit observers to mutate the state synchronously, under the assumption that the notifier could simply re-anchor after calling each observer. This alternative is structurally impossible:

- **The Asynchronous Promise Reality:** `CausalFence.causalAnchor()` is fundamentally an asynchronous primitive that returns a `Capture[PrimaryState]`, not a synchronous value. The moment an advance is enqueued onto the fence (such as an
  observer appending an entry to disk), the new state does not exist synchronously; it is an in-flight operation awaiting persistence or pipeline sequencing.
- **Inability to Resume Synchronously:** A synchronous loop (such as iterating over fulfilled watermark awaiters during commit advancement) cannot block or inline-await an asynchronous promise. To pass a freshly anchored state to subsequent
  awaiters,
  the entire remainder of the loop would have to be severed and converted into an asynchronous callback chain (`awaiter.seizeWith(causalAnchor())`), destroying synchronous stack execution.

#### 3. Rejection of Alternative 2: Dynamic Mutation Detection

Another tempting design is dynamic fallback detection: having commit index advancement notify synchronously by default, but dynamically check if the fence was modified
(`!primaryStateFence.committedState.is(primaryState0) || !primaryStateFence.isEmpty`), re-anchoring remaining awaiters only when a mutation occurs. This alternative was formally rejected due to two fatal concurrency hazards:

- **Intra-Batch Turn Splitting (Execution Reordering):** If a commit index advancement satisfies multiple awaiters simultaneously (e.g., an electorate change and subsequent client commands), and an early awaiter mutates the fence,
  dynamically re-anchoring subsequent awaiters via `seizeWith(causalAnchor())` defers them to future sequencer turns. Consequently, awaiters that achieved consensus within the exact same batch are arbitrarily split across different
  execution frames, violating linear dispatch expectations.
- **Stack Re-Entrancy and State Invalidation:** Allowing an observer to mutate the fence synchronously executes arbitrary state mutation logic while commit index advancement is actively running on the call stack. The observer's callback
  could alter roles (triggering abdication or follower transitions), schedule pipeline drives, or mutate the pending awaiter collection while the outer loop is still traversing it.

#### 4. The Resulting Purity Invariant

By enforcing the Decoupled Mutation Contract, commit index advancement maintains the strict assertion: `assert(primaryStateFence.committedState.is(primaryState0))`. This mathematical invariant proves that commit index calculation and
notification is strictly pure with respect to the causal state. It guarantees that a valid `PrimaryState` reference remains pristine and temporally safe from the beginning of the notification loop to the end, eliminating re-entrancy bugs,
stale-state references, and batch turn-splitting.

---

## 7. Diagnostic Inspection & Causally Consistent Observation

- **Quiescent Diagnostic State Boundary:** Diagnostic observation of internal participant and role state (such as peer learner replication progress and electorate tracking) must not bypass single-threaded sequencer confinement or observe
  partially applied causal mutations.
- **Inter-Step Temporal Consistency:** In discrete-event simulation, test harnesses, and external diagnostics, inspecting the active role's diagnostic snapshot is causally consistent at step boundaries (whenever a discrete sequencer step
  completes or when the runnable queue is empty). At these boundaries, all synchronous causal chain derivations (including active electorate changes and peer replication metrics) are guaranteed to be in identical lockstep.
- **Three-Tier Operational vs. Verification Oracle Boundary:** Discrete-event simulation and verification environments enforce a strict three-tier architectural boundary separating operational execution from privileged correctness
  inspection: (1) the physical environment and network control tier governs message delivery, packet loss, virtual time advancement, asynchronous persistence completion, and node execution stepping; (2) the external client and control-plane
  gateway tier mediates command submissions and configuration transitions strictly through client-visible protocol responses without inspecting internal node state; and (3) the omniscient invariant oracle tier is quarantined into a
  dedicated verification component reserved exclusively for cross-node safety assertions (including Election Safety, Leader Completeness, Log Matching, Leader Append-Only, and State Machine Safety), ensuring that external coordination and
  failover logic never rely on privileged internal state.
- **Deterministic Journal Replay & Reset Completeness Invariant:** Discrete-event operation journals that support timeline rollback (`undo`), forward re-execution (`redo`), and checkpoint tag restoration rely on full state reconstruction
  from an initial environment reset followed by deterministic operation replay. For replay to be strictly isomorphic to the original execution, environment reset must purge all subsystem state across all three tiers—including physical
  network channels, virtual clock timers, suspended persistence queues, client session counters, in-flight reconfiguration tracking tables, and oracle history buffers—and execute baseline initialization with journal recording suppressed so
  that replayed operations never accumulate duplicate journal entries or observe residual state from rolled-back branches.

---

## 8. Host Integration SPI Contracts & Boundaries

The consensus state machine in `ConsensusParticipantSdm` delegates all external capabilities (network communication, disk persistence, and domain state transitions) to three distinct host Service Provider Interfaces (SPIs).

### I. Cluster Communication Bridge (`ClusterBridge`)

- **Synchronous Cluster-Tier Probable-Participant Memorization Contract:** The host bridge method `ClusterBridge.getOtherProbableParticipants` returns a synchronous `ListSet[ParticipantId]` rather than an asynchronous `Capture`. This
  signature mandates that the host cluster tier proactively maintain and memorize within the participant's sequencer the optimistic set of other cluster nodes currently intended for consensus, ordered from most to least probable certainty
  of availability. By maintaining this probability-ordered view as nodes join, become reachable, crash, or leave, the cluster tier enables the consensus engine to synchronously extract high-priority fallback routing candidates for client
  responses and discover candidate peers during vanishing-electorate teardown without issuing asynchronous queries.
- **Asynchronous Completion Guarantee:** The consensus engine delegates all inter-participant communication to the host-provided transport bridge (`ClusterBridge`). All outbound RPC methods (`howAreYou`, `requestBallot`, `appendRecords`)
  return an asynchronous completion handle (`Capture[R]`) that contractually must resolve to either `Success` or `Failure`. The consensus state machine does not maintain internal per-request timeout watchdogs, relying strictly on the host
  layer to bound request lifecycles.
- **Aggregation vs. Pipelined Quorum Resilience:** While pipelined log replication makes forward progress upon reaching quorum without waiting for lagging peers, discovery and election phases aggregate responses across candidate peers
  (e.g., collecting state reports across the full electorate). If an outbound query is dropped without notifying the caller's completion observer of a transport failure, the aggregation pipeline awaits completion indefinitely, stalling the
  single-threaded participant actor.
- **Simulation Harness Equivalence:** In discrete-event simulation and test environments, dropping an in-flight network packet must resolve the sender's pending completion observer with a transport failure to emulate transport timeouts or
  connection termination. Silent removal of packets without failure resolution creates an artificial permanent deadlock that violates the reactive completion contract.
- **Uniform Transport Failure Handling:** The consensus protocol is fail-silent and retry-driven with respect to transport errors. All network-level failures are handled uniformly regardless of the underlying exception class; specific
  exception discrimination provides no algorithmic differentiation within the consensus engine.

### II. Storage SPI Contract (`Workspace` & `Storage`) & Persistence Boundary

- **Unopinionated Sequential Storage SPI:** The consensus participant accesses persistent state through the `Workspace` interface, managed by a host-provided `Storage` service (`load: Capture[WS]`, `save(workspace: WS): Capture[Unit]`).
  `Workspace` provides primitive sequential operations: appending raw records, truncating conflicting suffixes (`truncateSuffix`), truncating compacted prefixes (`truncatePrefix`), resetting logs to snapshot baselines (`resetLog`), and
  durably recording current terms and cast votes (`setTermAndVote`).
- **Encapsulation of Raft Log Matching Invariants:** The storage SPI is intentionally unopinionated and completely decoupled from Raft consensus rules. It performs no term conflict detection, retroactive electorate scanning, or log matching
  verification. All Raft invariants—such as term-based conflict detection, uncommitted trailing suffix truncation, and configuration change extraction—are executed exclusively within the consensus state machine (`PrimaryState`) across the
  `CausalFence` before mutating the underlying workspace.
- **Atomic Causal Durability & Failure Semantics:** All mutations to `Workspace` are sequenced inside causal updates (`CausalFence.advance`). A failure of the underlying `Storage.save` represents an unrecoverable persistence fault; the
  consensus participant treats storage write failures as fatal to the active role, shutting down or restarting the participant as a crash-recovery event to prevent diverging volatile state from persistent history.

### III. Application State Machine SPI (`StateMachine`) & Deduplication Boundaries

- **Sequential Command Application Contract:** The consensus engine interfaces with the business domain exclusively through the `StateMachine` trait (`applyClientCommand`, `recoverIndexOfLastAppliedCommand`, `takeSnapshot`,
  `installSnapshot`). Client commands committed across a majority quorum are applied sequentially in strictly increasing log index order. The state machine exposes its applied watermark through `recoverIndexOfLastAppliedCommand` during
  recovery to establish the commit floor.
- **Non-Blocking Snapshot Generation:** When the uncompacted log buffer exceeds the compaction threshold, `StateMachine.takeSnapshot()` produces a serialized snapshot of the domain state. The state machine implementation must support taking
  snapshots without blocking concurrent client command execution, and must guarantee that the captured snapshot state reflects state transitions strictly up to the requested index.
- **Delegation of Monotonic Client Sequence Tracking and Deduplication:** The consensus engine treats `ClientCommand` as an opaque binary payload, deliberately omitting client identity tracking, monotonic sequence validation, and request
  deduplication from the distributed log. Because log compaction truncates committed log entries past snapshot boundaries, the consensus log cannot durably retain per-client sequence histories to filter replayed or retried commands across
  node crashes or leadership transitions. Delegating deduplication to the application state machine ensures that client sequence watermarks and transaction deduplication tables are preserved within domain snapshots, transferred atomically
  during snapshot installation, and survive log compaction without coupling the consensus storage SPI to application-specific session schemas.

---

## 9. Node Lifecycle & Control Plane Synchronization Invariants

- **Terminal Quiescence Irreversibility Invariant:** Quiescence is an irreversible terminal lifecycle state indicating that a retiring participant has satisfied its retirement obligations, committed its excluding configuration change, and
  acknowledged explicit quiescence authorization from the cluster leader. Because the active leader clears quiescence authorization tracking once acknowledged, it will never reissue quiescence permission for that configuration change.
  Resurrecting a quiesced participant under the prior configuration violates quiescence finality: upon receiving leader replication, the resurrected node adopts the excluding configuration and enters `Retiring`, but cannot re-obtain
  quiescence authorization. Host environments and supervisors must enforce finality upon receiving a quiesced notification by permanently dismantling the participant instance rather than maintaining node-level administrative resurrection
  targets.
- **Presumed Active Electorate Tracking under Epistemic Uncertainty:** Because an external control plane or coordinator interacts with the cluster strictly through client-facing endpoints without omniscient access to internal node logs, its
  view of cluster membership represents a presumed active electorate rather than proven internal state. The coordinator updates this presumed electorate strictly upon observing terminal configuration responses (`SuccessfullyChanged` or
  `AlreadyChanged`) or authoritative exclusion rejections (`Excluded`), using it alongside the target configurations of in-flight proposals to prioritize routing candidates and evaluate fault-tolerance quorum budgets.
- **Reconciliation of Indeterminate Configuration Responses:** When a leadership transition or network partition yields an indeterminate outcome for a proposed configuration change (e.g., loss of request tracking after an initial phase is
  logged or committed), the control plane must not prematurely abandon the proposal or revert its presumed electorate tracking. It must reconcile the proposal state with the newly elected leader until obtaining a definitive terminal
  response, preventing divergence between the consensus log and external control-plane tracking.
- **Leader Redirection Advisory Invariant (`AskTheLeader` Non-Membership):** Receiving an `AskTheLeader(suggested)` redirection communicates purely advisory peer-routing state with the responder's known leader at the head of `suggested`; it
  conveys no authoritative statement regarding active voting electorate membership. Because a leader excluded by a committed configuration change continues to lead as a transitional ghost leader to drive configuration convergence, the
  designated leader at the head of `suggested` may itself be excluded from the active electorate. The external coordinator must never mutate its presumed active electorate based on `AskTheLeader`. Active electorate membership is updated
  strictly upon receiving definitive terminal outcomes (`SuccessfullyChanged`, `AlreadyChanged`, or explicit membership derivation from committed configuration changes).
