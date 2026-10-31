---
type: "Milestone"
id: "M-A"
title: "Physical Network Transport Bridge"
status: "TODO"
progress_percentage: 0
target_modules: ["nexus/cluster", "consensus"]
prerequisites: ["single-group-consensus-engine"]
timestamp: "2026-10-31T15:45:00Z"
---

# Milestone A: Physical Network Transport Bridge (`consensus` &harr; `nexus/cluster`)

**Status**: `[TODO]` | **Progress**: 0% | **Target Modules**: `nexus/cluster`, `consensus` | **Parent Roadmap**: [Matrix System Architecture Roadmap](../index.md)

---

## 1. Objective & Architectural Value

- **Objective**: Implement a production-grade adapter binding `ConsensusParticipantSdm.ClusterParticipant` to `ParticipantService` over non-blocking NIO2 socket channels in `nexus/cluster`.
- **Architectural Value**: Transitions the single-group consensus state machine from an in-memory simulation into a real distributed system capable of running across separate processes and physical hosts over TCP. Validates wire
  serialization, non-blocking transmitter backpressure, and network socket lifecycle handling under real latency.

---

## 2. Scope & Deliverables

### In-Scope

- **Consensus Message Codecs**: Implement binary serializers and deserializers in `nexus/cluster` for all consensus records: `StateInfo`, `Ballot`, `AppendRecords`, client commands, and responses.
- **Outbound RPC Dispatch**: Map consensus bridge calls (`howAreYou`, `requestBallot`, `appendRecords`) to `nexus/cluster` transmitters (`SequentialTransmitter`).
- **Inbound Message Dispatch**: Dispatch received network frames to `ClusterParticipant.Delegate` methods (`onHowAreYou`, `onRequestBallot`, `onAppendRecords`, `onCommandFromClient`) strictly within the participant's `sequencer` thread.
- **Cluster Electorate Integration**: Translate cluster topology and connectivity changes into `requestElectorateChange` calls.
- **Multi-Process Test Harness**: Implement an automated test suite executing a multi-node cluster over loopback TCP sockets.

### Out-of-Scope

- Co-located persistent disk storage (handled in Milestone M-B).
- Shard routing and multi-raft coordination (handled in Milestone M-D).

---

## 3. Dependencies & Prerequisites

- **Upstream Prerequisite**: Complete and verified single-group consensus state machine (`ConsensusParticipantSdm`).
- **Downstream Dependents**: Milestone M-D (Multi-Raft Sharding & Control Plane Coordinator).

---

## 4. Detailed Work Breakdown

- [ ] Define binary frame codecs and macro derivations in `nexus/cluster` for consensus protocol messages (`StateInfo`, `Ballot`, `AppendRecords`, `Record`).
- [ ] Implement `ClusterParticipantAdapter` fulfilling the `ConsensusParticipantSdm.ClusterParticipant` interface.
- [ ] Connect outbound consensus RPC invocations to `nexus/cluster` `Transmitter` instances.
- [ ] Implement the inbound network reception pipeline routing frames to `ClusterParticipant.Delegate` within the participant's `sequencer`.
- [ ] Wire cluster membership events to consensus electorate transition triggers.
- [ ] Implement an end-to-end multi-process test suite verifying linearizable consensus over real loopback network sockets.

---

## 5. Acceptance Criteria & Verification

1. All consensus RPCs and client commands serialize and deserialize losslessly across network channels without memory leaks.
2. Inter-node consensus executes successfully across distinct JVM processes connected over TCP loopback sockets.
3. Inbound network frames execute on the consensus participant's `sequencer` without concurrency violations or race conditions.
4. Electorate changes and node failures trigger proper leader elections and linearizable command replication over physical sockets.

---

## 6. References

- [Multi-Raft System Architecture: Cluster Transport](../../architecture/system-architecture.md#6-runtime-integration--component-relationships)
- [Lazy Raft Consensus Protocol Specification: Host Bridge Contract](../../architecture/lazy-multi-raft-consensus.md#i-cluster-communication-bridge-clusterparticipant)
- [ConsensusParticipantSdm Source Code](file:///C:/Projects/tools/matrix/consensus/src/main/scala/ConsensusParticipantSdm.scala)
- [ParticipantService Source Code](file:///C:/Projects/tools/matrix/nexus/cluster/src/main/scala/cluster/service/ParticipantService.scala)
