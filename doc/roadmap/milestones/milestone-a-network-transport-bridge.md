---
type: "Milestone"
id: "M-A"
title: "Physical Network Transport Bridge"
status: "IN_PROGRESS"
progress_percentage: 85
target_modules: ["nexus/cluster", "consensus/core", "consensus/nio2-cluster-bridge"]
prerequisites: ["single-group-consensus-engine"]
timestamp: "2026-11-07T17:06:00Z"
---

# Milestone A: Physical Network Transport Bridge (`consensus/nio2-cluster-bridge` &harr; `nexus/cluster`)

**Status**: `[IN_PROGRESS]` | **Progress**: 85% | **Target Modules**: `nexus/cluster`, `consensus/core`, `consensus/nio2-cluster-bridge` | **Parent Roadmap**: [Matrix System Architecture Roadmap](../index.md)

---

## 1. Objective & Architectural Value

- **Objective**: Implement a production-grade adapter (`Nio2ClusterBridgeCdm`) in `consensus/nio2-cluster-bridge` binding `ConsensusClusterBridgeCdm.ClusterBridge` to non-blocking NIO2 socket channels and serializers in `nexus/cluster`.
- **Architectural Value**: Transitions the single-group consensus state machine from an in-memory simulation into a real distributed system capable of running across separate processes and physical hosts over TCP. Validates wire serialization, non-blocking transmitter backpressure, and network socket lifecycle handling under real latency.
- **Delegated Snapshot Chunking Invariant**: Leverages Lazy Raft's heartbeat-less architecture to delegate payload chunking and frame streaming entirely to the cluster transport layer, keeping consensus RPC contracts atomic.

---

## 2. Scope & Deliverables

### In-Scope

- **Consensus Message Codecs**: Implement binary serializers and deserializers in `consensus/nio2-cluster-bridge` for all consensus records: `StateInfo`, `Ballot`, `AppendRecords`, `InstallSnapshot`, client commands, and responses.
- **Outbound RPC Dispatch**: Map consensus bridge calls (`howAreYou`, `requestBallot`, `appendRecords`, `installSnapshot`, `permitQuiescence`) to `nexus/cluster` transmitters (`SequentialTransmitter`).
- **Inbound Message Dispatch**: Dispatch received network frames to `ClusterBridge.Delegate` methods (`onHowAreYou`, `onRequestBallot`, `onAppendRecords`, `onInstallSnapshot`, `onCommandFromClient`) strictly within the participant's `sequencer` thread.
- **Cluster Electorate Integration**: Translate cluster topology and connectivity changes into `requestElectorateChange` calls.
- **Multi-Process Test Harness**: Implement an automated test suite executing a multi-node cluster over loopback TCP sockets.
- **Transport Snapshot Streaming & Partitioning**: Support streaming arbitrary-sized snapshot payloads through `nexus/cluster` without circular storage buffer exhaustion.

### Out-of-Scope

- Co-located persistent disk storage (handled in Milestone M-B).
- Shard routing and multi-raft coordination (handled in Milestone M-D).

---

## 3. Dependencies & Prerequisites

- **Upstream Prerequisite**: Complete and verified single-group consensus state machine (`ConsensusParticipantSdm`).
- **Downstream Dependents**: Milestone M-D (Multi-Raft Sharding & Control Plane Coordinator).

---

## 4. Detailed Work Breakdown

- [x] Define binary frame codecs and wire protocol hierarchy in `consensus/nio2-cluster-bridge` for consensus and client RPCs (`ClusterWireProtocol`).
- [x] Implement `Nio2ClusterBridgeCdm` fulfilling the `ConsensusClusterBridgeCdm.ClusterBridge` interface.
- [x] Connect outbound consensus RPC invocations to `nexus/cluster` `Transmitter` instances.
- [x] Implement the inbound network reception pipeline routing frames to `ClusterBridge.Delegate` within the participant's `sequencer`.
- [x] Wire cluster membership events to consensus electorate transition triggers.
- [x] Implement an end-to-end multi-process test suite verifying linearizable consensus over real loopback network sockets.
- [ ] Implement transport-level snapshot payload streaming and continuous receiver de-accumulation in `nexus/cluster` to transmit arbitrary snapshot payloads without circular storage buffer exhaustion.

---

## 5. Acceptance Criteria & Verification

1. All consensus RPCs and client commands serialize and deserialize losslessly across network channels without memory leaks.
2. Inter-node consensus executes successfully across distinct JVM processes connected over TCP loopback sockets.
3. Inbound network frames execute on the consensus participant's `sequencer` without concurrency violations or race conditions.
4. Electorate changes and node failures trigger proper leader elections and linearizable command replication over physical sockets.
5. Snapshot payloads exceeding transport buffer capacity stream across network channels without exhausting `DualEndedCircularStorage` or requiring intermediate heap allocations.

---

## 6. References

- [Multi-Raft System Architecture: Cluster Transport](../../architecture/system-architecture.md#6-runtime-integration--component-relationships)
- [Lazy Raft Consensus Protocol Specification: Host Bridge Contract](../../architecture/lazy-multi-raft-consensus.md#i-cluster-communication-bridge-clusterbridge)
- [ConsensusParticipantSdm Source Code](file:///C:/Projects/tools/matrix/consensus/core/src/main/scala/ConsensusParticipantSdm.scala)
- [ParticipantService Source Code](file:///C:/Projects/tools/matrix/nexus/cluster/src/main/scala/cluster/service/ParticipantService.scala)
