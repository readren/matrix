---
type: "Milestone"
id: "M-B"
title: "Durable Co-located Storage Engine"
status: "TODO"
progress_percentage: 0
target_modules: ["consensus", "common"]
prerequisites: ["single-group-consensus-engine"]
timestamp: "2026-10-31T15:45:00Z"
---

# Milestone B: Durable Co-located Storage Engine (`Workspace` Implementation)

**Status**: `[TODO]` | **Progress**: 0% | **Target Modules**: `consensus`, `common` | **Parent Roadmap**: [Matrix System Architecture Roadmap](../index.md)

---

## 1. Objective & Architectural Value

- **Objective**: Implement a high-performance, durable `ConsensusPrimaryStateCdm.Workspace` and `Storage` engine adhering strictly to Section 4 of the [Multi-Raft System Architecture](../../architecture/system-architecture.md).
- **Architectural Value**: Guarantees true physical durability by anchoring term/vote metadata, log buffer append/truncation, and snapshot images to persistent disk storage (e.g. via memory-mapped append-only WAL files or embedded RocksDB).
  Validates crash-recovery, log suffix truncation, and snapshot installation against real non-volatile media.

---

## 2. Scope & Deliverables

### In-Scope

- **Metadata Storage**: Atomic persistence of `currentTerm` and `votedFor` metadata records.
- **Append-Only Write-Ahead Log (WAL)**: Sequential log segment writer supporting synchronous `fsync` barriers and non-blocking background flush.
- **Suffix Truncation Engine**: Fast log suffix truncation on conflicting leader terms without copying or rewriting intact log prefixes.
- **Prefix Compaction & Snapshots**: Log prefix truncation past snapshot boundaries (`truncatePrefix`) and snapshot persistence/streaming.
- **Crash Recovery Initializer**: Reconstructing the volatile and persistent `PrimaryState` components from on-disk WAL records and snapshot images upon process restart.
- **Durability Verification Suite**: Adversarial process-kill and crash-injection test suite verifying no data loss or linearizability violation occurs across restarts.

### Out-of-Scope

- Network transport (handled in Milestone M-A).
- Multi-raft shard multiplexing (handled in Milestone M-D).

---

## 3. Dependencies & Prerequisites

- **Upstream Prerequisite**: Complete and verified single-group consensus state machine (`ConsensusParticipantSdm`).
- **Downstream Dependents**: Milestone M-D (Multi-Raft Sharding & Control Plane Coordinator).

---

## 4. Detailed Work Breakdown

- [ ] Design binary disk format for term/vote metadata and append-only WAL log record segments.
- [ ] Implement `DurableWorkspace` fulfilling the `ConsensusPrimaryStateCdm.Workspace` interface.
- [ ] Implement `DurableStorage` fulfilling the `Storage` service interface.
- [ ] Implement atomic suffix truncation (`truncateSuffix`) preserving prior valid record indices.
- [ ] Implement snapshot serialization, storage, and log prefix compaction (`truncatePrefix`).
- [ ] Implement crash-recovery initializer reconstructing state machine and consensus state from disk.
- [ ] Build crash-injection test suite verifying persistence invariants across process kill/restart cycles.

---

## 5. Acceptance Criteria & Verification

1. All term updates, vote grants, and committed records are durably persisted to non-volatile disk storage before RPC response.
2. Suffix truncation correctly discards conflicting uncommitted entries without corrupting valid prefix entries or snapshots.
3. Process crashes and subsequent restarts successfully recover `PrimaryState` to the exact pre-crash committed watermark.
4. Snapshot installation completely cleans compacted log segments and restores state machine state without memory leaks.

---

## 6. References

- [Multi-Raft System Architecture: Agnostic Persistence via IoC](../../architecture/system-architecture.md#4-agnostic-persistence-via-inversion-of-control-ioc)
- [Lazy Raft Consensus Protocol Specification: Storage SPI Contract](../../architecture/lazy-multi-raft-consensus.md#ii-storage-spi-contract-workspace--storage--persistence-boundary)
- [ConsensusPrimaryStateCdm Source Code](file:///C:/Projects/tools/matrix/consensus/core/src/main/scala/ConsensusPrimaryStateCdm.scala)
