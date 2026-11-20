---
type: "Milestone"
id: "M-A"
title: "Physical Network Transport Bridge"
status: "COMPLETED"
progress_percentage: 100
target_modules: ["nexus/cluster", "consensus/core", "consensus/nio2-cluster-bridge"]
prerequisites: ["single-group-consensus-engine"]
timestamp: "2026-11-20T01:40:00Z"
---

# Milestone A: Physical Network Transport Bridge (`consensus/nio2-cluster-bridge` &harr; `nexus/cluster`)

**Status**: `[COMPLETED]` | **Progress**: 100% | **Target Modules**: `nexus/cluster`, `consensus/core`, `consensus/nio2-cluster-bridge` | **Parent Roadmap**: [Matrix System Architecture Roadmap](../index.md)

---

## 1. Objective & Architectural Value

- **Objective**: Implement a production-grade adapter (`Nio2ClusterBridgeCdm`) in `consensus/nio2-cluster-bridge` binding `ConsensusClusterBridgeCdm.ClusterBridge` to non-blocking NIO2 socket channels and serializers in `nexus/cluster`.
- **Architectural Value**: Transitions the single-group consensus state machine from an in-memory simulation into a real distributed system capable of running across separate processes and physical hosts over TCP. Validates wire serialization, non-blocking transmitter backpressure, and network socket lifecycle handling under real latency.
- **Delegated Snapshot Chunking Invariant**: Leverages Lazy Raft's heartbeat-less architecture to delegate byte source chunking and frame streaming entirely to the cluster transport layer, keeping consensus RPC contracts atomic.

---

## 2. Scope & Deliverables

### In-Scope

- **Consensus Message Codecs**: Implement binary serializers and deserializers in `consensus/nio2-cluster-bridge` for all consensus records: `StateInfo`, `Ballot`, `AppendRecords`, `InstallSnapshot`, client commands, and responses.
- **Outbound RPC Dispatch**: Map consensus bridge calls (`howAreYou`, `requestBallot`, `appendRecords`, `installSnapshot`, `permitQuiescence`) to `nexus/cluster` `PipelinedTransmitter` instances (superseding the legacy `SequentialTransmitter` / `Transmitter` pair, which remains only as a candidate for future removal if confirmed unnecessary).
- **Inbound Message Dispatch**: Dispatch received network frames to `ClusterBridge.Delegate` methods (`onHowAreYou`, `onRequestBallot`, `onAppendRecords`, `onInstallSnapshot`, `onCommandFromClient`) strictly within the participant's `sequencer` thread.
- **Cluster Electorate Integration**: Translate cluster topology and connectivity changes into `requestElectorateChange` calls.
- **Multi-Process Test Harness**: Implement an automated test suite executing a multi-node cluster over loopback TCP sockets.
- **Transport Snapshot Streaming & Partitioning**: Support streaming arbitrary-sized snapshot byte sources through `nexus/cluster` without circular storage buffer exhaustion.
- **Adaptive Record Batch Encoding & Streaming**: Encode `Record` batches in `AppendRecords` and `InstallSnapshot` RPCs according to a configurable `RecordBatchEncodingStrategy` selector (`WithinControlPackage`, `StagedOnSequencer`, `StagedOnCodecExecutor`) and automatically place pre-encoded batches inline or out-of-line via `ByteSource` based on exact encoded byte length and handshake-negotiated control-package capacity.

### Out-of-Scope

- Co-located persistent disk storage (handled in Milestone M-B).
- Shard routing and multi-raft coordination (handled in Milestone M-D).

---

## 3. Dependencies & Prerequisites

- **Upstream Prerequisite**: Complete and verified single-group consensus state machine (`ConsensusParticipantSdm`).
- **Downstream Dependents**: Milestone M-D (Multi-Raft Sharding & Control Plane Coordinator).

---

## 4. Detailed Work Breakdown

### Completed Foundational Deliverables

- [x] Define binary frame codecs and wire protocol hierarchy in `consensus/nio2-cluster-bridge` for consensus and client RPCs (`ClusterWireProtocol`).
- [x] Implement `Nio2ClusterBridgeCdm` fulfilling the `ConsensusClusterBridgeCdm.ClusterBridge` interface.
- [x] Connect outbound consensus RPC invocations to `nexus/cluster` `PipelinedTransmitter` instances.
- [x] Implement the inbound network reception pipeline routing frames to `ClusterBridge.Delegate` within the participant's `sequencer`.
- [x] Wire cluster membership events to consensus electorate transition triggers.
- [x] Implement an end-to-end multi-process test suite verifying linearizable consensus over real loopback network sockets.
- [x] Implement transport-level byte source streaming and eager pre-delivery in `nexus/cluster` to transmit arbitrary snapshot byte sources without circular storage buffer exhaustion.

### Pending Remediation & Hardening (Categorized)

#### Category 1: Correctness & Concurrency Defects
- [x] `[Correctness / Concurrency]` **Synchronous Trampoline Race Remedy (Defect C1)**: Refactor `ByteSource.AsyncReader` and `ByteSink` to report synchronous completion status via return values (or implement an atomic CAS state machine on completion handlers), eliminating completion thread races that permanently wedge socket transmission pipelines.
- [x] `[Correctness / Wire-Framing]` **Trailing Stream Leftover Buffer Management (Defects C2 & O2)**: Eliminate transfer buffer overflow by streaming incoming chunks directly into `circularStorage.writeEnd` buffers (saving 64 KiB direct memory per connection and eliminating cross-buffer copying), or implement multi-buffer slicing across `circularStorage.advanceWriteEnd()` in `finishTrailingStream` to prevent `BufferOverflowException` and limit-clipping on pipelined messages.
- [x] `[Correctness / Protocol]` **Zero-Length ByteSource Inline Handling (Defect C3)**: Bypass trailing stream queueing and network sentinels when `source.size == 0L`, projecting an empty sealed `ByteSource` directly on the receiver to prevent spurious `DeserializerAndFrameMismatch` faults and connection restarts.
- [x] `[Correctness / DoS-Defense]` **Chunk Header Length & Continuation Validation (Defect C4)**: Bound VLQ continuation bytes to `VLQ.INT_MAX_LENGTH` (5 bytes), reject negative chunk lengths, and bound chunk sizes to remaining expected stream bytes (excepting sentinels `0` and `CORRUPTED_PACKAGE_SENTINEL`) to prevent CPU busy-spins and stream inflation.
- [x] `[Correctness / Storage-Contract]` **FileByteSink Write Contract & Seal Failure Propagation (Defect C5)**: Implement an internal drain loop in `FileByteSink.write` to handle partial filesystem writes, and propagate `channel.close()` exceptions from `seal()` to the receiver as transport faults.
- [x] `[Correctness / Protocol]` **Mid-Stream Transport Fault Policy Alignment (Defect C6)**: Formally declare mid-stream transport faults as channel-fatal, triggering immediate socket teardown and delegate restart to prevent unconsumed trailing chunks from corrupting subsequent control packages, aligning code and ADR.
- [x] `[Correctness / Invariant]` **Transmitter Single-Flight Transmission Guard Retention (Defect C7)**: Retain the active-transmission guard across the entire trailing stream phase until transmission completion executes, preventing concurrent transmission calls from corrupting stream queues (superseded in `PipelinedTransmitter` by `CoordinationState` stage ownership and replaced in the standalone `Transmitter` by `transmissionInProgress`).
- [x] `[Correctness / Lifecycle]` **ByteSource Lifecycle Ownership & Fallback Temp-File Cleanup (Defect C8)**: Attach a fallback JVM `Cleaner` to `FileByteSource` instances configured with `deleteOnClose = true` to prevent orphan staging files on unclosed/rejected consensus snapshots, and formalize ownership obligations in `Workspace`/`Storage` SPI Scaladoc.
- [x] `[Correctness / Protocol]` **Transmitter `getBuffer` Behind-Transmission Sentinel Verification (Defect C9)**: Empirically verify and correct the `sendFrame(writeEndBuffer, false)` parameterization in `getBuffer` to guarantee mid-package terminator sentinels are not emitted when buffers fill during serialization (subsequently superseded by full-package serialize-before-transmit in both `PipelinedTransmitter` and `Transmitter`).

#### Category 2: Performance & Allocation Efficiency
- [x] `[Efficiency / Allocation]` **Transmitter Reusable State Hoisting (Optimization O1)**: Eliminate per-message `LazyRef`, closure, and primitive boxing heap allocations (`ObjectRef`, `LongRef`, `BooleanRef`, `IntRef`) across non-streaming and fast-path messages. `PipelinedTransmitter` hoists its writers, completion handlers, `invokeWithoutAttachment` adapter, and isolated per-stage state to class/companion level (allocating only a `MsgSubmission` envelope when queueing under contention); the legacy `Transmitter` and `SequentialTransmitter` pair have likewise been refactored with hoisted class-level state and handlers.
- [x] `[Efficiency / I/O-Amplification]` **Small-Write Coalescing in `FileByteSink` (Optimization O4)**: Coalesce sub-block writes synchronously inside `FileByteSink` using a lazily allocated heap staging buffer (default 64 KiB, capped to `expectedSize`) until a full block accumulates, `expectedSize` is reached, or `seal()` runs, bypassing staging with zero copy for full-block or stream-completing writes and hoisting the internal `CompletionHandler` to eliminate per-write handler allocations.
- [x] `[Efficiency / Memory]` **BufferByteSink Heap Buffers & Constructor Buffer Injection (Optimization O5)**: `BufferByteSink` now provides a primary constructor wrapping an existing writable `ByteBuffer` directly (deriving `expectedSize = backing.remaining().toLong` across its initial `[position, limit)` range) and a secondary `byteLength: Int` constructor allocating a heap `ByteBuffer` (`ByteBuffer.allocate`) for small streams ($\le 64$ KiB) via `ByteSink.DefaultAllocator` to avoid direct-memory zeroing and GC cleaner overhead.

#### Category 3: Code Hygiene & Repository Cleanliness
- [x] `[Hygiene / Formatting]` **Import & Line-Ending Normalization**: Replace the redundant `scala.util.control.NonFatal(e)` qualification in `Receiver.scala` with the imported `NonFatal`, remove empty import lines in `ConsensusProtocolSerializers.scala`, and standardize CRLF/LF line endings across modified transport and consensus files.
- [x] `[Hygiene / Cleanliness]` **Redundant Snapshot Forwarder Removal**: Remove redundant snapshot alias files (`FileSnapshotByteSource.scala` in `consensus/nio2-cluster-bridge` and `consensus/nio2-persistence`, and `BufferSnapshotByteSource.scala` in `consensus/core/src/test/scala/env/`) once callers reference `FileByteSource` and `BufferByteSource` directly.
- [x] `[Configuration / Clarity]` **Asymmetric TX/RX Buffer & Gathering Configuration**: Separate transmission and reception `ringMaxLength` limits in `ClusterBridgeConfig` (as `maxRingLengthForTransmission` and `maxRingLengthForReception`, alongside `sizeOfBuffersInRing`) and `ParticipantService.DelegateConfig` (as `transmitterRingMaxLength` and `receiverRingMaxLength`) with aggregate capacity validation, and expose `maxGatheringBuffers` in `ClusterBridgeConfig`.

#### Category 4: Documentation & Invariant Integrity
- [x] `[Documentation / Integrity]` **Documentation Restoration & Specification Alignment**: Restore deleted invariant comments in `Transmitter` and `Receiver`, reattach orphaned Scaladoc to `readFull` in `Deserializer.scala`, reconcile method signatures in `adr-trailing-stream-bulk-byte-source-transfer.md`, eliminate unsubstantiated specification claims (disk preallocation, memory unmapping, `sendfile` transport), and purge session-debugging local variable names from ADR invariant titles.

#### Category 5: Automated Test Suite Hardening
- [x] `[Testing / Verification]` **Automated Test Coverage Matrix Hardening (Gap D7)**: Implement dedicated test scenarios covering zero-length byte sources, multiple sources per message, in-band stream aborts, pipelined messages immediately following a stream, malformed/excessive chunk headers, and asynchronous completion races.

#### Category 6: Wire Protocol Scaling & Payload Streaming
- [x] `[Scaling / Wire-Protocol]` **Configurable Record-Batch Encoding Strategy and Exact-Size Out-of-Line `ByteSource` Streaming for `AppendRecords` and `InstallSnapshot`**: Add `RecordBatchEncodingStrategy` (`WithinControlPackage`, `StagedOnSequencer`, `StagedOnCodecExecutor`) and a configurable selector (`IArray[Record] => RecordBatchEncodingStrategy`, defaulting to `WithinControlPackage` for empty batches and `StagedOnCodecExecutor` for non-empty batches) to `ClusterBridgeConfig` (`consensus/nio2-cluster-bridge`). Pre-encode staged batches in an isolated reference-deduplication scope (`ChunkedHeapWriter` / `ChunkedBufferByteSource`), advertise `maxInboundControlPackageBytes` in `PeerHandshake` and `HandshakeAck`, place pre-encoded batches inline (`RecordBatch.EncodedInline`, tag `1`, decoded via `NestedScopeReader`) when they fit within the negotiated per-peer inline ceiling or out-of-line (`RecordBatch.EncodedOutOfLine`, tag `2`, decoded off-`sequencer` via `ReadableChannelReader`) when they exceed it, preserve per-peer FIFO RPC delivery ordering across canonical `PeerConnection` instances, fail pending RPCs fast on `PipelinedTransmitter.NotDelivered` reports, and close temporary batch and snapshot `ByteSource` instances deterministically across all completion and error paths.

---

## 5. Acceptance Criteria & Verification

1. All consensus RPCs and client commands serialize and deserialize losslessly across network channels without memory leaks.
2. Inter-node consensus executes successfully across distinct JVM processes connected over TCP loopback sockets.
3. Inbound network frames execute on the consensus participant's `sequencer` without concurrency violations or race conditions.
4. Electorate changes and node failures trigger proper leader elections and linearizable command replication over physical sockets.
5. Snapshot byte sources exceeding transport buffer capacity stream across network channels without exhausting `DualEndedCircularStorage` or requiring intermediate heap allocations.
6. Asynchronous completion trampolines are completely race-free across thread boundaries, guaranteeing zero connection stalls or deadlocks.
7. Pipelined messages following trailing streams are received without buffer overflow exceptions or byte truncation.

---

## 6. References

- [Multi-Raft System Architecture: Cluster Transport](../../architecture/system-architecture.md#6-runtime-integration--component-relationships)
- [Lazy Raft Consensus Protocol Specification: Host Bridge Contract](../../architecture/lazy-multi-raft-consensus.md#i-cluster-communication-bridge-clusterbridge)
- [ConsensusParticipantSdm Source Code](file:///C:/Projects/tools/matrix-ai/consensus/core/src/main/scala/ConsensusParticipantSdm.scala)
- [ParticipantService Source Code](file:///C:/Projects/tools/matrix-ai/nexus/cluster/src/main/scala/cluster/service/ParticipantService.scala)
- [PipelinedTransmitter Source Code](file:///C:/Projects/tools/matrix-ai/nexus/cluster/src/main/scala/cluster/channel/PipelinedTransmitter.scala)
- [Remediation Plan: Serialization & Transport Streaming Tier](../../../.gemini/antigravity/brain/9329ef1a-b9df-4fa3-80b3-23f1379fe343/plan-remediation-streaming-transport.md)

---

## 7. Deferred Architectural Enhancements

Non-blocking post-M-A transport optimizations (`O3`, `O6`, `O7`, `O8`), the Consensus Snapshot Concurrency Governor and Elastic Transport Streaming Pool design, and legacy transmitter pair (`SequentialTransmitter`/`Transmitter`) removal evaluation have been relocated to the dedicated [Transport Optimization & Cleanup Backlog](../backlog-transport-optimizations.md).
