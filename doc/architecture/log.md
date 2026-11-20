---
type: "Log"
title: "Knowledge Base Change Log"
description: "History of modifications to the OKF bundle."
tags: ["log", "changelog"]
timestamp: "2026-11-19T22:36:00Z"
---

## [2026-11-19T22:36:00Z]
- **Updated** `cluster-serialization.md`: Aligned quiescent pruning signature to `pruneTo(targetLength)` and aligned `ParticipantService.DelegateConfig` field names (`transmitterRingMaxLength` and `receiverRingMaxLength`) in the Aggregate Transport Capacity Invariant.

## [2026-11-19T18:55:00Z]
- **Updated** `adr-record-batch-encoding-placement.md`, `adr-trailing-stream-bulk-byte-source-transfer.md`, and `cluster-serialization.md`: Aligned `RingLength`-oriented transport configuration and exception names (`RingLengthExceededException`, `sizeOfBuffersInRing`, `maxRingLengthForTransmission`, `maxRingLengthForReception`, `minRingLengthOnIdle`, `ringMaxLength`, `ringMinLengthOnIdle`).

## [2026-11-19T15:47:00Z]
- **Updated** `adr-record-batch-encoding-placement.md`: Renamed `maxInlineBuffersPerTxRing` to `stagedBatchInlineThreshold`, typed positive ring-buffer counts via the `RingLength` opaque subtype of `Int` (`>= 1`) across `nexus/cluster` and `ClusterBridgeConfig`, and removed the zero out-of-line special case.

## [2026-11-19T04:38:00Z]
- **Updated** `adr-record-batch-encoding-placement.md`: Replaced `maxInlineEncodedBatchBytes` and `DeriveFromTransmitterControlCapacity` with the transmitter-ring buffer-count threshold `maxInlineBuffersPerTxRing` (defaulting to $2 \times \text{DefaultMinBuffersOnIdle} = 4$ buffers) in the placement equation and Negotiated Inline Placement Bound Invariant.

## [2026-11-19T01:52:00Z]
- **Updated** `adr-record-batch-encoding-placement.md`: Replaced the static `ControlPackageHeadroomBytes` heuristic with per-message `envelopeBytes` measurement (fixed primitive bound for `AppendRecords` and counting `Serializer.Writer` pass over `SnapshotData[ParticipantId]` with `putByteSource` overridden for `InstallSnapshot`, plus buffer-boundary alignment slack) and documented `ClusterBridgeConfig.maxInlineEncodedBatchBytes` as the origin of `effectiveLocalMaxInlineBatchBytes`.

## [2026-11-18T22:30:00Z]
- **Updated** `adr-record-batch-encoding-placement.md` and `cluster-serialization.md`: Updated `Receiver` initial buffer write limit invariants to reserve `FRAME_HEADER_MAX_SIZE - 1` trailing headroom (`buffersCapacity - FRAME_HEADER_MAX_SIZE + 1`) and refined the single-frame peer control-package payload capacity formula to `maxBuffersInRxRing * (sizeOfRingBuffers - FRAME_HEADER_MAX_SIZE + 1) - FRAME_HEADER_MAX_SIZE - 1`.

## [2026-11-18T19:22:00Z]
- **Updated** `adr-record-batch-encoding-placement.md`: Corrected the peer `Receiver` control-package payload capacity formula to subtract $2 \times \text{FRAME\_HEADER\_MAX\_SIZE}$ for the single control frame header and terminal sentinel while retaining the per-buffer `buffersInitialLimit` factor, and clarified that `PipelinedTransmitter` emits a single-frame control package.

## [2026-11-18T17:27:00Z]
- **Updated** `adr-record-batch-encoding-placement.md` and `cluster-serialization.md`: Aligned `ClusterBridgeConfig` control-capacity formulas and invariant descriptions with `sizeOfRingBuffers`, `maxBuffersPerTxRing`, and `maxBuffersInRxRing`, distinguishing them from `ParticipantService.DelegateConfig`'s `frameCapacity`, `transmitterMaxBuffers`, and `receiverMaxBuffers`.

## [2026-11-18T04:30:00Z]
- **Created** `adr-record-batch-encoding-placement.md` and **Updated** `index.md`, `byte-streaming-primitives.md`, and `cluster-serialization.md`: Documented the architectural decision and invariants keeping `ConsensusParticipantSdm` size-agnostic via `RecordBatchEncodingStrategy` selection, handshake-negotiated inline-vs-out-of-line batch placement, isolated batch reference-deduplication scopes (`ChunkedHeapWriter`, `ChunkedBufferByteSource`, `NestedScopeReader`, `ReadableChannelReader`), canonical `PeerConnection` outbound backlog draining, and single-flight pause-and-resume inbound batch decoding.

## [2026-11-17T21:14:00Z]
- **Updated** `cluster-serialization.md` and `lazy-multi-raft-consensus.md`: Reflected separate `transmitterMaxBuffers` and `receiverMaxBuffers` capacity validation across `ParticipantService.DelegateConfig` and `ClusterBridgeConfig`, and removed the stale `FileSnapshotByteSource` alias reference from `lazy-multi-raft-consensus.md`.

## [2026-11-17T13:22:00Z]
- **Updated** `byte-streaming-primitives.md`: Formalized symmetric no-inline-callback and synchronous entry-time failure semantics across `ByteSource.AsyncReader.read` and `ByteSink.write`, specifying that closed-resource and entry-time failures throw synchronously (`ClosedChannelException`) without invoking `CompletionHandler` prior to returning `PENDING`.

## [2026-11-17T12:20:00Z]
- **Updated** `byte-streaming-primitives.md`: Documented `FileByteSink` small-write block coalescing using a lazily allocated heap staging buffer (default 64 KiB, capped to `expectedSize`), zero-copy bypass for full-block and stream-completing writes, synchronous tail flushing on `seal()`, and hoisted single-flight completion handling.

## [2026-11-17T11:38:00Z]
- **Updated** `byte-streaming-primitives.md`, `cluster-serialization.md`, and `cluster-serialization-guide.md`: Updated `BufferByteSink` in-memory tier specifications for small streams ($\le 64$ KiB) from direct memory allocation to heap `ByteBuffer` allocation and noted direct `ByteBuffer` wrapping support.

## [2026-11-17T10:13:00Z]
- **Updated** `cluster-serialization.md`: Refined the Asynchronous Completion Loop Trampolining Invariant to attribute iterative completion trampolining specifically to `PipelinedTransmitter` and `Receiver`, and documented the non-trampolined trailing-stream contract of `Transmitter`.

## [2026-11-17T09:41:00Z]
- **Updated** `byte-streaming-primitives.md` and `adr-trailing-stream-bulk-byte-source-transfer.md`: Clarified the relation between the atomic stream trampolines (`PipelinedTransmitter` and `Receiver`) and the `ByteSource.AsyncReader` / `ByteSink` contract expectation that callers without an iterative trampoline rely on synchronous completions being returned via the `Int` return value rather than inline `CompletionHandler` invocations prior to `PENDING`.

## [2026-11-17T00:59:00Z]
- **Updated** `cluster-serialization.md`: Documented `SequentialTransmitter`'s atomic `pruning` exclusivity around `pruneTo`, bounded wait-queue admission control (`RejectedExecutionException` on `MaxQueuedCount`), per-message submission parameter preservation, and strict FIFO callback ordering prior to starting queued transmissions.

## [2026-11-16T16:12:00Z]
- **Updated** `cluster-serialization.md` and `adr-trailing-stream-bulk-byte-source-transfer.md`: Documented the simplified serialize-before-transmit single-frame control package behavior and bounded gathering writes of `Transmitter` (and `SequentialTransmitter`), including zero-wire-byte fail-fast reporting on control serialization and buffer-capacity failures, omission of frame length headers on zero-byte control payloads, and retention of the single-flight active-transmission guard across trailing streams.

## [2026-11-15T18:20:00Z]
- **Updated** `cluster-serialization.md`: Refined Section 8 terminology so the 3-bit value paired with the 28-bit wait-queue count inside the 31-bit coordination state is described as the joint stage state rather than a pipeline mode.

## [2026-11-15T17:24:00Z]
- **Updated** `cluster-serialization.md`: Refined the `PipelinedTransmitter` coordination-state specification in Section 8 from two coupled per-stage sub-states to a single unified 6-mode pipeline state (`idle`, `serializing-only`, `transmitting-only`, `pipelined`, `handoff-pending`, and `pruning`) paired with the 28-bit wait-queue counter.

## [2026-11-13T21:35:00Z]
- **Updated** `cluster-serialization.md` and `adr-trailing-stream-bulk-byte-source-transfer.md`: Clarified `PipelinedTransmitter` pipeline descriptions so stages represent isolated operational phases and state compartments coordinated across calling and NIO.2 completion threads rather than active actors.

## [2026-11-13T16:34:00Z]
- **Updated** `cluster-serialization.md`, `cluster-serialization-guide.md`, `adr-trailing-stream-bulk-byte-source-transfer.md`, `lazy-multi-raft-consensus.md`, and `log.md`: Standardized all pipeline stage terminology from numbered stage labels to explicit serialization and transmission stage terminology.

## [2026-11-13T16:13:00Z]
- **Updated** `cluster-serialization.md`: Documented the Bounded Wait-Queue Admission Control Invariant for `PipelinedTransmitter` (31-bit non-negative coordination state with a 28-bit wait-queue capacity limit of `2^28 - 1`, synchronous `RejectedExecutionException` on queue saturation prior to state mutation, preservation of strict FIFO serialized completion callbacks, and caller exception propagation semantics).

## [2026-11-12T23:59:00Z]
- **Updated** `adr-trailing-stream-bulk-byte-source-transfer.md`: Formalized the Atomic Work-Stealing Stream Trampoline Invariant (serializing synchronous returns, inline pre-return `PENDING` callbacks, and multi-threaded asynchronous completions in $O(1)$ stack depth with at-most-once stream finalization), the Deferred Sink Sealing and Rollback Unlinking Invariant (ensuring already-sealed staging sinks of a multi-stream message are unlinked if a later stream in the same package aborts or faults), and the Unified Channel Virtualization Invariant across both circular storage framing reads and trailing stream chunk reads while preserving chronological order of staged leftover bytes.

## [2026-11-12T23:55:00Z]
- **Updated** `cluster-serialization.md`, `cluster-serialization-guide.md`, `adr-trailing-stream-bulk-byte-source-transfer.md`, and `lazy-multi-raft-consensus.md`: Documented the standalone `PipelinedTransmitter` two-stage architecture invariants (isolated serialization-stage encoding and transmission-stage bounded gathering socket I/O and trailing streaming, shared `TransmissionProtocol` wire sentinels and completion reports, atomic FIFO queue reservation, strict FIFO completion callback ordering across serialization failures, dual-stage quiescent pruning, and lazy stream transfer buffer release) alongside the preserved legacy `Transmitter` and `SequentialTransmitter` behind-transmission coordination, updated cluster services and consensus bridge callers to `PipelinedTransmitter`, and documented `Receiver` trailing stream fault handling distinguishing clean in-band peer stream aborts (channel stays open, aborted package skipped) from local/framing faults (socket teardown), plus 5-byte (32-bit) chunk header VLQ bounds.

## [2026-11-12T05:13:00Z]

- Updated Section 8 of `doc/architecture/cluster-serialization.md` to align the Concurrent Behind-Transmission Coordination Invariant with semantically honest state terminology (`serializing-and-transmitting`, `transmission-paused-while-serializing`, `serialization-completed-and-transmitting`, `serialization-failed`, and `transmission-failed`).

## [2026-11-11T20:52:00Z]

- Updated Section 8 of `doc/architecture/cluster-serialization.md` to reflect circular-storage exhaustion and lock-free behind-transmission coordination:
  - Revised the Transport Buffer Capacity Ceiling Invariant so `DualEndedCircularStorage` reports exhaustion via an empty `Maybe` without throwing inside the collection, and `Transmitter` handles buffer ceiling exhaustion by failing fast with `BuffersCountExceededException` reported as `TransmissionFailure` (sending the corrupted-package sentinel first if frames are already in flight).
  - Formalized the Concurrent Behind-Transmission Handoff Invariant specifying the lock-free atomic five-state coordination protocol between message serialization and asynchronous NIO2 frame transmission.

## [2026-11-11T14:40:00Z]

- Formalized the Transport Frame Header Contiguity & Reserved Buffer Headroom Invariant in Section 8 of `doc/architecture/cluster-serialization.md`:
  - Synthesized the rationale for holding back transport buffer write limits by the maximum VLQ header size (`buffersCapacity - FRAME_HEADER_MAX_SIZE`), explaining why frame headers and wire sentinels are strictly forbidden from splitting across circular storage buffers.
  - Specified the dynamic limit extension mechanism into reserved headroom on split header reception, guaranteeing single-buffer decoding for the transport framing loop and eliminating multi-buffer reassembly state machines across asynchronous completion dispatches.

## [2026-11-10T20:50:00Z]

- Documented trailing stream leftover pipelining design rationale, alternative evaluation, and channel virtualization invariants in `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md`:
  - Articulated the comparative analysis of four architectural options for handling trailing stream leftover bytes arriving across TCP message boundaries: rejected direct multi-buffer ring slicing due to buffer capacity mismatch and framing state desynchronization, rejected direct slice retention in `streamTransferBuffer` due to buffer reuse collision hazard during consecutive streams, and rejected layered channel decorator wrappers due to allocation overhead and lifecycle decoupling.
  - Documented the selected isolated leftover staging architecture (`streamTransferLeftover` and `readChannel`) in Section 6, specifying Case 1 (staging into isolated heap buffer and resetting circular storage target) versus Case 2 (in-place `target.compact()` for streams contained entirely in initial packet) and explaining the rationale for buffer reuse safety, framing state machine uniformity, zero-allocation fast paths, and idle buffer pruning safety.
  - Formalized the Trailing Stream Leftover Staging and Channel Virtualization Invariant in Section 7.

## [2026-11-10T16:30:00Z]

- Completed Automated Test Coverage Matrix Hardening (Gap D7) and resolved transport framing and circular storage boundary defects:
  - Renamed `StreamingDefectsSuite.scala` to semantically honest `ClusterWireStreamingSuite.scala` in `consensus/nio2-cluster-bridge`, implementing a comprehensive test matrix (TC-01 through TC-15) covering property-based testing with minimal and near-minimal randomized capacities ($C_{buf} \in [10, 128]$ bytes, $C_{stream} \in [16, 256]$ bytes, $chunkSize \in [1, 64]$ bytes).
  - Hardened multi-buffer control package deserialization in `Receiver.deserializePackage`: when a package payload exhausts the current circular storage buffer and the package terminator sentinel resides in the subsequent buffer, `Receiver` catches up `readEndBuffer` to `writeEndBuffer` prior to checking package consumption, eliminating false-positive `TheDeserializerHasNotConsumedTheWholePackage` reception faults.
  - Corrected split frame header limit calculation in `Receiver`: when a frame header spans buffer boundaries (`SPLIT`), the buffer limit is extended to `nextFrameHeaderPos + FRAME_HEADER_MAX_SIZE` using reserved capacity, preventing truncation or clamping of already received bytes.
  - Implemented `streamTransferLeftover` staging in `Receiver`: unconsumed pipelined bytes following a trailing stream in `streamTransferBuffer` are staged into a dedicated leftover buffer and fed into `Receiver`'s framing loop on the subsequent reception cycle via `readChannel`, preventing out-of-order circular storage corruption and socket read hangs on zero-length stream follow-up packages.
  - Formalized the Multi-Buffer Deserializer Boundary Catch-Up Invariant, Split Frame Header Limit Invariant, and Trailing Stream Leftover Staging Invariant in `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md`, and marked Category 5 (Gap D7) as completed in `doc/roadmap/milestones/milestone-a-network-transport-bridge.md`.

## [2026-11-10T15:15:00Z]

- Parameterized streaming transfer buffer capacity across `nexus/cluster` and `consensus/nio2-cluster-bridge`:
  - Promoted hardcoded direct transfer buffer capacity (64 KiB) to configurable constructor parameters across `Transmitter`, `SequentialTransmitter`, `Receiver`, `ParticipantService.DelegateConfig`, and `ClusterBridgeConfig`, with default fallbacks preserving backwards compatibility.
  - Updated `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md` to document the configurable transfer buffer capacity per channel.

## [2026-11-10T12:00:00Z]

- Updated ADR `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md`, `doc/roadmap/index.md`, and `doc/roadmap/milestones/milestone-a-network-transport-bridge.md` to document the planned Consensus Snapshot Concurrency Governor and Elastic Transport Streaming Buffer Pool architecture:
  - Formalized the necessity of decoupling transport buffer allocation from concurrency throttling: transport-level bounded buffer pools cause catastrophic socket deadlocks and TCP ZeroWindow stalls under load, while 8 KiB circular storage chunking increases disk/syscall thrashing eightfold.
  - Documented the two-tier architectural solution: an application-level consensus governor in `consensus/core` and `consensus/nio2-cluster-bridge` enforcing an admission cap ($K$) via protocol-level backpressure, coupled with an elastic direct buffer pool in `nexus/cluster` with deadlock-free on-demand allocation and bounded idle retention ($K$).

## [2026-11-09T23:00:00Z]

- Completed architectural remediation of zero-copy trailing stream payload transport across `common`, `nexus/cluster`, and `consensus/nio2-cluster-bridge`:
  - Refactored `ByteSource.AsyncReader.read` and `ByteSink.write` to return synchronous completion status (`Int` bytes transferred or `PENDING`), eliminating fragile boolean trampolining and preventing stack overflow across thread-local completions.
  - Implemented multi-write slicing loop in `FileByteSink.write` ensuring entire buffer contents are persisted to asynchronous channels before invoking completion handlers.
  - Hardened `FileByteSource` lifecycle with a hybrid model combining explicit `close()` unlinking with a fallback `java.lang.ref.Cleaner` cleanup task to prevent orphaned temporary files on discarded streams.
  - Resolved circular storage buffer slicing in `Receiver.finishTrailingStream` across `DualEndedCircularStorage` buffer boundaries, preventing buffer overflow on large subsequent message packages.
  - Bounded VLQ chunk continuation decoding to 5 bytes (< 28 bits) and clamped chunk sizes to 64 KiB, immediately tearing down channel sockets upon mid-stream protocol violations to prevent socket starvation and frame desynchronization.
  - Preserved `frameBuffer(1)` non-null state in `Transmitter` across trailing streaming to protect single-flight invariants against concurrent callers.
  - Established regression test suite `readren.consensus.nio2.StreamingDefectsSuite` covering all reproduction scenarios and updated ADR `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md`.

## [2026-11-09T05:05:00Z]

- Established the dedicated architectural concept specification for transport-agnostic byte streaming primitives in `doc/architecture/byte-streaming-primitives.md`:
  - Formalized the capability-first architecture of `ByteSource` and `ByteSink` in `readren.common`, defining the Bounded Extent & Exact Size Invariant, Multi-Session Repeatable Extraction Invariant, Dual Modality (Push vs Pull) Invariant, Zero-Copy Bulk Transfer Invariant, and Deterministic Lifecycle Invariant.
  - Specified the Single-Flight Invariant and Cross-Thread Serialized Continuity Invariant for `ByteSource.AsyncReader`, detailing how Java NIO.2 completion hand-offs establish JMM happens-before ordering without atomic compound operations.
  - Formalized the Staging Pre-Allocation & Declared Size Invariant, Early Handle Projection Invariant, Incremental Non-Blocking Ingestion Invariant, and Strict Terminal State Machine Invariant (`seal` vs `discard`) for `ByteSink` and `ByteSink.Allocator`.
  - Split `ByteSource.scala` into distinct `ByteSource.scala` and `ByteSink.scala` source files (and split `ByteSourceTest.scala` into `ByteSourceTest.scala` and `ByteSinkTest.scala`), removing legacy forwarder type aliases from `object ByteSource`.
  - Rewrote Scaladoc across `common/src/main/scala/ByteSource.scala` and `common/src/main/scala/ByteSink.scala` to formulate contracts around intrinsic capabilities and operational guarantees rather than downstream consumers.
  - Optimized channel allocation overhead in `nexus/cluster.Transmitter` and `Receiver` for out-of-line streaming: hoisted `pendingByteSources` and `pendingByteSinks` queues to reusable instance fields reset per message, made `Transmitter.byteSourceReadHandler` and `Transmitter.streamHandler` lazy vals to eliminate handler allocations for non-streaming messages, and replaced temporary 5-byte heap buffer and lambda allocations in `Receiver` with primitive bitwise VLQ chunk header decoding across buffer boundaries.

## [2026-11-09T03:30:00Z]

- Formalized the transition from generic `Payload` naming to semantically precise `ByteSource` and `ByteSink` contracts in `readren.common`:
  - Retained `ByteSource` and `ByteSink` within `readren.common` to preserve foundational architectural layering, guaranteeing that consensus state machine logic and persistence SPI contracts remain decoupled from cluster transport and networking abstractions.
  - Renamed ADR `doc/architecture/adr-trailing-stream-bulk-payload-transfer.md` to `doc/architecture/adr-trailing-stream-bulk-byte-source-transfer.md` and updated `doc/architecture/index.md` accordingly.
  - Updated Section 8 of `doc/architecture/cluster-serialization.md` and `doc/architecture/cluster-serialization-guide.md` to establish the Generic Out-of-Line ByteSource Serialization & Deferred Queueing Invariant and `ByteSink.Allocator` staging contracts.
  - Updated Sections 8.I, 8.III, and 8.IV of `doc/architecture/lazy-multi-raft-consensus.md` to specify `SnapshotByteSource`, `ByteSource`, `BufferByteSource`, and `FileByteSource` lifecycle ownership and zero-copy transfer semantics.
  - Updated `doc/roadmap/index.md` and `doc/roadmap/milestones/milestone-a-network-transport-bridge.md` to align milestone deliverables with `ByteSource` terminology.

## [2026-11-09T03:00:00Z]

- Synchronized the architectural OKF bundle with Generic Out-of-Line Payload Serialization (Model A — Eager Pre-Delivery):
  - Updated `doc/architecture/cluster-serialization.md` Section 8 to formalize the Generic Out-of-Line Payload Serialization & Deferred Queueing Invariant, Trailing Stream Chunk Framing & Sentinel Invariant, Eager Pre-Delivery Streaming & Bounded Staging Invariant, Asynchronous Completion Loop Trampolining Invariant, and Deferred Sink Sealing Invariant, eliminating obsolete heap-copying direct-buffer invariants.
  - Updated `doc/architecture/lazy-multi-raft-consensus.md` Sections 8.I and 8.III to formalize the Physical Transport Stream Snapshot Reception Invariant, Streaming Snapshot Carrier & Repeatable Async Reader Invariant, Zero-Copy Transport and Non-Blocking Async Invariant, and Snapshot Storage Ownership & Rejection Cleanup Invariant across `readren.common.Payload`.
  - Authored Section 8 in `doc/architecture/cluster-serialization-guide.md` providing developer guidance on macro derivation for product types containing `Payload` fields, deferred queueing (`putPayload` and `readPayload`), and staging thresholds via `Payload.Allocator`.
  - Updated `doc/roadmap/milestones/milestone-a-network-transport-bridge.md` and `doc/roadmap/index.md` to resolve the Physical Transport Streaming Gap and mark Milestone M-A as completed at 100%.

## [2026-11-09T02:50:00Z]

- Updated ADR `doc/architecture/adr-trailing-stream-bulk-payload-transfer.md` to define transport control sentinels (`CORRUPTED_PACKAGE_SENTINEL = 0xffff_fffeL`, `STREAM_FOLLOWS_SENTINEL = 0xffff_fffdL`) as positive unsigned 64-bit `Long` constants and formalize the Unsigned Wire Sentinel Decoding Invariant, replacing the previous signed two's complement representation to ensure consistent equality semantics with `VLQ.tryToDecodeUnsignedInt`.

## [2026-11-08T18:00:00Z]

- Formally accepted ADR `doc/architecture/adr-trailing-stream-bulk-payload-transfer.md` and updated `doc/architecture/index.md` to establish the generic out-of-line `Payload` serialization architecture: integrated polymorphic non-blocking asynchronous I/O abstractions (`Payload.AsyncReader`, `Payload.AsyncSink`, `Payload.Allocator`, `DefaultAllocator`), iterative completion loop trampolining in `Transmitter` and `Receiver` to eliminate call-stack recursion exhaustion, deferred sink sealing preventing premature channel closure, `DualEndedCircularStorage` buffer compaction on single-buffer stream completion, and aligned negative two's complement sentinel decoding in `Receiver` matching `VLQ.tryToDecodeUnsignedInt`.

## [2026-11-08T16:25:00Z]

- Updated proposed ADR `doc/architecture/adr-trailing-stream-bulk-payload-transfer.md` and `doc/architecture/index.md` to specify Generic Out-of-Line `Payload` Serialization (Model A): a foundational `Payload` trait in `common`, deferred out-of-line payload queueing via `Serializer.Writer.putPayload` and `Deserializer.Reader.readPayload`, concatenated chunk-framed trailing streams following `STREAM_FOLLOWS_SENTINEL`, eager pre-delivery streaming into staging files in `Receiver`, and explicit `SnapshotPayload` lifecycle ownership across `Workspace` replacement and `onInstallSnapshot` rejection.

## [2026-11-07T22:10:00Z]

- Refactored stream deserialization and reception mechanics across `nexus/cluster` and downstream test suites, eliminating `continuousBuffer`, `getContentBytes(maxBytesToConsume)`, and `CONSECUTIVE_CONTENT_BYTES_REQUIRED_BY_MOST_DEMANDING_OPERATION`: made primitive decoding methods on `Deserializer.Reader` abstract (`readByte`, `peekByte`) or default big-endian bitwise assemblies (`readShort`, `readInt`, `readLong`, `readFloat`, `readDouble`, `readBytes`), implemented zero-copy single-instruction fast-path contiguous reads in `Receiver` with seamless fallback to byte-by-byte boundary assembly, and formalized the Stream Deserialization Primitive Fast-Path & Split Boundary Invariant in Section 8 of `doc/architecture/cluster-serialization.md`.

## [2026-11-07T19:55:00Z]

- Implemented dynamic protocol version negotiation in `consensus/nio2-cluster-bridge`, aligning transport handshake mechanics with `nexus/cluster`'s `ParticipantService`: upgraded `PeerHandshake` and `ClientHandshake` to advertise supported version sets (`Set[ProtocolVersion]`), implemented highest mutually supported version selection via `ProtocolVersion.newerFirstOrdering`, threaded negotiated `agreedVersion` through active `PeerConnection` and `ClientConnection` transmission and reception loops, and formalized the Dynamic Protocol Version Negotiation & Handshake Evolution Invariant in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`.

## [2026-11-07T18:05:00Z]

- Resolved 8 active TODO items in `consensus/nio2-cluster-bridge`: authored complete Scaladoc on `ClusterBridgeConfig` adhering to repository standards, replaced monolithic `rpcTimeout` with differentiated `fastRpcTimeout`, `snapshotRpcTimeout`, and `clientRpcTimeout`, renamed `ClusterWireFrame` to `ClusterWireMessage` to semantically separate logical packages from physical transport buffer frames, eliminated per-RPC heap allocation by having `PendingRpc` extend `sequencer.Captor`, replaced eager $O(N)$ full-map scanning on every outbound RPC with $O(1)$ collision checks across 16-bit circular correlation sequence reuse, and renamed `SocketClusterBridgeCdm` to `Nio2ClusterBridgeCdm` across the codebase.

## [2026-11-07T17:06:00Z]

- Formalized the Heartbeat-Free Application-Level Snapshot Atomicity & Transport Chunking Invariant in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`, establishing that because Lazy Raft eliminates background heartbeats and follower election timers, snapshot transfers cannot cause election churn, allowing the consensus engine to maintain an atomic logical `InstallSnapshot` contract while delegating frame slicing, payload streaming, and byte reassembly entirely to the cluster transport layer (`nexus/cluster`).
- Updated `doc/roadmap/milestones/milestone-a-network-transport-bridge.md` and `doc/roadmap/index.md` to record the delegation of snapshot chunking to transport and schedule transport-level payload streaming and receiver de-accumulation for arbitrary snapshot sizes.

## [2026-11-06T23:45:00Z]

- Formalized loopback TCP multi-node test harness invariants, dynamic ephemeral port registration, outbound transmitter pipeline queue confinement, and streaming snapshot reception in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`, verified complete end-to-end consensus operation across 5 integration scenarios over real Java NIO2 TCP sockets in `consensus/nio2-cluster-bridge` (discovery and election, linearizable command replication and follower redirection, log compaction and streaming `InstallSnapshot`, dynamic electorate change and node quiescence, node disconnect and reconnect recovery), resolved a re-entrant queue deadlock in `SequentialTransmitter`, and marked Milestone A (Physical Network Transport Bridge) as completed in `doc/roadmap/milestones/milestone-a-network-transport-bridge.md` and `doc/roadmap/index.md`.

## [2026-11-06T21:30:00Z]

- Formalized the Component Definition Module (CDM) decomposition and service aggregation invariants in Section 8.IV of `doc/architecture/lazy-multi-raft-consensus.md`, establishing a directed acyclic graph of component definition slices rooted in `ConsensusWireTypesCdm` (pure domain types, `ClusterView`, and `ResponseToClient`), extracting `ConsensusClusterBridgeCdm` for the cluster transport and membership SPI, decoupling `ConsensusElectorateCdm` and `ConsensusPrimaryStateCdm` as independent peer modules, structuring `ConsensusParticipantSdm` as the downstream aggregating service module, and migrating `consensus/nio2-cluster-bridge` to implement `SocketClusterBridgeCdm` directly without exposure to consensus election or storage internals.

## [2026-11-06T18:55:00Z]

- Implemented Iteration 4 of Milestone A in `consensus/nio2-cluster-bridge`, delivering `ClusterBridgeSdm` (`SocketClusterBridge`) and `ClusterClient` over Java NIO2 and `nexus/cluster` framing abstractions, connecting inbound/outbound consensus RPCs (`HowAreYou`, `RequestBallot`, `AppendRecords`, `InstallSnapshot`, `PermitQuiescence`) strictly within `sequencer: SchedulingDoer`, managing circular 16-bit correlation ID tracking and timeout schedules, resolving simultaneous mutual connection opening via deterministic total-order tie-breaking, implementing client endpoints (`SubmitClientCommand`, `SubmitElectorateChange`), and verifying 100% test pass rate across handshake, wake-up scheduling, correlation tracking, and multi-node integration test suites.

## [2026-11-06T18:25:00Z]

- Formalized the socket lifecycle, handshake incubation, and receiver continuity invariants in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`, establishing that `Receiver` instances created upon TCP socket acceptance in `consensus/nio2-cluster-bridge` must be preserved across the entire lifetime of the channel and propagated directly to active connection delegates (`PeerConnection` or `ClientConnection`) to prevent dropping bytes buffered in `DualEndedCircularStorage` during the initial handshake read.

## [2026-11-06T14:55:00Z]

- Formalized the physical wire protocol framing, correlation sequence arithmetic, and discriminator band allocation invariants in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`, establishing the `ClusterWireFrame` sealed sum hierarchy in `consensus/nio2-cluster-bridge`, specifying 16-bit signed circular wrap-around awaiter capacity eviction (`CorrelationId.isLapsedRelativeTo`), mapping 18 wire frame variants to discrete numerical bands under `FLAT` match mode, and updating Milestone M-A progress in `doc/roadmap/milestones/milestone-a-network-transport-bridge.md`.

## [2026-11-06T03:02:00Z]

- Formalized the pure wire protocol sum type invariant for replication responses in Section 8.I of `doc/architecture/lazy-multi-raft-consensus.md`, establishing that `AppendResult` represents solely peer protocol responses (`AppendResult_Accepted` and `AppendResult_Rejected`), eliminating synthetic transport failure representations (`AppendResult_Failed`) from domain models and serialization hierarchies, and handling asynchronous RPC transport errors strictly at the transport boundary via `Maybe[AppendResult]`.

## [2026-11-05T21:50:00Z]

- Formalized the aggregate transport capacity invariant in Section 11 of `doc/architecture/cluster-serialization.md`, enforcing at construction time that `maxBuffers * buffersCapacity <= Int.MaxValue` in `Receiver` and `ParticipantService.DelegateConfig`, validating incoming frame lengths against `totalBufferCapacity` in `Receiver.completed` to reject corrupted frames and prevent negative indexing, and standardizing frame accumulators, package bounds, and `Reader.remainingContentBytes` on 32-bit `Int`.

## [2026-11-05T20:55:00Z]

- Refined the pre-allocation remaining content bound invariant in Section 11 of `doc/architecture/cluster-serialization.md`, specifying that total package content bytes are accumulated from decoded frame headers during packet ingestion in `Receiver.completed` rather than dynamically traversing circular storage buffer distances, providing an exact $O(1)$ content ceiling to the reader without iterator allocations or hot-path traversal overhead.

## [2026-11-05T18:30:00Z]

- Formalized adversarial payload defense and allocation guard invariants in Section 11 of `doc/architecture/cluster-serialization.md`, specifying pre-allocation validation of VLQ length headers against physical transport buffer distance, defensive allocation ceilings (`AllocationLimits`) bounding element and byte allocations across collections and zero-wire-footprint types, structured non-fatal deserialization fault isolation (`PayloadCorruptedException`) preventing `OutOfMemoryError` denial-of-service conditions in transport reception, and synthesized exhaustive discriminator fallback branches in macro-derived sum deserializers.

## [2026-11-05T16:35:00Z]

- Formalized the ghost leader ballot rejection invariant in Section 9 of `doc/architecture/lazy-multi-raft-consensus.md`, specifying that a ghost leader driving quiescence authorization under an empty electorate must reject ballot requests immediately with a blank ballot rather than re-entrantly delegating to itself, ensuring rapid election rejection without recursive loop hazards when no valid voting electorate exists.

## [2026-11-05T15:22:00Z]

- Formalized the dynamic role dispatch invariant for command application completion in Section 8.III of `doc/architecture/lazy-multi-raft-consensus.md`, specifying that state-machine application callbacks must dispatch dynamically to the participant's active role rather than the historical role instance that scheduled the applier, guaranteeing that incoming leaders receive state-machine completion events to fulfill in-flight client response covenants across role transitions without head-of-line blocking or stalled queues.

## [2026-11-04T22:10:00Z]

- Formalized AnyVal value class and Scala 3 opaque type alias serialization invariants in Section 2 and Section 5 of `doc/architecture/cluster-serialization.md`, specifying top-level derivation for opaque types via translucent underlying type resolution, strict domain encapsulation enforcement preventing silent alias piercing during product field traversal, explicit-over-structural precedence for AnyVal codecs, zero-allocation field unboxing fallback directly against underlying representations, and value class self-recursion short-circuiting.

## [2026-11-04T19:45:00Z]

- Formalized the single-evaluation macro invariant in `doc/architecture/common-primitives.md`, specifying that expression parameters in synthesized quote ASTs across `MaybeMacros` (`some`, `map`, `flatMap`, `liftPartialFunction`) must bind to intermediate local references, and restricting `ToolsMacro` (`deriveToString`) strictly to the enclosing class instance (`this` or an enclosing self-type alias) to prevent duplicated execution and preserve private constructor parameter access.

## [2026-11-04T19:00:00Z]

- Formalized Scala 3 tuple serialization and composite resolution invariants in Section 10 of `doc/architecture/cluster-serialization.md`, specifying inductive structural product decomposition over `EmptyTuple` and `*:[H, T]`, zero-discriminator product wire parity matching case class field layout, strict left-to-right deserialization evaluation order avoiding right-associative stream inversion hazards, and transparent implicit propagation into macro-derived product types without nominal wrappers.

## [2026-11-03T21:55:00Z]

- Formalized object reference deduplication table lifecycle and zero-allocation invariants in Section 9 of `doc/architecture/cluster-serialization.md`, specifying connection-bound lifecycles for `RecyclableIdentityIntMap` and `RecyclableObjectBuffer` on `Transmitter` and `Receiver`, open-addressing linear probing with primitive integer value arrays and inlined lookups to eliminate boxing and closure churn, composite multi-frame reference accumulation with in-place $O(\text{size})$ reset, and thread-confined lock-free execution across sequential transport pipelines.

## [2026-11-03T21:10:00Z]

- Formalized object reference deduplication framing, indexing, and value-type bypass invariants in `doc/architecture/cluster-serialization.md`, specifying signed VLQ wire encoding (`putIntVlq` / `readIntVlq`) to prevent wire collisions between reference identifiers and the `0x40` escape header, establishing 0-based reference table indexing across serialization writers and deserialization readers, enforcing strict value-type bypass via `ValueOrReferenceTest` to prevent primitive boxing and index desynchronization, and restoring symmetric reference deduplication across transport and collection serializers.

## [2026-11-03T16:30:00Z]

- Formalized the streaming snapshot protocol and carrier invariants in Section 8.III of `doc/architecture/lazy-multi-raft-consensus.md`, specifying the abstract `SnapshotPayload` repeatable channel supplier contract in `consensus/core`, extracting `FileSnapshotPayload` to the `consensus_nio2-persistence` subproject, isolating in-memory `BufferSnapshotPayload` within the simulation test suite, and establishing `Workspace` ownership of snapshot file persistence lifecycles.

## [2026-11-02T21:25:00Z]

- Formalized transport buffer capacity ceiling and quiescent pruning invariants in `doc/architecture/cluster-serialization.md`, specifying hard buffer limits (`maxBuffers`) with fail-fast backpressure and in-flight corrupted-package sentinel aborts without connection teardown, alongside hysteresis-based ring pruning (`pruneTo`) down to minimal buffer floors when channels transition to idle.

## [2026-11-02T19:20:00Z]

- Formalized binary payload transfer and buffer lifecycle invariants in `doc/architecture/cluster-serialization.md`, specifying chunked boundary-aware bulk transfer across transport frames, direct slice and `ByteBuffer` streaming without intermediate heap allocations, and buffer lifecycle isolation requiring durable payloads (such as snapshot data) to be copied into immutable heap structures (`IArray[Byte]`) rather than retaining zero-copy references to ephemeral pooled direct buffers.

## [2026-11-02T17:53:00Z]

- Formalized protocol versioning and product schema evolution in `doc/architecture/cluster-serialization.md`, specifying monotonic integer `ProtocolVersion` semantics (`opaque type ProtocolVersion <: AnyVal = Int`), `@since(version: Int)` version attribution, compile-time companion default value enforcement, dynamic wire adaptation via writer/reader governing versions, and preservation of invariant type class safety.

## [2026-11-02T15:05:00Z]

- Formalized the interaction between `DiscriminationCriteria` and `NestedSumMatchMode` in `doc/architecture/cluster-serialization.md` and `doc/architecture/cluster-serialization-guide.md`, specifying discriminator requirements across leaf variants versus intermediate sum traits in `FLAT` and `TREE` modes, unified versus split match-type criteria, and mode decoupling via explicit intermediate serializer givens.

## [2026-11-02T13:45:00Z]

- Refactored `doc/architecture/cluster-serialization.md` to purge compiler AST implementation details in accordance with OKF invariant constraints, created `doc/architecture/cluster-serialization-guide.md` illustrating synthesized code expansions across singletons, products, self-recursion, flat/tree nesting modes, and custom `DiscriminationCriteria`, and indexed the developer guide in `doc/architecture/index.md`.

## [2026-11-02T12:41:00Z]

- Created `doc/architecture/cluster-serialization.md` and updated `doc/architecture/index.md` to formalize the cluster binary serialization and macro derivation architecture, specifying direct Tasty reflection type classification (`Singleton`, `Sum`, `Product`) without `scala.deriving.Mirror`, parameterized product and sum child-variant type unification, non-ADT concrete class support, diamond-inheritance deduplication in `FLAT` mode, explicit field type givens with allocation-free direct and indirect self-recursion, and match-type `DiscriminationCriteria[-S]` reduction with static subtyping fallback for intermediate sum types.

## [2026-10-31T20:00:00Z]

- Renamed the consensus host transport SPI contract from `ClusterParticipant` to `ClusterBridge` (and its simulation harness counterpart to `TestClusterBridge`) in `doc/architecture/lazy-multi-raft-consensus.md` and
  `doc/architecture/consensus-test-framework.md`, and restructured the `consensus` module into `consensus/core` (`consensus_core`) and `consensus/nio2-cluster-bridge` (`consensus_nio2-cluster-bridge`).

## [2026-10-31T15:45:00Z]

- Relocated the canonical Open Knowledge Format (OKF) bundle from repository root `okf/` to `doc/architecture/` as part of repository-wide documentation consolidation, and updated path constraints in `AGENTS.md` and
  `.agents/skills/okf_maintenance/SKILL.md`.

## [2026-10-30T19:10:00Z]

- Formalized the Centralized Leader Command Application and Follower Parity architecture in `okf/adr-causal-fence-state-synchronization.md`, documenting the single sequential application loop, allocation-free FIFO response buffer
  (`ArrayDeque`), and decoupled abdication safety under client pipelining while eliminating per-command promise allocations.

## [2026-10-30T00:00:00Z]

- Formalized the Allocation-Free, Re-entrancy-Safe FIFO Awaiter Dispatch specification in `okf/adr-causal-fence-state-synchronization.md`, documenting the contiguous prefix fulfillment invariant, atomic head dequeue prior to notification,
  re-entrancy safety against synchronous appends and leadership clears, and stack purity under the Decoupled Mutation Contract while removing the obsolete defective implementation notice.
- Documented strictly monotonic state machine command application tracking and participant duplicate rejection in `okf/consensus-test-framework.md` and verification oracle specifications.
- Documented concurrent client command pipelining ($W > 1$) and multi-client contention pool architecture in the consensus discrete-event simulation harness.

## [2026-10-29T21:25:00Z]

- Conceptualized consensus commit advancement and awaiter fulfillment in `okf/lazy-multi-raft-consensus.md` and `okf/adr-causal-fence-state-synchronization.md`, replacing specific code method names (`recalculateCommitIndex`) and obsolete
  types (`CommitIndexAwaiter`) with timeless architectural descriptions, and removed internal variable references.
- Added a defective implementation notice to `okf/adr-causal-fence-state-synchronization.md` marking the In-Place Partition & Reverse-Execution pattern as violating FIFO subscription order when partial commits leave multiple unfulfilled
  awaiters behind.
- Updated `okf/sequencer-core.md` to establish `Mono` as the base single-value lazy computation trait (purging obsolete `Observable (future Mono)` references) and removed machine-specific absolute Windows paths.
- Cleaned up `okf/adr-spare-slot-pattern-evaluation.md` and `okf/adr-scheduling-subscription-design.md`, removing dead sandbox links and replacing absolute file paths with relative links and conceptual descriptions.

## [2026-10-29T20:34:00Z]

- Created `okf/coding-guidelines.md` establishing standardized repository conventions for Scala 3 explicit block bracing, continuous prose wrapping, semantic naming patterns (boundary `on...` versus internal `handle...`), and Scaladoc
  continuation formatting, resolving the broken index link in `okf/index.md`.
- Enhanced OKF cross-document connectivity by adding outbound cross-references across isolated ADRs and core component specifications (`okf/adr-causal-fence-state-synchronization.md`, `okf/adr-component-definition-module-pattern.md`,
  `okf/transcript-lazy-multi-raft-design.md`, `okf/sequencer-core.md`, and `okf/consensus-test-framework.md`).

## [2026-10-29T20:28:00Z]

- Enhanced cross-document traceability between `okf/system-architecture.md` and `okf/lazy-multi-raft-consensus.md` by embedding deep section anchor links connecting high-level architectural mechanisms (Adaptive Pipelining, fault detection,
  electorate transitions, Storage SPI boundary, and client deduplication) to their formal protocol specifications, and linked `CausalFence` synchronization to `okf/adr-causal-fence-state-synchronization.md`.

## [2026-10-29T20:20:00Z]

- Refined the Adaptive Pipelining ("First-Stitch" Recovery) specification in Section 2.I of `okf/system-architecture.md` to clarify the operational sequence: pausing client commands in memory on dormant shards, triggering reactive leader
  election, and executing the paused command in the newly inaugurated leader's initial replication wave.

## [2026-10-29T20:02:00Z]

- Expanded Section 8 of `okf/lazy-multi-raft-consensus.md` to specify the complete triad of host integration SPI contracts (`ClusterParticipant` transport bridge, `Workspace` storage SPI, and `StateMachine` application SPI), formalizing the
  unopinionated sequential storage boundary against the consensus invariant engine and detailing the delegation of client command deduplication to the application state machine across log compaction snapshot boundaries.

## [2026-10-29T19:37:00Z]

- Restructured the OKF core architecture documentation by decoupling the macro-framework design from the micro-consensus protocol engine: created `okf/system-architecture.md` to document the distributed Multi-Raft application framework
  (Gunther's USL scaling model, Control Plane vs. Data Plane separation, shard isolation, zero-RTT routing, and co-located persistence IoC), refocused `okf/lazy-multi-raft-consensus.md` strictly as the
  `Lazy Raft Consensus Protocol Specification` for the single-group consensus state machine (`ConsensusParticipantSdm`), purged obsolete implementation-level class references (`Accessible`) in favor of timeless Storage SPI (`Workspace`) and
  consensus invariant boundaries, and updated `okf/index.md`.

## [2026-10-28T01:40:00Z]

- Formalized the Graceful Teardown Fault Injection & Two-Tier Disarm Safeguard in `okf/consensus-test-framework.md` (Section 8), specifying that network partitions remain active during empty electorate post-commit retirement until internal
  consensus protocol retries reach half of the leader's retry quota, ensuring exhaustive fault testing of the two-phase retirement protocol while preventing false-positive peer abandonment.

## [2026-10-27T23:35:00Z]

- Added dedicated Section 5 (Adversarial Channel Selection & Traffic Shaping Architecture) to `okf/consensus-test-framework.md`, formalizing the multiplicative hybrid model (shifted exponential epoch drift, continuous log-uniform node
  affinity multipliers, and semantic head-packet weighting), non-starvation invariants, and substrate decoupling.

## [2026-10-27T23:30:00Z]

- Formalized the `AdversarialPacketPicker` component in `okf/consensus-test-framework.md`, documenting the multiplicative hybrid model combining continuous log-uniform node affinity drift across shifted exponential epochs with semantic RPC
  head-packet weighting for adversarial discrete-event simulation while preserving point-to-point FIFO delivery.

## [2026-10-27T21:10:00Z]

- Aligned Section 4 of `okf/consensus-test-framework.md` to formalize the Two-Tier Discrete Step Dispatcher (Hierarchical Process Stepper), eliminating references to "scheduler" to cleanly distinguish immediate discrete simulation step
  dispatching (compute vs. network domain arbitration) from future virtual timer scheduling (`VirtualClock`).

## [2026-10-22T20:50:00Z]

- Formalized the Ghost Leader Term Promotion & Non-Abdication Invariant Under Vanishing Electorates in `okf/lazy-multi-raft-consensus.md`, establishing that a ghost leader finalizing a zero-voting-member electorate change
  ($C_{\text{new}} = \emptyset$) must ignore higher terms observed in outbound replication responses (`AppendResult_Rejected`) or uncoordinated discovery probes (`HowAreYou`), promote its own tenure term to match the highest observed peer
  term ($T_{\text{leader}} \leftarrow T_{\text{peer}}$) without stepping down or triggering an election, and only abdicate upon receiving an authoritative inbound append from a higher-term leader.

## [2026-10-22T19:00:00Z]

- Created `okf/consensus-test-framework.md` and updated `okf/index.md`, establishing the unified architectural specification for the consensus simulation test framework, including the three-tier boundary (physical substrate, administrative
  control plane, quarantined invariant oracle), subsystem modularization (`readren.consensus.env`), two-tier hierarchical process scheduling model, decoupled local virtual clock timer mechanics, deterministic operation journal replay, and
  continuous per-step safety verification model.

## [2026-10-20T20:25:00Z]

- Refined the Synchronous Cluster-Tier Probable-Participant Memorization Contract (`ClusterParticipant.getOtherProbableParticipants`) in Section 13 of `okf/lazy-multi-raft-consensus.md`, formalizing that the host cluster tier maintains its
  sequencer-confined memorized peer view via local administrative update handlers and outbound consensus electorate notifications (`onActiveElectorateChanged`) without omniscient cross-node consensus state inspection.

## [2026-10-19T19:53:00Z]

- Updated Section 12 of `okf/lazy-multi-raft-consensus.md` to formalize the Deterministic Journal Replay & Reset Completeness Invariant across the modularized discrete-event simulation subsystems.

## [2026-10-19T19:15:00Z]

- Updated Section 10 and Section 14 of `okf/lazy-multi-raft-consensus.md` to formalize probability-ordered `ListSet` candidate routing hints across all non-terminal `ElectorateChangeResponse` (`suggestedParticipants: ListSet`) and
  `ResponseToClient` (`RedirectTo` and `Unable` carrying `otherParticipants: ListSet`) variants, unifying leader redirection (`AskTheLeader` and `RedirectTo`) with ordered fallback candidates and specifying head-candidate priority
  heuristics across follower, isolated, retiring, leader, and joint consensus states.

## [2026-10-19T16:15:00Z]

- Updated Section 10 and Section 14 of `okf/lazy-multi-raft-consensus.md` to purge vestigial node-level administrative resurrection target specifications (superseded by terminal quiescence irreversibility) and formalize external
  control-plane tracking of the presumed active electorate under epistemic uncertainty.

## [2026-10-19T14:51:00Z]

- Updated Section 10 and Section 12 of `okf/lazy-multi-raft-consensus.md` to formalize coordinator in-flight synchronization when receiving `AnotherChangeInProgress` matching the coordinator's desired target configuration, and to specify
  the three-tier architectural boundary separating physical environment controls, external client/control-plane gateways, and quarantined omniscient invariant oracles.

## [2026-10-16T17:21:00Z]

- Documented the Rejection of Concurrent Configuration Changes and Administrative Target Preservation during Configuration Inactivity invariants in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that the leader rejects
  concurrent configuration proposals with `AnotherChangeInProgress` containing active request identification and routing hints rather than queueing proposals to prevent stale execution against quiesced nodes, and establishing that
  administrative coordinators must maintain active configuration participants as administrative targets during idle periods to prevent cluster starvation.

## [2026-10-14T21:10:00Z]

- Documented the Retiring Participant Re-inclusion Invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that retiring participants evaluate re-inclusion against all subsequent configuration changes (whether
  transitional joint configurations or stable configurations delivered via log records or state machine snapshots) whose index exceeds the exclusion index, transitioning immediately to `STARTING` to resume active cluster participation.

## [2026-10-14T17:10:00Z]

- Documented the Transitional Joint Consensus Quorum-Buffering for Re-included Retirees invariant and Stateful Role Immediate Retirement on Exclusion Invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that retiring
  participants belonging to the target configuration ($C_{\text{new}} \setminus C_{\text{old}}$) buffer $C_{\text{new}}$'s quorum threshold alongside joining participants, preserving quorum evaluation liveness for $C_{\text{new}}$ during
  re-admission of previously excluded nodes, and establishing that stateful participants whose commit watermarks advance past an excluding configuration change must immediately transition to `RETIRING` to truthfully report retiree rank
  across all subsequent protocol interactions.

## [2026-10-13T17:57:00Z]

- Documented the Leader Redirection Advisory Invariant (`AskTheLeader` Non-Membership) and Terminal Quiescence Irreversibility Invariant in Section 14 of `okf/lazy-multi-raft-consensus.md`, formalizing that `AskTheLeader` redirections
  convey advisory peer-routing hints rather than active electorate membership, preventing contamination of administrative target pools with excluded ghost leaders, and establishing that host supervisors must enforce terminal finality upon
  quiescence to uphold the one-time quiescence authorization contract.

## [2026-10-12T13:28:00Z]

- Documented the Obsolete Leader Immediate Abdication and Electorate Isolation Invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that a leader receiving higher-term replication payloads must immediately abdicate
  prior to commit watermark advancement and configuration derivation to dismantle leadership replication pipelines before log truncation, preserve truthful lifecycle notification ordering, and isolate electorate derivation from obsolete
  leader state.

## [2026-10-09T22:00:00Z]

- Documented Strongly-Typed Participant Identifiers and Candidate Routing Hints invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that electorate change responses are parameterized by the participant identifier type
  (`ElectorateChangeResponse[+P]`) to provide compile-time type safety for leader redirections (`AskTheLeader(leaderId: P)`), that candidate routing suggestions (`suggestedPeers: Set[? <: P]`) are derived strictly from authoritative
  consensus state (via `deriveElectorateFrom`, followed leader references, or committed excluding membership in `Excluded`) on an advisory, best-effort basis, that callers must fall back to broader discovery if suggested peers fail
  (especially in partitioned states like `Secluded`), that roles lacking consensus knowledge return an empty set to signal lack of better knowledge and eliminate circular echoing of host transport discovery, and that terminal outcomes
  (`SuccessfullyChanged` and `AlreadyChanged`) remain static allocation-free singletons extending `ElectorateChangeResponse[Nothing]`.

## [2026-10-06T14:48:00Z]

- Documented the Joint Consensus Retirement Recognition for Excluded Followers invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that the leader must recognize `RETIRING` role responses from old-only peers excluded
  during transitional joint consensus as terminal retirement synchronization (`AO_IS_RETIRING`), decommissioning further append dispatches and evaluating commit advancement via `respondedAsRetiring` to finalize the stable configuration
  without triggering infinite replication loops against retired nodes.

## [2026-10-05T22:20:00Z]

- Documented the Client Eviction on Excluded Response and Active-Pool Target Prioritization during Administrative Coordination invariants in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that client-side coordination must
  immediately evict participants returning `Excluded` from active target pools and prioritize known active voting members over retiring or historical nodes during candidate selection and round-robin target rotation.

## [2026-10-05T20:38:00Z]

- Documented the Quiescence Permission Deduplication Across Successive Transitions invariant in Section 7.I of `okf/lazy-multi-raft-consensus.md`, formalizing that participants that have acknowledged quiescence authorization from the active
  leader are omitted from subsequent quiescence authorization broadcasts within that leadership tenure to eliminate redundant RPC retry loops targeting already-quiesced nodes, and establishing that re-inclusion in a succeeding active
  electorate clears acknowledged quiescence state.

## [2026-10-02T19:26:00Z]

- Updated Section 6.III (Quiescence Protocol & Convergence Preconditions) and Section 8.I (Ghost Leader Electorate Transition Invariant) in `okf/lazy-multi-raft-consensus.md`, formalizing that ghost leaders remain in office until all
  outbound quiescence permission authorizations are acknowledged or retries are exhausted before abdicating to `Retiring`, thereby enabling unconditional lifecycle cleanup in `Leader.handleExit` while preserving non-blocking election
  progress in surviving configurations.

## [2026-10-01T14:25:00Z]

- Updated Section 6.III (Quiescence Protocol & Convergence Preconditions) and Section 7.I (Retiree Replication Invariant Under Quorum Authority) in `okf/lazy-multi-raft-consensus.md`, formalizing that retiring participants decommissioned
  due to unreachable retry exhaustion during terminal empty-electorate cluster shutdown are omitted from quiescence authorization broadcasts, preventing futile authorization delivery to abandoned participants whose commit watermarks were
  never advanced to the excluding configuration change.

## [2026-09-30T20:53:00Z]

- Documented the Retiree Replication Invariant Under Quorum Authority in Section 7.I of `okf/lazy-multi-raft-consensus.md`, establishing that active leaders and ghost leaders governing a non-empty active electorate must persistently
  replicate log records to excluded participants without abandonment, strictly confining retry-exhaustion decommissioning to terminal empty-electorate cluster shutdown.
- Documented the Independent Exclusion Commitment Invariant for Retiring Participants in Section 7.I of `okf/lazy-multi-raft-consensus.md`, formalizing that ghost leader step-down evaluations verify retiree commitment against each
  participant's individual excluding configuration change index rather than the ghost leader's own exclusion index.

## [2026-09-30T18:25:00Z]

- Updated Section 6 (Participant Role Lifecycle, Unanchored Joining & Electorate Transition Overview) of `okf/lazy-multi-raft-consensus.md`: documented the unanchored learner joining architecture, formalizing that joining nodes initialize
  configuration state purely from local storage without requiring external configuration index knowledge, operate as unanchored learners in the `Joining` role until fused log batches or snapshots introduce their inclusion, and transition
  directly to `Follower`; updated Section 7.III (Log Compaction Boundaries) to document the Snapshot-Superseded Log Record Fusion Invariant, formalizing that log batches whose entries predate a verified local snapshot are evaluated as
  consistent without requiring previous log index matching against compacted history.

## [2026-09-30T03:55:00Z]

- Documented the Active Electorate Replication Resilience over Quiescence invariant in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that the leader must distinguish between retiring followers and active cluster peers when
  processing `QUIESCED` append responses; active configuration peers (including newly introduced members in joint consensus) must not have their replication progress decommissioned, but must instead be treated as transiently unreachable
  with rolled-back optimistic send indices and retry scheduling to prevent permanent joint consensus quorum deadlock when an active peer restarts.

## [2026-09-29T18:25:00Z]

- Documented the Ingress Gating Precondition for Graceful Decommissioning in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing the architectural requirement that the host integration layer, ingress gateway, or
  `ClusterParticipant` bridge must gate, reject, or drain inbound client command traffic prior to or concurrently with requesting an empty electorate change (`Set.empty`), establishing that retired and quiesced participants permanently
  reject client commands with `Unable(FALLBACK)` and that a zero-voting-member cluster cannot commit entries.

## [2026-09-28T20:12:00Z]

- Updated Section 10 (Electorate Change Protocol & Response Semantics) in `okf/lazy-multi-raft-consensus.md`: formalized the invariant that commit watermark advancement and request completion must strictly precede ghost leader retirement
  and quiescence authorization, preventing loss of request tracking during second-phase configuration change replication and ensuring uncommitted log entries are finalized prior to role exit; formalized the lower-bound monotonicity
  invariant for committable record index searches across consensus configurations, preventing watermark regression.

## [2026-09-26T23:35:00Z]

- Decoupled the election protocol domain in Section 9 (On-Demand Election Protocol, Single-Vote-Per-Term Invariant & Promotion Dynamics) and Section 13 (Host Bridge RPC Completion & Transport Termination Invariants) of
  `okf/lazy-multi-raft-consensus.md`: formalized the boundary separation between Phase 1 local candidate nomination (`CandidateNomination`) and Phase 2 remote ballot solicitation (`RequestBallot` yielding `Ballot`), documented the
  elimination of artificial candidate ID wrapping on neutral quorum ballots, established that responding voters report their own role rank for neutral quorum buffering rather than candidate identities, and updated outbound host bridge RPC
  inventories to reflect `RequestBallot`.

## [2026-09-25T22:45:00Z]

- Formalized the 3-tier candidate membership hierarchy (Surviving > Retiring > Joining) during joint consensus transitions in `okf/lazy-multi-raft-consensus.md`, articulating the mathematical proofs for election liveness (old-set precedence
  over new-only to prevent split-vote livelock) and ghost leader elimination (surviving precedence over retiring to avoid redundant handovers), and specifying fallback ordering for disjoint cluster migrations.

## [2026-09-25T18:15:00Z]

- Articulated the old-set candidate precedence invariant ($C_{\text{old}}$ over $C_{\text{new}} \setminus C_{\text{old}}$) during joint consensus transitions in `okf/lazy-multi-raft-consensus.md`, formalizing how prioritizing old
  configuration members prevents split-vote election deadlocks when uncommitted transitional entries create asymmetric active electorates between lagging nodes operating under $C_{\text{old}}$ and updated nodes operating
  under $C_{\text{old,new}}$.

## [2026-09-24T14:45:00Z]

- Streamlined discovery and reconciliation data pipelines by replacing intermediate hash map caches with direct positional propagation of peer inquiry replies (`IArray[Try[StateInfo]]`) across state reconciliation, commit index absorption,
  and candidate vote decision; formalized structural binding of the inquiry-active electorate (`electorateAtRequest`) via lexical propagation in `start` and universal reconciliation guards rather than carrier payload replication, while
  preserving modular `electorate` domain signatures for vote evaluation.

## [2026-09-24T06:25:00Z]

- Purged remaining ballot vestiges across consensus protocol models and role transitions: eliminated `INTERNAL_VACATE_HANDOFF` and `isInternalVacateHandoff` from `CommandAttemptFlag`, realigning vacated command delegation directly to
  `LEADERSHIP_VACATED`; renamed `StateInfo.tiesWith` to `matchesValues` and removed unused `isTyingWith`, formalizing state comparison as an allocation-avoidance cache comparator rather than an election-round tie-breaker; and removed
  diagnostic tracking scaffolding (`borrame`) from `CandidateDecider`.

## [2026-09-24T02:30:00Z]

- Refined Section 10 (Electorate Change Protocol & Response Semantics) in `okf/lazy-multi-raft-consensus.md`, articulating client-facing terminality contracts, leader redirection semantics, and commit decoupling, while eliminating
  language-level Scala implementation mechanics in accordance with OKF persistent-only principles.

## [2026-09-24T01:35:00Z]

- Updated Section 10 (Electorate Change Response Architecture) in `okf/lazy-multi-raft-consensus.md`, formalizing the refactoring of `ElectorateChangeResponse` into a Scala 3 parameterized ADT
  `enum ElectorateChangeResponse(val isTerminal: Boolean)`, eliminating legacy trait hierarchies and runtime type inspection, establishing zero-allocation static singletons for parameterless outcomes, and strictly encapsulating redirection
  target identifiers within `AskTheLeader`.

## [2026-09-23T22:45:00Z]

- Updated Section 9 (On-Demand Election Protocol, Single-Vote-Per-Term Invariant & Promotion Dynamics) and Section 10 (Electorate Change Response Partitioning) in `okf/lazy-multi-raft-consensus.md`, formalizing the purge of the vestigial
  intra-term `Ballot` mechanism across all consensus protocol data models, electorate quorum accumulators, participant state machine transitions, and non-terminal electorate change responses, realigning election safety strictly with
  canonical Raft monotonic term bumping and single-vote-per-term invariants.

- Updated Section III (Quiescence Protocol & Convergence Preconditions) in `okf/lazy-multi-raft-consensus.md`, formalizing that quiescence authorization tracking and retry wake-up scheduling are strictly encapsulated within the `Leader` role lifecycle alongside retirement log replication pipelines, eliminating cross-role tracking state leaks and simplifying quiescence convergence preconditions.

## [2026-09-23T20:13:00Z]

- Updated Section III (Quiescence Protocol & Convergence Preconditions) in `okf/lazy-multi-raft-consensus.md`, formalizing that log replication pipelines for retiring participants are strictly encapsulated within the `Leader` role lifecycle and terminate upon leaving `Leader`, eliminating stale driver registry leaks across subsequent role transitions and refining quiescence convergence preconditions.

## [2026-09-23T17:45:00Z]

- Documented Delegation of Client Command Deduplication to the State Machine in Section 4 of `okf/lazy-multi-raft-consensus.md`, formalizing that the consensus engine treats client commands as opaque payloads, delegating per-client deduplication, freshness ordering, and retry filtering to the application-level state machine to maintain storage SPI simplicity and compatibility with log compaction snapshot boundaries.

## [2026-09-22T22:50:00Z]

- Updated `okf/adr-component-definition-module-pattern.md` with Section 6 (Horizontal CDM Composition via Self-Types) and Section 7 (Decoupling State Mutators from Coordination Lifecycles), formalizing the self-type mixin convention between interdependent CDMs (`ConsensusPrimaryStateCdm` and `ConsensusElectorateCdm`), complete isolation of state persistors from coordinator actor lifecycles, and explicit watermark parameterization.

## [2026-09-22T06:10:00Z]

- Documented the Component Definition Module (CDM) Pattern for Service Decomposition in `okf/adr-component-definition-module-pattern.md`, specifying the architectural conventions for decomposing complex Service Definition Modules (SDMs) while preserving path-dependent type unity and eliminating generic type parameter bloat.

## [2026-09-22T03:30:00Z]

- Removed the obsolete transitional `Promoting` role and its associated `PROMOTING` ordinal, realigning `LEADER` as the sole leading role ordinal (`32`) matching `ER_LEADING`.
- Documented Direct Leadership Inauguration in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing that because candidate term bumping and self-voting are committed to persistent storage prior to Phase 2 vote collection, winning an
  election permits immediate synchronous inauguration into `Leader` without an intermediate persistence holding state or covenant barriers.

## [2026-09-22T00:55:00Z]

- Updated Section 1 of `okf/adr-causal-fence-state-synchronization.md` to formalize the Temporal Window of Causal Validity invariant, specifying that state references are invalidated across asynchronous suspension points and requiring
  explicit re-anchoring via `causalAnchor()` before accessing state.
- Updated Command-Query Separation in State Discovery in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing the three-phase decoupling of consensus role updates into a side-effect-free discovery query, an asynchronous state
  reconciliation command advancing causal fences and absorbing committed logs, and a pure synchronous electoral vote calculation.

## [2026-09-21T19:55:00Z]

- Documented the Commit Watermark Broadcast Invariant in Section 8 and Command-Query Separation in State Discovery in Section 9 of `okf/lazy-multi-raft-consensus.md`, establishing the obligation to re-drive replication across all active and
  retiring peer pipelines upon commit index advancement to prevent ghost leader retirement deadlocks in heartbeat-free clusters, and formalizing that state discovery evaluation operates strictly as a side-effect-free query whose active
  leader outcome bypasses numerical quorum constraints in the subsequent command transition.

## [2026-09-20T16:20:00Z]

- Documented Read Consistency Architecture & Omission of Quorum Verification (`ReadIndex`) in Section 7 of `okf/lazy-multi-raft-consensus.md`, formalizing the architectural rationale for omitting lease-free quorum verification and leader
  leases in favor of network-quiescent session watermarks, defining the applied command boundary invariant over commit index bounds for reads, and detailing in-memory sequencer awaiters over database triggers.

## [2026-09-20T01:45:00Z]

- Documented Early-Terminating Quorum Accumulation & Strict Phase Isolation in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing reactive incremental quorum resolution for Phase 1 discovery and Phase 2 voting across stable and
  transitional configurations, short-circuiting on decisive victory or mathematical impossibility, immediate term preemption and active leader aborts, instant cancellation and disposal of in-flight peer inquiries, and the strict prohibition
  of cross-phase trailing reply preemption.

## [2026-09-17T15:40:00Z]

- Documented Temporal Window of Causal Safety & Asynchronous Re-Anchoring in Section 10 of `okf/lazy-multi-raft-consensus.md`, formalizing that `PrimaryState` references are strictly valid only within the synchronous body of consumers
  subscribed to `advance` or `causalAnchor`. Crossing asynchronous boundaries (such as commit index awaiters) or deferred dispatches renders prior state references obsolete, requiring explicit re-anchoring via
  `primaryStateFence.causalAnchor()` before inspecting primary or derived state.

## [2026-09-17T15:03:00Z]

- Documented Out-of-Band Commit Index Absorption Invariant in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing how active candidates resolve election deadlocks when losing to structurally ineligible participants
  (retirees/joiners with more complete logs) by advancing `commitIndex` out-of-band via Raft's Log Matching Property without active leader heartbeats.

## [2026-09-17T14:56:00Z]

- Documented Deterministic Candidate Ranking Precedence Order in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing the exact evaluation tuple
  `(currentTerm, isLeading, lastRecordTerm, lastRecordIndex, isCandidate, isInCommonConfig, participantId)` and articulating the architectural rationale for prioritizing `isLeading > completeness` (minimizing unprovoked leader abdications
  within an active term) and `completeness > isCandidate` (forcing lagging candidates to absorb committed records from retiring or joining participants before election eligibility).

## [2026-09-16T23:05:00Z]

- Documented Local Voter Evaluation Boundary in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing that voters evaluate candidate vote solicitations strictly against local log completeness and the single-vote-per-term invariant
  without dispatching secondary discovery queries to third-party peers, preventing query cascades and cluster election deadlock under minority partitions.

## [2026-09-16T22:30:00Z]

- Documented Pre-Vote State Discovery, Preemptive Candidate Term Bumping, and Voter Epoch Fencing in Section 9 of `okf/lazy-multi-raft-consensus.md`, formalizing the Fenced Epoch Voting Invariant, Single-Vote-Per-Term Invariant, and the
  elimination of post-quorum term bumping.

## [2026-09-12T01:35:00Z]

- Documented Host Bridge RPC Completion & Transport Termination Invariants in Section 12 of `okf/lazy-multi-raft-consensus.md`, formalizing the asynchronous completion obligations of host-provided `Capture` handles, the impact of unresolved
  queries on multi-candidate aggregation pipelines, simulation harness packet drop equivalence, and uniform transport exception semantics.

## [2026-09-11T21:15:00Z]

- Documented the Diagnostic Inspection & Causally Consistent Observation Invariant in Section 11 of `okf/lazy-multi-raft-consensus.md`, specifying that internal participant and role diagnostics must observe discrete sequencer step
  boundaries to preserve causal lockstep across configuration changes and replication metrics without violating single-threaded sequencer confinement.

## [2026-09-11T18:50:00Z]

- Documented the Client Response Contract & Commit Watermark Invariant in Section 7 of `okf/lazy-multi-raft-consensus.md`, establishing that `Processed` responses must expose both the distributed log `RecordIndex` and `StateMachineResponse`
  to decouple session consistency (read-your-writes and linearizable reads) from application payload schemas.

## [2026-09-05T02:00:41Z]

- Documented retirement driver invariants in okf/lazy-multi-raft-consensus.md, including leader push semantics, leaderCommit clamping vs. trailing ghost records, snapshot fallback mechanics, the stateless Retiring role, and
  GracefullyReleased non-fatal pipeline aborts.

## [2026-08-29T20:07:00Z]

- Documented Configuration Change Response & Ballot Propagation Invariants in Section 9 of `okf/lazy-multi-raft-consensus.md`.

## [2026-08-29T16:50:00Z]

- Documented the Universal Log Fusion & Obsolete Suffix Truncation Invariants across all consensus participant roles in Section 7 of `okf/lazy-multi-raft-consensus.md`.

## [2026-08-28T15:20:00Z]

- Documented on-demand reactive election protocol, deterministic candidate ranking, ballot mechanics, the one-vote-per-ballot invariant, and leader promotion term-advancement dynamics in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-24T18:14:00Z]

- Documented the architecture of the persistence split between the `Workspace` (dumb log SPI) and `Accessible` (Raft invariant enforcement) layers in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-23T01:17:00Z]

- Documented Recovered Commit Floor Invariant and Log Compaction Truncation Bounds in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-22T00:34:00Z]

- Documented Failover Retirement Recovery across newly elected leaders and Contiguous Log Slicing from log buffer offset in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-21T05:52:00Z]

- Documented Ghost Leader Reconfiguration Invariant and Asynchronous Learner Convergence vs. Quorum Progress Decoupling dynamics in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-04T16:48:00Z]

- Documented retirement driving mechanism, log-append retries, and snapshot fallback invariants during configuration retirement transitions in `okf/lazy-multi-raft-consensus.md`.
- Refined OKF maintenance guidelines in `AGENTS.md` and `.agents/skills/okf_maintenance/SKILL.md` to mandate high-level invariant focus, forbid code implementation details/variable names, and prevent raw session/debugging logs.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-04T13:08:00Z]

- Formulated structural design suggestions for `RetirementDriver` cleanup sequencing and `CandidateDecider` retiree voting eligibility constraints.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-04T13:04:00Z]

- Analyzed `ConsensusParticipantSdm.scala:3843` assertion error triggered at `scratch_2.log:2770-2772`, preventing driver cleanup on lag.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-03T21:17:00Z]

- Analyzed `CandidateDecider` and `whenVotingAnother` livelock when a candidate node votes for a higher-term `RETIREE` whose `commitIndex` exceeds the node's local log.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-03T19:05:00Z]

- Analyzed `ConsensusParticipantSdm` quiescence state machine and verified `RetirementDriver` callback object identity reference.
- Updated `okf/lazy-multi-raft-consensus.md` timestamp.

## [2026-08-02T12:48:00Z]

- Documented `Trial` sentinel update to `EMPTY = new AnyRef` and null-safety requirements in `isEqualTo`, `equals`, and `contains` when wrapping `Null` or `Maybe.empty` in `okf/common-primitives.md`.

## [2026-08-01T17:31:00Z]

- Added `trait SchedulingDoer extends Doer, SchedulingExtension` in `Doer.scala`.
- Made `SchedulingExtension` extend `ScheduledFluxExtension` (`trait SchedulingExtension extends ScheduledFluxExtension { thisDoer: Doer => ... }`).
- Replaced type intersection `Doer & SchedulingExtension` with `SchedulingDoer` across cluster services (`ParticipantService`), Akka integration (`ActorBasedSchedulingDoer`), benchmark suites (`SchedulingDoerBenchmark`), and test suites
  (`ScheduledMonoTests`, `ScheduledFluxTests`, `GeneratorsForDoerTests`).
- Updated `okf/sequencer-core.md` timestamp.

## [2026-08-01T16:27:00Z]

- Mixed `FluxExtension` and `LoopingExtension` directly into `Doer` trait (`trait Doer extends DoerCore, DoerTaskOps, FluxExtension, LoopingExtension`).
- Simplified provider facades (`CooperativeWorkersDp`, `CooperativeSchedulerDpCompanion`, `StandardSchedulingDp`) and test traits (`FluxDoerTests`, `LoopingDoerTests`, `ScheduledFluxTests`, `GeneratorsForDoerTests`) to eliminate redundant
  extension mixins and casts.
- Updated `okf/sequencer-core.md` timestamp.

## [2026-07-31T15:48:00Z]

- Added explicit type ascriptions `(Maybe.empty: Maybe[Int])` in `common/src/test/scala/MaybeTest.scala` for `map`, `flatMap`, `fold`, and `exists` tests to fix parameter type inference in closures.

## [2026-07-31T15:40:00Z]

- Removed invalid extension method `==` in `Maybe.scala` to resolve compiler warnings `[E194]` (unselectable extension method) and `[E121]` (unreachable case pattern match).

## [2026-07-31T10:53:00Z]

- Implemented `mapImpl` and `flatMapImpl` quote macros in `MaybeMacros.scala` to enforce compile-time anti-nesting and non-nullability checks on return type parameter `B` for `Maybe.map` and `Maybe.flatMap`.
- Refactored `map` and `flatMap` extension methods in `Maybe.scala` to delegate to `MaybeMacros.mapImpl` and `MaybeMacros.flatMapImpl`.
- Updated `okf/common-primitives.md`.

## [2026-07-30T13:35:00Z]

- Designed `Capturer` functional operand execution count test suite in `sequencer/core` (`MonoTests.scala`):
    - Documented property-based test checking that all factory methods and transformation operators (`apply`, `defer`, `fromFutureBuilder`, `combine`, `andThen`, `withFilter`, `map`, `flatMap`, `transform`, `transformWith`, `recover`,
      `recoverWith`, and guarded variants) execute passed functional operands at most once (0 or 1 time), even across multiple subscriptions.
    - Updated `okf/sequencer-core.md`.

## [2026-07-29T18:56:00Z]

- Extracted `ResultIncrementalCoalescingDoerTests` mix-in trait in `sequencer/core`:
    - Centralized `ResultIncrementalCoalescing` test suite (`first contender wins`, `second contender supersedes`, `second contender yields`, `new competition starts after previous completes`) into a reusable trait.
    - Mixed `ResultIncrementalCoalescingDoerTests` into all provider test suites supporting scheduling & looping (`StandardSchedulingDpTest`, `CooperativeContainedPollingSchedulerDpTest`, `CooperativeFlatPollingSchedulerDpTest`, etc.).
    - Updated `okf/sequencer-core.md` documentation guidelines.

## [2026-07-29T17:49:00Z]

- Created `CooperativeWorkersDoerTests` mix-in trait in `sequencer/providers`:
    - Extracted shared `CooperativeWorkersDp` thread-pool lifecycle, pending runnable tracking, and worker sleep/wakeup race condition tests into a reusable trait.
    - Updated `okf/sequencer-core.md` guidelines.

## [2026-07-29T17:34:00Z]

- Modularized `CooperativeWorkersDpTestNew.scala` in `sequencer/providers`:
    - Created `CooperativeWorkersDpTestNew` extending `DoerProviderTestBase[DoerFacade]` and mixing in `VanillaDoerTests[DoerFacade]`.
    - Configured `TestThreadFactory` and `onUnhandledException` bridging.
    - Verified 100% test pass rate (38/38 tests passed).

## [2026-07-29T17:25:00Z]

- Modularized `DoerProvider` testing framework in `sequencer/core` and `sequencer/providers`:
    - Centralized suite lifecycle, logging, property sample generators (`forAllTaskOperandExceptions`, `forAllSubscribeExceptions`), and exception assertions in `DoerProviderTestBase`.
    - Created capability-focused test traits (`VanillaDoerTests`, `FluxDoerTests`, `SchedulingDoerTests`, `ScheduledFluxDoerTests`, `LoopingDoerTests`).
    - Created single concrete provider test suites (`StandardSchedulingDpTestNew`, `CooperativeContainedPollingSchedulerDpTestNew`, etc.) mixing in supported extension traits.
    - Documented testing architecture and thread exception suppression in `okf/sequencer-core.md`.

## [2026-07-28T17:25:00Z]

- Implemented `ScheduledFluxExtension` in `sequencer/core/src/main/scala/ScheduledFluxExtension.scala`.
- Moved periodic stream scheduling operations (`task.scheduled`, `Flux_schedules`, `Flux_schedulesFlat`) to `ScheduledFluxExtension`, returning `Flux[A]` / `TimedFlux[A]`.
- Refactored single-shot delay implementations in `SchedulingExtension` (`Task_Delayed`, `Task_DelaysSupplier`, `Task_DelaysSupplierFlat`) returning `Task[A]` / `TimedTask[A]`.
- Mixed `ScheduledFluxExtension` into `StandardSchedulingDp.ProvidedDoerFacade` and `CooperativeSchedulerDpCompanion.SchedulingDoerFacade`.
- Added abstract test suite `ScheduledFluxDoerProviderTest` and updated `SchedulingDoerProviderTest` for single-shot delay assertions.
- Updated `okf/sequencer-core.md` documentation.

## [2026-07-28T17:20:00Z]

- Clarified public method mapping between `SchedulingExtension` (single-shot `Task`/`Mono` delays) and `ScheduledFluxExtension` (periodic `Flux` stream schedules).

## [2026-07-28T17:15:00Z]

- Evaluated trait naming options (`ScheduledFluxExtension` vs `TimedFluxExtension`) for the proposed Option 3 extension in `sequencer/core`.

## [2026-07-28T17:13:00Z]

- Analyzed architectural design options for integrating periodic scheduling primitives with `FluxExtension` to replace incorrect `Task` return types with `Flux` in `SchedulingExtension`.

## [2026-07-27T20:17:00Z]

- Fixed `emitterGen` in `GeneratorsForDoerTests.scala` by deferring `StreamEmitter` emissions to `subscribeSync` invocation.
- All 17 test cases in `FluxDoerProviderTest` suites now pass across all 7 `DoerProvider` implementations.

## [2026-07-27T20:09:00Z]

- Added concrete `FluxDoerProviderTest` suites for all `DoerProvider` implementations in `sequencer/providers`.
- Mixed `FluxExtension` into `StandardSchedulingDp.ProvidedDoerFacade` and `CooperativeSchedulerDpCompanion.SchedulingDoerFacade`.

## [2026-07-27T19:52:00Z]

- Documented `FluxExtension` testing infrastructure and abstract `FluxDoerProviderTest` suite specification in `okf/sequencer-core.md`.

## [2026-07-26T15:51:15Z]

- Updated `okf/common-primitives.md` to document how `Maybe`'s opaque type scope prevents macro `OrType` matching on nested `Maybe[T]` instances.

## [2026-07-25T13:28:00Z]

- Updated `okf/common-primitives.md` to reflect `Maybe`'s refactored `opaque type` definition and compile-time macro anti-nesting guards.

## [2026-07-22T17:45:00Z]

- Added architectural decision record `okf/adr-spare-slot-pattern-evaluation.md` evaluating the spare-slot zero-allocation pattern in `sequencer/core` primitives.

## [2026-07-22T15:35:00Z]

- Refined the DoerProvider selection guide in `okf/sequencer-core.md` by integrating structural "like/unlike" comparative points directly into every scheduler item description, removing redundant shared traits, and simplifying the workload
  descriptions.

## [2026-07-22T15:15:00Z]

- Aligned `CooperativeLocalPollingSchedulerDp` and `CooperativeContainedPollingSchedulerDp` descriptions in `okf/sequencer-core.md` using identical phrasing for shared features, highlighting memory reclamation behavior as their primary
  differentiator.

## [2026-07-22T15:00:00Z]

- Refactored the DoerProvider selection guide in `okf/sequencer-core.md` to sort all seven scheduling implementations in descending order of utility probability (with Local and Contained Polling first) and added comparative "like/unlike"
  analysis.

## [2026-07-22T14:30:00Z]

- Added documentation for the `CooperativeContainedPollingSchedulerDp` contained scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-18T02:12:36Z]

- Added the complete empirical benchmark results table to the bottom of `okf/sequencer-core.md`.

## [2026-07-18T02:04:31Z]

- Corrected Sharded Polling scheduler performance affirmations in `okf/sequencer-core.md` based on empirical benchmark results showing that Local Polling consistently outperforms Sharded Polling due to its lock-free execution model.

## [2026-07-16T22:05:00Z]

- Updated `okf/sequencer-core.md` with performance characteristics and trade-offs of Hierarchical, Sharded, and Local Polling schedulers discovered during benchmarking under load.

## [2026-07-15T04:35:00Z]

- Added high-level architecture documentation for the `ThreadDrivenScheduler` component to `okf/sequencer-core.md`.

## [2026-07-15T04:15:00Z]

- Documented the optional `trackSleepTime` constructor parameter and telemetry-overhead design toggle in `okf/sequencer-core.md`.

## [2026-07-14T23:12:00Z]

- Added the design goal, execution model, and mathematical correctness proof for Timer Timing Precision and millisecond rounding to `okf/sequencer-core.md`.

## [2026-07-14T19:30:00Z]

- Updated Hierarchical Polling scheduler cons in `okf/sequencer-core.md` to reflect lazy initialization of its private priority queue.

## [2026-07-14T02:56:00Z]

- Renamed all scheduling-enabled `DoerProvider` classes to follow a symmetrical, clean, and semantically honest naming convention in `okf/sequencer-core.md`.

## [2026-07-14T02:15:00Z]

- Added documentation for the `CooperativeShardedPollingSchedulerDp` sharded scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-14T01:27:00Z]

- Added documentation for the new `CooperativeLocalPollingSchedulerDp` thread-local scheduler implementation in the DoerProvider selection guide in `okf/sequencer-core.md`.

## [2026-07-13T18:30:00Z]

- Added the DoerProvider Selection Guide detailing all eight scheduler and executor implementations in `okf/sequencer-core.md`.

## [2026-07-13T03:48:00Z]

- Documented two-level scheduling heap architecture and chronological ordering guarantees for cross-doer scheduling in `okf/sequencer-core.md`.

## [2026-07-09T14:30:00Z]

- Added ADR `okf/adr-simplify-error-handling.md` detailing the design decision to remove generic exception guarding and unify unhandled exception routing under `onUnhandledException`.
- Updated `okf/index.md` to reference the new ADR.

## [2026-07-09T00:08:20Z]

- Incorporated the final design discussion regarding quorum response times (Order Statistics / median latency) and availability vulnerabilities of small subgroups vs. full cluster replication in `okf/lazy-multi-raft-consensus.md` and
  `okf/transcript-lazy-multi-raft-design.md`.

## [2026-07-08T23:32:00Z]

- Added concept documentation for the Lazy Multi-Raft consensus architecture (`okf/lazy-multi-raft-consensus.md`).
- Included and translated the design discussion transcript on Lazy Multi-Raft to English (`okf/transcript-lazy-multi-raft-design.md`).
- Updated `okf/index.md` to reference the new documents.

## [2026-07-07T15:00:00Z]

- Added testing guidelines and compatibility analysis regarding the suppression of simulated exceptions and why changing the signature of `onUnhandledException` is avoided, documented within `okf/sequencer-core.md`.

## [2026-07-03T16:58:00Z]

- Documented `Muxer` encapsulation and `DefaultCapturer` unchecked variance inheritance design decisions in `okf/sequencer-core.md` and added explanatory Scaladoc notes in `sequencer/core/src/main/scala/Doer.scala`.

## [2026-07-01T12:15:00Z]

- Added and refined ADR `okf/adr-scheduling-subscription-design.md` detailing the design options and recommendation for exposing schedules in timed task subscriptions, detailing the new `.onSubscription` side-effect hook.
- Documented user-guide specifications for the scheduling split model, TimedSubscription access, and immediate subscription hooks in `okf/sequencer-core.md`.
- Implemented `TimedSubscription` and `TimedTask` traits, split scheduling extension methods (lazy on `Task` receiving duration properties, immediate/hot on `Capturer` receiving pre-built `Delay <: Schedule` directly), and updated scheduled
  supplier factories inside [SchedulingExtension.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala).
- Implemented the `onSubscription(Schedule => Unit)` hook and backing class `Task_OnSubscription` inside [SchedulingExtension.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/main/scala/SchedulingExtension.scala), providing
  synchronous pre-trigger access to schedules.
- Added a unit test in [SchedulingDoerProviderTest.scala](file:///C:/Projects/tools/matrix/sequencer/core/src/test/scala/SchedulingDoerProviderTest.scala) verifying `onSubscription` lifecycle execution.

## [2026-07-01T11:59:48Z]

- Documented `ResultIncrementalCoalescingGrouped` architecture and reference lifetime invariants in `okf/sequencer-core.md`.
- Documented the `Muxer` iteration safety model and FIFO subscription order invariants in `okf/sequencer-core.md`.
- Documented `ResultIncrementalCoalescing` monotonic convergence invariants in `okf/sequencer-core.md`.

## [2026-06-26T01:42:00Z]

- Documented strict equality compatibility requirements (using `eq null` check and `.equals`) for `Trial`'s equality checks in `okf/common-primitives.md`.

## [2026-06-26T01:40:00Z]

- Documented NPE risk in direct `.equals` calls on empty Trial instances and the need for null-safe `==` checks in `okf/common-primitives.md`.

## [2026-06-26T01:25:00Z]

- Updated OKF documentation in `okf/common-primitives.md` to reflect the fix of `getOrElse` and `exists`, and the privacy change to `Trial.Failure` preventing nested collision issues. Addressed pending design arguments for exception
  comparison.

## [2026-06-25T18:51:15Z]

- Documented Task and Capturer semantic evaluation invariants in sequencer-core.md.
- Documented the introduction of shared helper classes (DefaultCapturer_FlatMap, DefaultCapturer_FlatMapGuarded, DefaultCapturer_TransformWith, DefaultCapturer_RecoverWith) to optimize Observable and Task pipelines.

## [2026-06-25T02:00:00Z]

- Optimized Capturer-returning combinators (withFilter, map, mapGuarded, flatMapGuarded, transform, transformWith, recover, recoverWith) on DefaultCapturer to use lightweight anonymous DefaultCapturer subclasses directly, removing Captor
  allocations.

## [2026-06-24T19:30:00Z]

- Documented transition function caching (`maybeMonoB`) design guidelines for `Capturer` combinators in `sequencer-core.md`.

## [2026-06-24T13:17:00Z]

- Documented Sequencer Core components and Captor zero-allocation implementation guidelines in sequencer-core.md.

## [2026-06-24T02:10:00Z]

## [2026-08-10T19:08:00Z]

- Documented per-peer in-flight append backpressure, monotonic serial tracking, and `AppendResult` ADT non-mutation invariants in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-11T20:15:00Z]

- Documented Raft §5.4.2 commitment verification invariants, current-term no-op entry insertion triggers, and committed-only log compaction boundaries in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-12T15:10:00Z]

- Documented `Retiring` participant `StateInfo` election bounding invariants (exposing excluding configuration change index/term to prevent election deadlocks) in `okf/lazy-multi-raft-consensus.md`.

## [2026-08-12T20:16:00Z]

- Restructured and expanded Section 6 in `okf/lazy-multi-raft-consensus.md` to be fully self-contained, defining the Participant Role Lifecycle, Retirement Replication Driver Architecture, Quiescence Protocol, and Quiescence Preconditions.
- Corrected the Quiescence Precondition specification in `okf/lazy-multi-raft-consensus.md` to clarify that retirement driver clearance is evaluated against the participant's own local registry (for drivers it initiated as leader to catch
  up excluded followers), not a remote leader registry.

- **2026-08-26**: Added Electorate Validation Scope to Ghost Leader Reconfiguration Invariant in lazy-multi-raft-consensus.md.

## [2026-09-01T23:10:00Z]

- Updated component specifications in `okf/sequencer-core.md` to reflect the renaming of the memoized reactive trait `Capturer` (and `DefaultCapturer`) to `Capture` (and `DefaultCapture`), aligning architectural documentation with the
  concept-noun to agent-noun taxonomy (`Capture` vs. `Captor` / `Keeper`).
- Synchronized scheduling extension specifications in `okf/adr-scheduling-subscription-design.md` and reactive primitive evaluations in `okf/adr-spare-slot-pattern-evaluation.md` with the `Capture` nomenclature.
- 2026-09-09: Added ADR `okf/adr-causal-fence-state-synchronization.md` to document the Game-Changing Invariant and the Decoupled Mutation Contract for `CausalFence`.

## [2026-09-17T21:05:00Z]

- Expanded Section 10 of `okf/lazy-multi-raft-consensus.md` and Section 2 of `okf/adr-causal-fence-state-synchronization.md` with an exhaustive architectural specification of the Decoupled Mutation Contract.
- Documented the hot-path performance asymmetry rationale (allocations and latency eliminated from client command replication in exchange for caller self-deferral on rare mutating paths).
- Documented the technical rejection of synchronous inline re-anchoring due to the asynchronous nature of `CausalFence.causalAnchor()`.
- Documented the technical rejection of dynamic mutation detection in `recalculateCommitIndex` due to intra-batch turn-splitting and stack re-entrancy hazards.
- Specified the resulting purity invariant `assert(primaryStateFence.committedState.is(primaryState0))` for commit index recalculation.

## [2026-09-17T22:30:00Z]

- Updated Section 7.I of `okf/lazy-multi-raft-consensus.md` with the Upfront Current-Term Entry Insertion invariant under Raft §5.4.2 to prevent deadlock when resuming uncommitted log entries from superseded terms.
- Specified the In-Flight Reconfiguration Pipeline Serializability invariant in Section 7.I of `okf/lazy-multi-raft-consensus.md` requiring leader recovery pipelines for uncommitted configuration changes to be bound to the configuration
  completion handle to serialize subsequent reconfiguration requests.

## [2026-09-18T18:15:00Z]

- Documented the Commit Watermark Barrier Invariant in Section 7.I of `okf/lazy-multi-raft-consensus.md`, establishing that awaiting commitment operates strictly as a monotonic watermark barrier that guarantees commitment upon resolution
  and never completes with uncommitted or transient failure statuses while leading.

## [2026-09-18T20:20:00Z]

- Added the Peer vs. Retiree Replication Pipeline Disjunction invariant to Section 7.I of `okf/lazy-multi-raft-consensus.md`, establishing mutual exclusion between active peer and retiring learner pipelines and defining recovery semantics
  for active peers responding in the `RETIRING` role.

## [2026-09-21T13:10:00Z]

- Updated `okf/adr-causal-fence-state-synchronization.md` and Section 11 of `okf/lazy-multi-raft-consensus.md` to specify the Role Exit Decoupling & Re-Entrancy Prevention invariant under the Decoupled Mutation Contract.
- Documented that leader abdication must resolve in-flight commit watermark awaiters in a decoupled manner (e.g. via `sequencer.run`) to prevent vacated client command handoffs from re-entrantly executing role transitions and mutating the
  primary state fence inside inbound RPC turns (such as `onAppendRecords`).
- Documented that sequential configuration change chaining must explicitly self-defer via `Capture_defer` before executing the synchronous prologue of chained requests.

## [2026-09-22T21:05:00Z]

- Aligned consensus domain model terminology across `okf/adr-component-definition-module-pattern.md`, `okf/lazy-multi-raft-consensus.md`, and `okf/adr-causal-fence-state-synchronization.md` to reflect the `Electorate` authority hierarchy:
  - Transitioned membership authority naming from generic configuration to `Electorate`, distinguishing voting authorities from node/runtime configurations.
  - Specified `SoleElectorate` (single stable majority) and `JointElectorate` (two-phase transition) models.
  - Aligned change and log entry references to `ElectorateChange`, `SoleElectorateChange`, and `JointElectorateChange`.
  - Updated participant enumeration terminology to `members` (`activeParticipants` / `members` / `peers`).
  - Updated candidate ranking invariant references from `isInCommonConfig` to `isInCommonSet`.

## [2026-10-07T21:38:00Z]

- Added Section 14 (Node Lifecycle & Control Plane Target Synchronization Invariants) to `okf/lazy-multi-raft-consensus.md`.
- Formalized that participant re-activation and initial provisioning are governed strictly by the cluster control plane target configuration rather than autonomous resurrection from uncommitted or historical peer proposal states.
- Documented that `cluster.onQuiesced` represents terminal retirement of a consensus instance for its configuration epoch, releasing local resources until explicitly commanded to start by the host daemon.
- Formalized rollback consistency: when an electorate change proposal aborts or fails to achieve committed quorum replication, administrative target membership restores to the surviving committed configuration.

## [2026-10-08T23:45:00Z]

- Expanded Section 14 (Node Lifecycle & Control Plane Target Synchronization Invariants) in `okf/lazy-multi-raft-consensus.md`.
- Formalized quiescence finality: reaching `cluster.onQuiesced` constitutes authoritative proof from the consensus engine that node retirement is committed under an excluding configuration change.
- Established declarative administrative target governance: target membership is derived strictly from intended configuration descriptors ($C_{desired}$ during in-flight transitions, falling back to active membership when idle), ensuring
  retiring nodes are not marked as administrative targets for resurrection upon reaching quiescence.
- Defined reconciliation requirements for indeterminate responses (`RequestTrackingLost...`): the control plane must reconcile unconfirmed configuration proposals with succeeding leaders to eliminate split-brain divergence between the log
  and administrative target sets.

## [2026-10-15T21:42:00Z]

- Updated Section 6 (Participant Exclusion, Retirement Dynamics & Quiescence Protocol) of `okf/lazy-multi-raft-consensus.md` to specify the initial retirement driving invariant for newly crowned leaders.
- Formalized that a newly crowned leader assumes office with an empty active retirement driver registry and must drive the retirement of all historical non-active nodes (`Universe \ Cnew`) upon committing its first sole electorate change,
  preventing abandoned replication to prior retirees when coronation occurs during in-flight, uncommitted configuration transitions.

## [2026-10-21T20:30:00Z]

- Refined the Synchronous Cluster-Tier Probable-Participant Memorization Contract in Section 13 of `okf/lazy-multi-raft-consensus.md`, specifying that the host bridge method `ClusterParticipant.getOtherProbableParticipants` supplies an
  optimistic set of nodes intended for consensus ordered by descending probability of certainty to support client fallback routing and vanishing-electorate teardown, and purged the misplaced administrative control-plane entry from Section
  10.

## [2026-11-06T02:52:00Z]

- Documented consensus protocol binary serialization invariants in Section 13 of `doc/architecture/lazy-multi-raft-consensus.md`, covering fixed-width term codecs, byte-encoded role ordinals and election ranks, deterministic insertion-order collection preservation for advisory routing hints, and zero-copy channel decoupling for streaming snapshot payloads.
