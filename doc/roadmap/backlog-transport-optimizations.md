---
type: "Backlog"
id: "T-OPT"
title: "Transport Optimization & Cleanup Backlog"
status: "DEFERRED"
priority: "Post-M-D / Profiling-Driven (non-blocking for M-B, M-C, and M-D)"
target_modules: ["common", "nexus/cluster", "consensus/nio2-cluster-bridge"]
prerequisites: ["M-A"]
timestamp: "2026-11-20T01:40:00Z"
---

# Transport Optimization & Cleanup Backlog (`common`, `nexus/cluster`, `consensus/nio2-cluster-bridge`)

**Status**: `[DEFERRED]` | **Scope**: Non-blocking post-M-A throughput optimizations, buffer/thread pooling, wire-format compactness, and legacy cleanup | **Parent Roadmap**: [Matrix System Architecture Roadmap](index.md)

This track consolidates optional transport and serialization improvements relocated from [Milestone A: Physical Network Transport Bridge](milestones/milestone-a-network-transport-bridge.md) so that functional completion of Milestone M-A is cleanly separated from profiling-driven tuning and multi-shard resource pooling. None of these items block [Milestone B](milestones/milestone-b-durable-storage-engine.md), [Milestone C](milestones/milestone-c-actor-runtime-hosting.md), or [Milestone D](milestones/milestone-d-multi-raft-sharding.md).

---

## 1. Wire-Format Compactness

- [ ] `[Efficiency / Wire-Format]` **Anchor + Relative VLQ Delta Wire Format for `StateInfo` and `AppendRecords` (Optimization O7)**: Encode `StateInfo` and `AppendRecords` using full-width anchor terms and record indices (`currentTerm` / `inquirerTerm` and `lastRecordIndex` / `prevLogIndex`) paired with relative VLQ deltas for surrounding terms and indices (tracking the `TODO` markers in `ConsensusProtocolSerializers.scala` and `ClusterWireProtocol.scala`). Increment `ProtocolVersion.OF_THIS_PROJECT` if backward compatibility with `ProtocolVersion(1)` peers is required at the time of implementation.

---

## 2. I/O Pipeline, Memory Pooling & Multi-Shard Governance

- [ ] `[Efficiency / Pooling]` **Pooled `ByteSink.Allocator` Support (Optimization O8)**: Implement a `ByteSink.Allocator` variant with buffer pool support (leveraging `BufferByteSink(backing: ByteBuffer)` and explicit `ByteSource.close()` return-to-pool lifecycle hooks for small streams, and optionally supplying pooled block buffers to `FileByteSink` for small-write accumulation). When Milestones M-B and M-D are active, ensure every adopted inbound `ByteSource` deterministically invokes `close()` regardless of size and align pool ownership at the node level across shards rather than per-actant.
- [ ] `[Efficiency / Throughput]` **Double-Buffering I/O Pipeline Evaluation (Optimization O3)**: Overlap disk I/O and network socket transfer during trailing stream transfers in `PipelinedTransmitter` (and in `Transmitter`, if retained) and `Receiver` using alternating double buffers to decouple disk latency from network latency.
- [ ] `[Efficiency / Threading]` **Linux File Channel Thread-Hop & Executor Profiling (Optimization O6)**: Profile `AsynchronousFileChannel`'s default thread pool on Linux against positional `FileChannel` I/O on a dedicated bounded executor (or `AsynchronousFileChannel.open` with a custom `ExecutorService`) at 64 KiB chunk granularity (assuming Optimization O4 already batches small inbound socket fragments). Because Linux OpenJDK emulates `AsynchronousFileChannel` via blocking `pread`/`pwrite` on a separate thread pool, switching to `FileChannel` on a dedicated executor does not eliminate cross-pool thread hops between the socket `AsynchronousChannelGroup` and disk worker threads; rather, its value lies in bounding disk concurrency (replacing the global unbounded default pool), eliminating per-operation NIO.2 `PendingFuture`/`Invoker` and `CompletionHandler` wrapper overhead, and draining partial writes synchronously in one task. *Priority / Benefit Downgrade with O3:* If Optimization O3 (double-buffered disk/socket overlap) is implemented, per-chunk thread-hop and disk-dispatch latency is hidden concurrently behind network transfer on all links slower than disk + dispatch throughput, downgrading O6 from a serial critical-path latency concern to a secondary CPU-efficiency and thread-pool-isolation optimization mainly relevant on ultra-fast networks (e.g., loopback or 10GbE+) or under high multi-peer snapshot concurrency.
- [ ] `[Scaling / Multi-Shard Resource Governance]` **Consensus Snapshot Concurrency Governor & Elastic Transport Streaming Pool**: Coordinate a two-tier design when multi-raft sharding scales concurrent snapshot transfers across peers: (1) an application-level consensus admission governor in `consensus/core` and `consensus/nio2-cluster-bridge` that bounds concurrent active snapshot transfers to a configured ceiling $K$, throttling outbound snapshot transfers and returning explicit protocol backpressure for excess inbound requests, and (2) an elastic direct buffer pool in `nexus/cluster` that allocates 64 KiB streaming buffers on demand to prevent transport channel blocking or TCP ZeroWindow stalls while bounding idle buffer retention to $K$ instances so burst buffers are collected by the JVM `Cleaner`.

---

## 3. Code Hygiene & Decommissioning

- [ ] `[Hygiene / Decommissioning]` **Legacy Transmitter Pair Removal Evaluation**: Evaluate removing `SequentialTransmitter.scala` and `Transmitter.scala` (along with `TransmitterHandoffSuite.scala` and `SequentialTransmitterSuite.scala`) once operational experience confirms they are unnecessary alongside `PipelinedTransmitter.scala`.
