---
type: "Concept"
title: "Byte Streaming Primitives (ByteSource & ByteSink)"
description: "Capabilities, operational models, lifecycle state machines, and concurrency contracts of the transport-agnostic ByteSource and ByteSink primitives in common."
tags: ["common", "primitives", "byte-source", "byte-sink", "streaming", "io"]
timestamp: "2026-11-17T09:41:00Z"
---

# Byte Streaming Primitives: `ByteSource` & `ByteSink`

The `readren.common` module provides foundational, transport-agnostic abstractions for handling finite sequences of binary data: [`ByteSource`](../common/src/main/scala/ByteSource.scala) (an immutable, repeatable supplier of sequential byte streams) and [`ByteSink`](../common/src/main/scala/ByteSink.scala) (a single-use, write-only staged accumulator that transitions into a [`ByteSource`](../common/src/main/scala/ByteSource.scala)). See [Common Primitive Wrappers](common-primitives.md) for lightweight value wrappers ([`Maybe`](../common/src/main/scala/Maybe.scala) and [`Trial`](../common/src/main/scala/Trial.scala)), and [Cluster Binary Serialization](cluster-serialization.md) for network framing integration.

---

## 1. Architectural Motivation & Transport Agnosticism

Traditional JVM streaming primitives—such as `java.io.InputStream` and `java.nio.channels.ReadableByteChannel`—are single-use, cursor-advancing streams: consuming bytes advances an internal pointer destructively, precluding concurrent reads or replayability without intermediate heap buffering. Furthermore, tightly coupling binary payloads to network packet frames or circular direct-buffer rings forces higher-level components to manage socket backpressure and buffer exhaustion.

`ByteSource` and `ByteSink` decouple the representation of bulk binary data from network transports, physical storage engines, and actor thread pools:
- **Decoupled Representation:** A `ByteSource` represents binary content independently of where it resides (in a direct `ByteBuffer`, on disk, or in memory-mapped files).
- **Transport Agnosticism:** Core consensus, persistence SPIs, and business logic interact exclusively with `ByteSource` and `ByteSink`. Network serializers, socket transmitters, and file writers consume or produce these abstractions without leaking channel-specific mechanics into domain models.

---

## 2. `ByteSource`: Capabilities & Operational Invariants

A `ByteSource` represents an immutable, repeatable supplier of sequential bytes with a known bounded extent.

- **Bounded Extent & Exact Size Invariant:** Every `ByteSource` has an exact, queryable byte length (`def size: Long`). It does not model infinite, continuous, or unknown-length streams.
- **Multi-Session Repeatable Extraction Invariant:** Reading bytes from a `ByteSource` is strictly non-destructive. Invoking `openAsyncReader()` or `openChannel()` produces a fresh, independent reading session positioned at offset 0. Multiple reading sessions can execute concurrently or sequentially over the same `ByteSource` instance without shared cursor mutations, data races, or cursor degradation.
- **Dual Modality (Push vs Pull) Invariant:** `ByteSource` provides two symmetrical extraction models:
  - *Asynchronous Push (`openAsyncReader()`):* Non-blocking sequential reading via `ByteSource.AsyncReader` using standard Java NIO.2 `CompletionHandler` callbacks, optimized for event-driven, non-blocking execution pipelines.
  - *Synchronous Pull (`openChannel()`):* Standard blocking channel reads via `java.nio.channels.ReadableByteChannel` (or `FileChannel`), enabling seamless integration with conventional stream decoders and legacy I/O pipelines.
- **Zero-Copy Bulk Transfer Invariant:** `ByteSource` exposes `transferTo(target: WritableByteChannel): Long`. When backed by persistent file channels (`FileByteSource`), implementations must leverage kernel-level zero-copy primitives (`FileChannel.transferTo` via OS `sendfile`) to stream data directly into target channels without copying bytes through JVM heap or user-space memory.
- **Deterministic Lifecycle Invariant:** `ByteSource` extends `java.lang.AutoCloseable`. Closing the root `ByteSource` releases underlying shared file handles, unmaps direct memory regions, or unlinks ephemeral storage files (when created with `deleteOnClose = true`). Closing an individual `AsyncReader` or channel session releases only that session's state and does not close or invalidate the parent `ByteSource` or other concurrent reader sessions.

---

## 3. `ByteSource.AsyncReader`: Concurrency & Single-Flight Contract

`ByteSource.AsyncReader` represents an active, sequential asynchronous reading session over a `ByteSource`.

- **Single-Flight Invariant:** At most one asynchronous `read` operation may be pending per `AsyncReader` instance at any point in time. Invoking `read(...)` while a prior `read(...)` is in progress is strictly prohibited and results in undefined behavior or channel faults.
- **Cross-Thread Serialized Continuity Invariant:** Successive non-overlapping `read` invocations do not require single-thread caller affinity. Under the Java Memory Model (JLS §17.4.5), NIO.2 completion hand-offs through thread pools establish a happens-before relationship between the completion of read $N$ (including internal offset advances) and the initiation of read $N+1$. Therefore, successive reads may safely be triggered from arbitrary thread pool workers (such as from within completion handler callbacks) without requiring `AtomicLong` or internal synchronization locks.
- **Synchronous Return vs. Inline Callback Invariant:** Callers without an iterative trampoline rely on `AsyncReader.read` reporting synchronous completions directly via its `Int` return value rather than invoking `CompletionHandler` inline before returning `PENDING`, preventing unbounded stack recursion on multi-chunk streams.

---

## 4. `ByteSink`: Staged Ingestion & Lifecycle State Machine

A `ByteSink` is a single-use, write-only staged accumulator that ingests bytes asynchronously and transitions into an immutable `ByteSource`.

- **Staging Pre-Allocation & Declared Size Invariant:** A `ByteSink` is provisioned with an `expectedSize: Long`, allowing backing storage implementations to pre-allocate memory buffers or reserve disk blocks upfront, eliminating repeated dynamic reallocation overhead.
- **Early Handle Projection Invariant:** The projected `ByteSource` handle (`def byteSource: ByteSource`) is available immediately upon `ByteSink` instantiation, prior to byte transfer. This allows caller frameworks to synchronously assemble enclosing messages, domain wrappers, or metadata envelopes while the underlying binary bytes are still streaming into the sink.
- **Incremental Non-Blocking Ingestion Invariant:** `write(src, attachment, handler)` ingests byte chunks asynchronously using standard NIO.2 `CompletionHandler` callbacks. Each write operation consumes all available bytes between `src.position()` and `src.limit()` before invoking the completion handler. At most one write operation may be pending at any given time (single-flight write invariant). Callers without an iterative trampoline rely on `ByteSink.write` reporting synchronous completions directly via its `Int` return value rather than invoking `CompletionHandler` inline before returning `PENDING`, preventing unbounded stack recursion on multi-chunk streams.
- **Strict Terminal State Machine Invariant (`seal` vs `discard`):** An active `ByteSink` begins in the *Ingesting* state and must transition into exactly one of two mutually exclusive terminal states:
  - *`seal()` (Success):* Closes write channel descriptors, flushes buffers, and commits the underlying data to an immutable state, enabling the projected `byteSource` to be safely read.
  - *`discard()` (Abort/Fault):* Aborts ingestion immediately, closes channel descriptors, and unlinks/deletes any ephemeral backing resources (such as temporary disk files), preventing abandoned staging artifacts from leaking storage.

---

## 5. `ByteSink.Allocator`: Tiered Staging Strategy

The `ByteSink.Allocator` strategy isolates producers from physical storage topology decisions:

- **Tiered Staging Allocation Invariant:** `DefaultAllocator` dynamically selects the storage medium based on declared stream extent:
  - *In-Memory Tier ($\le 64$ KiB):* Streams below or equal to the staging threshold are staged in reusable direct memory buffers (`BufferByteSink`), avoiding file descriptor allocation and disk latency.
  - *Disk Tier ($> 64$ KiB):* Streams exceeding the staging threshold are staged in temporary files (`FileByteSink`) using asynchronous file channels, ensuring that direct memory consumption across arbitrary streams remains strictly $O(1)$.

---

## 6. Illustrative Applications (Non-Normative)

While `ByteSource` and `ByteSink` are completely transport-agnostic, their design specifically enables high-performance distributed systems patterns:

1. **Out-of-Line Network Message Streaming:** Network transports ([Cluster Binary Serialization](cluster-serialization.md)) can transfer large binary payloads out-of-line in chunked trailing streams following control packages. Senders stream `ByteSource` chunks directly into network socket channels, while receivers stage incoming chunks into a `ByteSink` without exhausting direct-buffer circular storage rings.
2. **Distributed Consensus State Machine Snapshots:** In consensus engines ([Lazy Multi-Raft Consensus](lazy-multi-raft-consensus.md)), state machine snapshots are captured as persistent `ByteSource` instances. Because `ByteSource` is repeatable, a leader can stream the identical snapshot concurrently to multiple lagging followers without cursor contention or intermediate heap array allocations (`IArray[Byte]`).
3. **Application Gateway Spooling:** Gateways and proxies can ingest multipart client uploads into a `ByteSink`, validate business headers upfront via early handle projection, and subsequently stream the sealed `ByteSource` to downstream services or durable object storage.
