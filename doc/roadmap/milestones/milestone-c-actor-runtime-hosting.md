---
type: "Milestone"
id: "M-C"
title: "Actor Runtime Supervised Hosting"
status: "TODO"
progress_percentage: 0
target_modules: ["nexus/core", "consensus"]
prerequisites: ["single-group-consensus-engine"]
timestamp: "2026-10-31T15:45:00Z"
---

# Milestone C: Actor Runtime Supervised Hosting (`consensus` &harr; `nexus/core`)

**Status**: `[TODO]` | **Progress**: 0% | **Target Modules**: `nexus/core`, `consensus` | **Parent Roadmap**: [Matrix System Architecture Roadmap](../index.md)

---

## 1. Objective & Architectural Value

- **Objective**: Package the consensus participant into a supervised actant within `nexus/core` using the Component Definition Module (CDM) pattern.
- **Architectural Value**: Formalizes lifecycle management, failure supervision, message mailboxes, and quiescence handling for consensus participants as first-class citizens of the Matrix actor runtime. Bridges consensus execution to the
  actant supervision tree.

---

## 2. Scope & Deliverables

### In-Scope

- **Consensus Actant Behavior**: Create an actant behavior (`Behavior` in `nexus/core`) wrapping `ConsensusParticipantSdm`.
- **Command Ingress Protocol**: Implement client command queuing through actant mailboxes (`Inqueue`, `Receptor`) with backpressure control.
- **Supervision & Failure Handling**: Map consensus crash/quiescence events (`onQuiesced`, persistent storage exceptions) to actant supervisor directives (`Restart`, `Stop`, `Escalate`).
- **Supervision Test Suite**: Validate actant supervision tree behavior under simulated consensus crashes and restarts.

### Out-of-Scope

- Network serialization (handled in Milestone M-A).
- Multi-raft sharding coordination (handled in Milestone M-D).

---

## 3. Dependencies & Prerequisites

- **Upstream Prerequisite**: Complete and verified single-group consensus state machine (`ConsensusParticipantSdm`).
- **Downstream Dependents**: Milestone M-D (Multi-Raft Sharding & Control Plane Coordinator).

---

## 4. Detailed Work Breakdown

- [ ] Design actant message protocol for client command submission and consensus lifecycle control.
- [ ] Implement `ConsensusParticipantActant` behavior module in `nexus/core`.
- [ ] Connect actant mailbox reception to `ConsensusParticipant` command submission within the sequencer.
- [ ] Implement supervision hooks translating quiescence and unrecoverable storage failures to actant lifecycle directives.
- [ ] Implement unit and integration tests verifying actant supervision, restart, and graceful teardown.

---

## 5. Acceptance Criteria & Verification

1. Consensus participants can be spawned, supervised, and stopped through `Nexus` and `ActantCore` APIs.
2. Inbound messages to the actant mailbox preserve strict single-threaded sequencing without blocking the host worker thread.
3. Unhandled persistence failures cleanly trigger actant supervisor recovery strategies without leaking file handles or memory.

---

## 6. References

- [Multi-Raft System Architecture: Runtime Integration](../../architecture/system-architecture.md#6-runtime-integration--component-relationships)
- [Component Definition Module Pattern](../../architecture/adr-component-definition-module-pattern.md)
- [ActantCore Source Code](file:///C:/Projects/tools/matrix/nexus/core/src/main/scala/core/ActantCore.scala)
- [Nexus Source Code](file:///C:/Projects/tools/matrix/nexus/core/src/main/scala/core/Nexus.scala)
