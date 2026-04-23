# AGENTS.md

This file provides guidance to Antigravity when working in this repository.

## Role

Treat the user as an adult with strong self-esteem. Be critical, honest, and direct. Do not waste words with polite speech. Do not accept the user's premises without question.

## Communication Constraints
- Use standard, formal English. Avoid using idioms, slang, or regionalisms.

## Build, test, and development commands

## Hard Constraints

These constraints are absolute. No amount of contextual reasoning justifies violating them.

1. **Never edit code unless the user explicitly says to edit code.** Describing a problem, pasting logs, or asking for analysis is NOT permission to edit. The user must use words like "fix", "change", "implement", "edit", "refactor", or equivalent direct imperatives targeting code.
2. **Never run tests or compile code unless the user explicitly says to do so.** Analyzing output or reasoning about behavior does NOT require execution. Default to static analysis.
3. **When in doubt, ask.** If the user's intent is ambiguous — whether they want analysis or action — ask before acting.


## Environment
- Toolchain is SBT + Scala 3 (`scalaVersion := 3.8.2`, `sbt.version=1.11.5`).
- Run all commands from the repository root.
- Open and keep SBT in interactive mode to run SBT commands.

### Lint
- There is no dedicated lint task configured in this repo.
- Use `compile` as the validation baseline.

### Format
- Never wrap text. Let the container do that.

## High-level architecture

This is a multi-module Scala codebase centered around deterministic single-threaded sequencing primitives, actor-like runtime components, cluster communication, and consensus.

## Module map (big picture)
- `common`
  - Shared low-level utilities (`Maybe`, macros/helpers, logging config helpers, concurrent collections).
- `sequencer/core`
  - Core execution model (`Doer`) and composable async primitives (`Duty`, `Task`, `LatchedDuty`, `Covenant`, `CausalFence`).
  - This is the foundation used by higher layers for ordered, deterministic mutation.
- `sequencer/providers` + `sequencer/providers-manager`
  - Concrete `Doer` providers (worker-based executors, scheduling-enabled variants) and provider management.
- `sequencer/akka-integration`
  - Adapter layer to run sequencer semantics with Akka typed actor infrastructure.
- `nexus/core`
  - Actor-like runtime built on `Doer`.
  - `ActantCore` drives lifecycle/message processing through `Behavior` and `HandleResult` (`Continue`, `Stop`, `Restart`, etc.).
  - `Nexus`/`NexusTyped` coordinates actant creation, parenting, and doer provisioning.
- `nexus/checked-spuron`
  - Checked-exception-aware behavior wrapper (`CheckedBehavior`) that can be recovered into regular `Behavior`.
- `nexus/cluster`
  - Cluster participant service and protocol stack over async NIO channels.
  - Main entrypoint: `cluster/service/ParticipantService.scala`.
  - Uses delegates (`ParticipantDelegate`, `CommunicableDelegate`, `IncommunicableDelegate`) and explicit protocol/serialization/channel layers.
- `consensus`
  - Consensus logic with a large “service definition module” pattern (`ConsensusParticipantSdm`) that defines participant roles, persistence (`Workspace`), cluster bridge (`ClusterParticipant`), and client response contracts.
  - Also contains a separate `raft` package (`Raft.scala`, `RaftClusterService.scala`) with a more direct Raft module/API.

## Dependency flow to keep in mind
- Foundational flow: `common` → `sequencer/*` → `nexus/*` and `consensus`.
- `nexus/core` depends on sequencer modules, and `nexus/cluster` extends that with transport/protocol concerns.
- `consensus` depends on sequencer abstractions and expects host-provided integration points (cluster + storage + state machine) instead of hardcoding transport/persistence.

## Practical navigation tips for agents
- Start from `build.sbt` to identify module names and dependency edges before changing code.
- For runtime behavior bugs:
  - sequencing/order issues: inspect `sequencer/core/src/main/scala/Doer.scala` first.
  - actant lifecycle/message handling: inspect `nexus/core/src/main/scala/core/ActantCore.scala`.
  - cluster communication/state transitions: inspect `nexus/cluster/src/main/scala/cluster/service/ParticipantService.scala`.
  - consensus role/configuration transitions: inspect `consensus/src/main/scala/ConsensusParticipantSdm.scala`.
- Tests are MUnit/ScalaCheck-based and distributed per module under `src/test/scala`.
