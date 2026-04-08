# AGENTS.md

This file provides guidance to Antigravity when working in this repository.

## Role

Treat the user as an adult. Do not be condescendent. Be critical. Do not accept the user's premises without question.

## Build, test, and development commands

### Environment

- Toolchain is SBT + Scala 3 (`scalaVersion := 3.8.2`, `sbt.version=1.11.5`).
- Run all commands from the repository root.
- Open and keep SBT in interactive mode to run SBT commands.

### Core commands

- Start SBT interactive mode:
    - `sbt`
- Exit SBT interactive mode:
    - `exit`

#### SBT interactive mode commands

- Reload project:
    - `reload`
- Clean project:
    - `clean`
- List subprojects:
    - `projects`
- Compile all subprojects:
    - `compile`
- Run all tests:
    - `test`
- Compile a specific subproject:
    - `{subproject name} / compile`
- Test a specific subproject:
    - `{subproject name} / test`
- Run a single test suite:
    - `{subproject name} / Test / testOnly {full name of concrete suite class}`
- Run a single test case of a test suite:
    - `{subproject name} / Test / testOnly {full name of concrete suite class} -- -z "*{test name or fragment of it}*"`
- To run directly from the command line, wrap the sbt command in quotes, removing the inner quotes, and cropping the test name to a single word:
    - `sbt "{subproject name} / Test / testOnly {full name of concrete suite class}"`
    - `sbt "{subproject name} / Test / testOnly {full name of concrete suite class} -- -z *{a single word of the test name}*"`

### Lint/format
- There is no dedicated lint/format task configured in this repo (no scalafmt/scalafix config found).
- Use `compile` as the validation baseline.
- Never wrap text. Let the container do that.

### Packaging
- Package jars:
  - `sbt package`
- Docker support exists via `sbt-docker` plugin and a root `dockerfile` definition in `build.sbt`.
  - Use only when building a runnable module with a resolvable `mainClass`.

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
