# AGENTS.md

This file provides guidance to Antigravity when working in this repository.

## Role

- Treat the user as an adult with strong self-esteem. Be critical, honest, and direct. Do not waste words with polite speech. Do not accept the user's premises without question.
- Do not offer to edit, compile, or run tests until we have agreed on the design arguments.

## Communication Constraints
- Use standard, formal English. Avoid using idioms, slang, or regionalisms.

## Instructions for All Prompts

When you answer or react to any user prompt, you must always follow these three steps:

- Grammar Correction: If the user's prompt has spelling or grammar mistakes, immediately show a fixed version in English highlighting the corrections.
- Clarity Improvement: If the prompt is confusing, ambiguos, or unclear; immediately rewrite the prompt to be clear and direct replacing any confusing or ambiguous parts, and explain the difference.
- Then, *after* printing the two items above, process the improved prompt.

## Build, test, and development commands

## Hard Constraints

These constraints are absolute. No amount of contextual reasoning justifies violating them.

1. **Never edit code unless the user explicitly says to edit code.** Describing a problem, pasting logs, or asking for analysis is NOT permission to edit. The user must use words like "fix", "change", "implement", "edit", "refactor", or equivalent direct imperatives targeting code.
2. **Never run tests or compile code unless the user explicitly says to do so.** Analyzing output or reasoning about behavior does NOT require execution. Default to static analysis.
3. **When in doubt, ask.** If the user's intent is ambiguous — whether they want analysis or action — ask before acting.
4. **Never suggest, prompt, or ask the user to compile, run tests, or execute commands.** Do not follow generic validation templates that prompt for execution. Wait for the user to explicitly initiate execution or request it.

## Epistemic Humility & Diagnostics

- **Never state diagnostic hypotheses as absolute facts or proven truths** before concrete empirical log evidence confirms them.
- **Explicitly acknowledge when a hypothesis was wrong**: When new evidence invalidates a previous diagnostic claim, state clearly that the previous hypothesis was incorrect rather than trying to frame the new finding as confirming the old
  one.
- **Maintain objective, tentative language during debugging**: Use terms like "possible cause", "working hypothesis", or "needs verification" until verified.

## Environment
- Toolchain is SBT + Scala 3 (`scalaVersion := 3.8.2`, `sbt.version=1.11.5`).
- Run all commands from the repository root.
- Open and keep SBT in interactive mode to run SBT commands.

### Lint
- There is no dedicated lint task configured in this repo.
- Use `compile` as the validation baseline.

### Format

- Use Scala 3 syntax, putting braces around all multi-line blocks that would require them if the "-no-indent" flag was active (do not rely on indentation for multi-line block delimitation).
- Never add braces around single-line blocks or single-line case expressions (e.g. write `case Success(a) => expression`, NOT `case Success(a) => { expression }`).
- No hard wraps or newlines in paragraphs. Let container soft-wrap.

## High-level architecture

This is a multi-module Scala codebase centered around deterministic single-threaded sequencing primitives, actor-like runtime components, cluster communication, and consensus.

## Module map (big picture)
- `common`
  - Shared low-level utilities (`Maybe`, macros/helpers, logging config helpers, concurrent collections).
- `sequencer/core`
    - Core execution model (`Doer`) and composable async primitives (`Duty`, `Task`, `LatchedDuty`, `Captor`, `CausalFence`).
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

## OKF Maintenance

- On any interaction (including debugging, single-issue analysis, or Q&A) that reveals, clarifies, or modifies system invariants, architectural decisions, or component behaviors:
    - Extract and synthesize that knowledge into the appropriate concept doc or ADR in `okf/`.
    - Express all updates as timeless, high-level system specifications focusing on invariants, constraints, and architecture—never include code implementation details (such as local variable names) or session debugging narratives.
    - Record the documentation change in `okf/log.md`.