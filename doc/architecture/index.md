---
type: "Index"
title: "Matrix Project Knowledge Base"
description: "Index of all architectural and operational specifications."
tags: ["index", "documentation"]
timestamp: "2026-11-02T13:45:00Z"
---

# Matrix Project Knowledge Base

## Core Architecture

- [Multi-Raft System Architecture](system-architecture.md)
- [Lazy Raft Consensus Protocol Specification](lazy-multi-raft-consensus.md)

## Components

- [Sequencer Core](sequencer-core.md)
- [Cluster Binary Serialization & Macro Derivation](cluster-serialization.md)
- [Cluster Binary Serialization Developer Guide](cluster-serialization-guide.md)
- [Consensus Simulation & Test Framework](consensus-test-framework.md)

## Guidelines

- [Coding Guidelines](coding-guidelines.md)
- [Common Primitives](common-primitives.md)

## Design Decisions (ADRs)

- [CausalFence and the Decoupled Mutation Contract](adr-causal-fence-state-synchronization.md)
- [Component Definition Module (CDM) Pattern for Service Decomposition](adr-component-definition-module-pattern.md)
- [Design of Scheduling and Timing Extensions](adr-scheduling-subscription-design.md)
- [Design Discussion Transcript: Lazy Multi-Raft](transcript-lazy-multi-raft-design.md)
- [Simplify Sequencer Error Handling](adr-simplify-error-handling.md)
- [Evaluation of Spare-Slot Pattern](adr-spare-slot-pattern-evaluation.md)