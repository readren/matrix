---
type: "Decision"
title: "Component Definition Module (CDM) Pattern for Service Decomposition"
description: "Architectural specification and design rationale for the Component Definition Module (CDM) pattern, enabling modular decomposition of complex service definitions while preserving path-dependent type unity and eliminating type parameter pollution."
tags: ["design-history", "consensus", "architecture"]
timestamp: "2026-09-22T22:50:00Z"
---

# ADR: Component Definition Module (CDM) Pattern for Service Decomposition

## Context

In complex distributed actor and consensus subsystems, modules often grow significantly in size and scope. Specifically, `ConsensusParticipantSdm` encapsulates the full lifecycle of a multi-raft consensus participant, including:
- Participant role transitions and message handling (follower, candidate, leader, retiree).
- Reactive lazy consensus election protocols (pre-vote state discovery and candidate voting quorums).
- Cluster electorate management across sole electorates and two-phase joint consensus transitions.
- Asynchronous log replication pipelines, log compaction, snapshot streaming, and causal fence mutations.

Traditional object-oriented decomposition approaches—such as extracting independent helper classes or pure functional components—face severe friction in Scala 3 when decomposing large Service Definition Modules (SDMs):

1. **Type Parameter Explosion**: The SDM defines multiple abstract member types (`ParticipantId`, `ClientCommand`, `ClientResponse`, `StorageRecord`, `SnapshotData`). Decomposed external components would each require generic type parameters for every abstract type they touch, polluting signatures across call sites.
2. **Path-Dependent Type Incompatibility**: The sequencing primitives (`Capture`, `Captor`, `CausalFence`) are path-dependent on the single sequencer instance (`sequencer: Doer`). If auxiliary components are instantiated as separate objects with their own sequencer references, the compiler cannot automatically unify path-dependent types (e.g., `this.sequencer.Capture[T]` vs `external.sequencer.Capture[T]`) without:
   - Introducing singleton type parameter bounds (`S <: Doer & Singleton`),
   - Or wrapping operations in runtime adapter conversions, both of which introduce cognitive overhead and performance penalties.
3. **Circular References and Coupling**: Auxiliary subsystems (such as quorum evaluation and candidate ranking) need to inspect peer progress and log terms without being coupled to the full state machine or internal lifecycle classes (`LearnerProgress`, `PrimaryState`).

## Decision

We introduce the **Component Definition Module (CDM)** pattern as the standardized architectural convention for decomposing large Service Definition Modules.

### 1. Structural Composition via Subtyping

A CDM is declared as a top-level trait (such as `ConsensusElectorateCdm`) that encapsulates a cohesive, bounded domain slice. The parent SDM (`ConsensusParticipantSdm`) directly extends the CDM trait:

```scala
trait ConsensusElectorateCdm:
  type ParticipantId <: AnyRef : {Ordering, ClassTag}
  val sequencer: Doer
  // Domain classes, traits, and algorithms nested within the CDM...

trait ConsensusParticipantSdm extends ConsensusElectorateCdm:
  // Full service lifecycle, state machine, and persistence bridges...
```

### 2. Elimination of Path-Dependent Type Friction

Because `ConsensusParticipantSdm` extends `ConsensusElectorateCdm`, both traits share the identical underlying singleton value `val sequencer: Doer`. All path-dependent types—such as `sequencer.Captor[T]` or `sequencer.Capture[T]`—are immediately and identically unified by the compiler without requiring singleton type parameters, auxiliary type equality proofs, or adapter allocations.

### 3. Abstract Member Types over Generic Type Parameters

Rather than parameterizing components with generic type arguments (`class Electorate[Id, Cmd, Resp, ...]`):
- The CDM declares only the minimal subset of abstract member types required for its sub-domain (`type ParticipantId <: AnyRef : {Ordering, ClassTag}`).
- The SDM provides or inherits the concrete bindings for these types.
- Subsystems within the CDM interact with member types naturally, avoiding type signature bloat across methods and classes.

### 4. Narrow Decoupling Interfaces (Views)

To prevent the CDM from depending on internal SDM structures, the CDM specifies minimal structural view traits for cross-boundary data queries:
- `PeerProgressView`: Exposes only the append watermark and retirement flag needed for quorum progress calculations, avoiding coupling to `LearnerProgress` replication buffers.
- `LogTermLookup`: Exposes only record term queries and snapshot index boundaries, avoiding coupling to the full `PrimaryState` or `CausalFence`.

Internal SDM classes implement these view traits, providing clean, unidirectional information flow without cyclic dependencies.

### 5. Domain Semantic Realignment

Types defined within the CDM represent the consensus voting and membership authorities directly as `Electorate`, subclassed by `SoleElectorate` (single stable consensus) and `JointElectorate` (two-phase transitional consensus). Aligning domain naming around the electorate concept eliminates semantic ambiguity between general cluster runtime settings and consensus voting membership, establishing unambiguous domain models (`Electorate`, `SoleElectorate`, `JointElectorate`, and `ElectorateChange`), with cluster participants modeled directly as `members`.

### 6. Horizontal CDM Composition via Self-Types

When decomposed CDMs depend on capabilities provided by other CDMs (for example, `ConsensusPrimaryStateCdm` requiring `LogTermLookup`, `ParticipantId`, and `sequencer` defined in `ConsensusElectorateCdm`), CDMs declare explicit self-type requirements rather than establishing deep vertical inheritance towers:

```scala
trait ConsensusPrimaryStateCdm { this: ConsensusElectorateCdm =>
  // Persistent state management, Workspace SPI, and PrimaryState transitions...
}
```

The parent SDM assembles these modular slices horizontally via flat mixin composition:

```scala
trait ConsensusParticipantSdm extends ConsensusElectorateCdm with ConsensusPrimaryStateCdm { thisModule =>
  // Coordinator, role behaviors, and event loop orchestration...
}
```

This preserves semantic honesty (`PrimaryState` is not an electorate; it requires an electorate execution context), keeps each CDM independently comprehended and tested, and exposes all component capabilities uniformly at the service assembly root.

### 7. Decoupling State Mutators from Coordination Lifecycles

State-managing CDMs (such as `ConsensusPrimaryStateCdm`) maintain pure state manipulation and persistence mathematics, completely isolated from actor execution lifecycles, role transitions (`become`), or coordinator identifiers (`boundParticipantId`):
- Pure Transition Pipeline: State transitions return `Capture[PrimaryState]` that execute persistence (`storage.save(workspace)`) and trap failures without introspecting or mutating active coordinator roles.
- Explicit Coordination Parameters: Methods that verify invariants against coordinator progress (such as suffix truncation bounds during record fusion) accept watermarks explicitly (e.g. `commitIndex: RecordIndex`), preventing implicit reading of mutable coordinator state.
- Boundary Failure Coordination: Persistence faults are intercepted and managed at the service boundary through decorating storage handles (or causal fence observers), transforming low-level I/O failures into deterministic lifecycle events (such as quiescence) without leaking runtime actor orchestration into persistence components.
