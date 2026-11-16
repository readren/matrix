---
name: semantic-naming
description: Mandatory naming rules for semantic honesty, explicit domain roles, relationship mapping, and pattern consistency across code, tests, comments, and documentation. Trigger this skill before authoring or renaming any type, member field, method, parameter, test fixture, Scaladoc comment, or architectural specification.
---

# Semantic Naming Principles

This skill defines the mandatory rules for naming software entities and architectural concepts so that code, tests, comments, Scaladoc, and OKF documentation remain self-explanatory, structurally honest, and free of cryptic shorthand.

## 1. Semantic Honesty

Names must accurately describe what a thing **is** or **does** from the perspective of its domain role and owner, never from arbitrary implementation scaffolding or execution sequence.

- **Avoid misleading transitive verbs:** Use `onEvent` rather than `notifyEvent` when the entity is the recipient rather than the sender.
- **Align with actual responsibility:** Ensure the identifier reflects the concrete behavior and ownership boundary of the entity.
- **Domain Roles Over Ordinal Numbering:** Never label role-distinct pipeline stages, workflow phases, collaborators, or state machines by execution order (`Stage1`/`Stage2`, `Stage 1`/`Stage 2`, `Phase1`/`Phase2`, `Step1`/`Step2`, `First`/`Second`). Name each stage after the responsibility it performs (such as `validation` and `persistence`, or `requestParsing` and `responseDispatch`) so meaning remains explicit and resilient if another stage is inserted or reordered. Reserve ordinal numbers strictly for genuinely homogeneous, fungible elements (such as indexed array slots, tuple components, or numbered cluster replica IDs).

## 2. Explicitness Over Brevity (No Cryptic Abbreviations)

Clarity and immediate readability strictly override character brevity. Every non-trivial identifier must spell out the domain concept in full standard words unless an abbreviation is universally established as a proper term across the codebase and protocol specifications (such as `IO`, `NIO`, `VLQ`, `RPC`, or `FIFO`).

- **Ban cryptic contractions and alphanumeric tokens:** Never introduce shortened prefixes, initials, or numbered shorthands (`S1`, `S2`, `s1`, `s2`, `q`, `buf`, `mgr`, `ctx`, `cb`, `fn`, `tx`, `rx`) for types, opaque types, constants, member fields, method signatures, parameters, or test fixtures. For example, name a queue-length counter `pendingItemCount` rather than `q`, a configuration record `databaseConfig` rather than `cfg`, and state types `ValidationState` and `PersistenceState` rather than `S1` and `S2`.
- **Restrict single-letter and ultra-short names to trivial local scope:** Terse names (`i`, `k`, `idx`, or single-expression lambda parameters) are acceptable only for local loop indices, narrow mathematical expressions, or one-line lambdas where type and intent are immediately visible within a few lines.

## 3. Explicit Relationship Mapping

When entities are related, their names should reflect that relationship directly, especially when the type system does not enforce it.

- **Opposites:** Use paired terms such as `enter`/`exit`, `start`/`stop`, or `open`/`close`.
- **Symmetry:** When two or more subsystems mirror one another (for example, `inbound*` vs `outbound*`, or `encode*` vs `decode*`), every corresponding buffer ring, queue, failure slot, timeout, and callback field must follow an exact symmetric structure.

## 4. Characteristic Differentiation

When names might otherwise collide, differentiate them explicitly by their domain context, boundary, or role.

- **Boundary vs. internal processing:** Use `handle...` for internal logic processing and `on...` for boundary-crossing event notifications.
- **Differentiate by intent, direction, or provenance:** Distinguish collaborators by what they own or represent rather than appending arbitrary suffixes (`Handler1`, `Handler2`, `Aux`).

## 5. Pattern Consistency

Apply established lexical and structural patterns uniformly across every member of a conceptual family.

- **Forbid mixed prefix contraction:** Never alternate between an expanded form and an abbreviated form for the same concept within a file or module (for example, never mix `connectionTimeout` with `connBuffer`, `messageQueue` with `msgHandler`, or drop the category prefix unevenly across sibling members such as `timeoutMillis` alongside `connectionBuffer`). Choose one canonical domain prefix (such as `connection` or `inbound`) and apply it consistently across all fields, methods, helpers, local variables, and test fixtures.
- **Universal category rules:** Once a pattern is established for a category of entities (such as `lifecycle hooks start with handle`), apply it without exception to every member of that category.

## 6. One-to-One Pattern Mapping

Maintain a strict one-to-one mapping between a **naming pattern** and a **kind of thing**.

- Do not reuse the same naming pattern for two unrelated concepts.
- Do not split one concept across two competing naming patterns.
- A distinct prefix or suffix should uniquely identify the nature, stage, boundary, or lifecycle role of the named entity.

## 7. Vocabulary Alignment Across Comments, Tests, and Documentation

The same honest domain vocabulary required for code symbols applies equally to all surrounding prose and verification artifacts.

- **Scaladoc, inline comments, and assertions:** Never refer to code responsibilities using temporary shorthand or ordinal labels (`"Stage 1"`, `"Stage 2"`, `"S1"`, `"S2"`) in comments, log messages, test descriptions, or assertion failures.
- **OKF and architecture documentation:** Architecture documents (`doc/architecture/`) and ADRs must describe components using their true domain roles (`validation stage`, `persistence stage`) rather than working abbreviations or numbered stage labels.

## 8. Contrast Examples (Rejected vs. Accepted)

- **State & counter identifiers:** Reject `S1`, `S2`, `q`, `cfg`; use `ValidationState`, `PersistenceState`, `pendingItemCount`, `configuration`.
- **Multi-stage / sibling fields:** Reject `stage1Buffer`, `part1Payload`, `stage2Queue`, `p2Error`; use `parsingBuffer`, `parsingPayload`, `persistenceQueue`, `persistenceError`.
- **Methods & prose:** Reject `runStage1Loop`, `startPart2`, `"handled in Stage 1"`; use `runParsingLoop`, `persistRecord`, `"handled during parsing"`.
- **Mixed shorthand prefixes:** Reject mixing `connectionTimeout` with `connBuffer`; use `connectionTimeout` with `connectionBuffer`.
- **Test fixtures & harness helpers:** Reject `Stage1Fault`, `s1DelayMs`, `enteredStep1`; use `ParsingFault`, `parserDelayMillis`, `enteredParsing`.

## 9. Pre-Completion Naming Gate

Before completing any implementation, refactoring, or documentation task, inspect every newly introduced or modified identifier, Scaladoc comment, test fixture, and markdown entry against three checks: (1) Can any uninitiated reader tell what every type, field, and method represents without reading bitmasks or surrounding implementation code? (2) Are there any numbered stage/phase/step names (`1`, `2`, `First`, `Second`) where domain role names apply? (3) Are there any cryptic abbreviations or inconsistent prefix contractions across sibling members? If any check fails, rename the symbols and prose immediately before finishing.
