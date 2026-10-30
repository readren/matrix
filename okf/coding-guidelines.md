---
type: "Guideline"
title: "Coding Guidelines"
description: "Repository conventions for Scala 3 syntax, formatting, Scaladoc structure, and semantic naming patterns."
tags: ["contributor-guide", "guideline"]
timestamp: "2026-10-29T20:34:00Z"
---

# Coding Guidelines

This document specifies the coding conventions, syntax rules, formatting constraints, and semantic naming principles enforced across the Matrix codebase.

---

## 1. Toolchain & Compiler Environment

- **Compiler Baseline:** The repository uses Scala 3.8 (`scalaVersion := 3.8.4`) and SBT 1.11.
- **Compiler Flags & Strict Discipline:** Core modules configure `-language:strictEquality` and `-source:future` to enforce sound type comparisons and forward compatibility.
- **Compilation as Baseline Validation:** There is no standalone linter. Clean compilation under `sbt compile` serves as the baseline validation standard.

---

## 2. Syntax & Formatting Rules

### Explicit Bracing over Significant Indentation

To maintain unambiguous AST structure across refactorings and tooling, code must adhere to explicit block delimitation:

- **Multi-Line Blocks:** Multi-line blocks must use explicit braces `{ ... }` as would be required if the Scala `-no-indent` compiler option were active. Never rely on indentation whitespace alone for multi-line block delimitation.
- **Single-Line Blocks:** Never add braces around single-line blocks, single-line method bodies, or single-line pattern matching cases (e.g., write `case Success(a) => expression`, NOT `case Success(a) => { expression }`).
- **Control Flow Formatting:** For single-line `if/else`, omit braces. For multi-line branches, enclose each branch block in braces.

### Continuous Prose & Markdown Wrapping

- **No Manual Line Breaks in Prose:** Never insert manual line breaks (`\n`) within paragraphs, markdown documents, bullet items, or commit messages. Every paragraph, list item, or commit body entry must be emitted as a single continuous
  line, allowing viewing environments to soft-wrap naturally. Hard line breaks are reserved strictly for separating distinct paragraphs, list items, or code blocks.

---

## 3. Semantic Naming Principles

Code entities must be named according to semantic honesty, directionality, and pattern consistency:

### I. Semantic Honesty

- Entity names must truthfully reflect what an entity *is* or *does* from the perspective of its role and owner.
- Avoid misleading transitive verbs on recipient objects. For instance, an observer or host interface method must be named `onQuiesced` rather than `notifyQuiesced` when the recipient is the actor executing the reaction.

### II. Boundary Events (`on...`) vs. Internal Logic Handlers (`handle...`)

The codebase strictly separates boundary events from internal algorithm transitions:

- **`on...` (Boundary Crossings & Bridge APIs):** Used for methods that represent events crossing architectural boundaries (Consensus ↔ Cluster, Consensus ↔ Host Observers, or Remote ↔ Local). Examples:
    - *Inbound RPCs in Delegates:* `onHowAreYou`, `onAppendRecords`, `onCommandFromClient`.
    - *Outbound Host Notifications:* `onActiveElectorateChanged`, `onQuiesced`.
    - *Notification Observers:* `onBecameIsolated`, `onAbdicating`.
- **`handle...` (Internal State Transitions):** Used for internal lifecycle execution within role hierarchies and state machines. Examples:
    - *Lifecycle Transition Hooks:* `handleEnter`, `handleExit`.
    - *Internal Mutation Handlers:* `handleActiveElectorateChanged`.

### III. Symmetrical Relationship Mapping

- Entities that have reciprocal relationships must use balanced, symmetrical terms: `enter` / `exit`, `start` / `stop`, `open` / `close`.
- Patterns must maintain one-to-one mapping: never use two different naming patterns for the same kind of entity, and never overload one naming pattern across disparate concepts.

---

## 4. Scaladoc Formatting Rules

All public traits, classes, methods, and types must be documented using standardized Scaladoc comments conforming to the following structure:

1. **Inline Description Opening:** Place the main method or class description on the same single line immediately following the opening `/**` delimiter.
2. **Continuation Backslash for Tags:** If a `@param` or tag description spans multiple lines, end the preceding line with a backslash `\` to indicate continuation.
3. **No Indentation Alignment on Continuations:** Do not indent continuation lines to align with the parameter name; begin continuation lines directly after the ` * ` prefix (an asterisk followed by a single space).
4. **Inline Closing Delimiter:** Place the closing delimiter `*/` inline at the end of the final comment line rather than on its own dedicated line.
5. **Double Square Bracket References:** Always reference classes, traits, methods, or types using double brackets `[[Symbol]]` (e.g., `[[PrimaryState]]`, `[[Doer]]`) instead of backticks. When referencing members in other scopes, prefix
   the symbol with the defining type (e.g., `[[List.empty]]`) so that compilers and IDEs resolve the link correctly.

### Scaladoc Formatting Example

```scala
/** Applies the given [[ClientCommand]] to the state machine.\
 * @param index the sequential [[RecordIndex]] in the log
 * @param command the user command payload to apply\
 * to the underlying storage engine
 * @return a [[sequencer.Capture]] yielding the [[StateMachineResponse]] */
def applyClientCommand(index: RecordIndex, command: ClientCommand): sequencer.Capture[StateMachineResponse]
```
