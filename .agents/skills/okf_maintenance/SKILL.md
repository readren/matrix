---
name: okf-maintenance
description: Detailed guidelines, schema, and procedures for maintaining the project's Open Knowledge Format (OKF) bundle.
---

# OKF Maintenance Guidelines

This skill defines the schema, templates, and procedures for maintaining the Open Knowledge Format (OKF) bundle in the `okf/` directory of the repository. The OKF bundle serves as the single source of truth for the project's persistent
documentation, acting as a User/Developer Guide, Design History (ADRs), Operations Bundle, and Contributor Guide.

## Naming & Path Constraints

- **Directory**: All files must live in `okf/` at the repository root.
- **Filenames**: Must be lowercase kebab-case (e.g., `sequencer-core-architecture.md`).
- **Links**: Use relative markdown links without absolute URLs (e.g., `[Core Architecture](sequencer-core-architecture.md)`).

## Document Schema

Every OKF document must begin with a YAML frontmatter block:

```yaml
---
type: "TypeString"
title: "Descriptive Title"
description: "Brief summary of the concept."
tags: ["category-tag", "component-tag", "sub-tag"]
timestamp: "YYYY-MM-DDTHH:MM:SSZ"
---
```

### Supported Types

1. **User/Developer Guide**:
    - `"Concept"`: High-level design patterns, system architecture, and domain terminology.
    - `"Tutorial"`: Step-by-step learning walkthroughs for beginners.
    - `"How-To"`: Task-oriented recipes solving specific integration problems.
    - `"Reference"`: Dry, technical specifications of components, APIs, networks, and formats.
2. **Design History**:
    - `"Decision"`: Architectural Decision Records (ADRs) capturing context, options, and consequences.
3. **Operations**:
    - `"Runbook"`: Maintenance, deployment, monitoring, and troubleshooting instructions.
4. **Contributor Guide**:
    - `"Guideline"`: Build guidelines, test commands, style rules, and workflow constraints.
5. **Special**:
    - `"Index"`: Reserved for `okf/index.md`.
    - `"Log"`: Reserved for `okf/log.md`.

## Categorization & Interconnection

To map relationships between files, use the following frontmatter categorization rules:

### 1. Mandatory Category Tags

Every document must include at least one category tag in its `tags` array:

- `"user-guide"`: Documents guiding developers integrating/using the system.
- `"design-history"`: Documents chronicling architecture decisions (ADRs).
- `"operations"`: Documents detailing runtime deployment, scaling, or debugging.
- `"contributor-guide"`: Documents defining guidelines, style rules, or environments.

### 2. Component Tags

Apply one or more module tags to link documents to codebase modules:

- `"common"`, `"sequencer"`, `"nexus"`, `"consensus"`

### 3. Cross-Linking

- Every file must reference related files using relative markdown links (e.g., `[Consensus Component](consensus-component.md)`).
- `okf/index.md` must categorize all documents under their respective guide sections (User Guide, Design History, Operations, Contributor Guide).

## Procedures

### 1. Initialization

If the `okf/` directory or its base files do not exist, create `index.md` and `log.md` immediately using the standard templates.

### 2. Synchronization on Interaction

Before concluding any analysis, research, or implementation task:

1. Identify if new persistent knowledge was generated (e.g. stable component specs, API constraints, ADRs, runbooks, or guidelines).
2. Create or update the corresponding `.md` files under `okf/`.
3. Update the `timestamp` in the frontmatter of any modified files.
4. Record the changes in `okf/log.md` and update `okf/index.md` if new files were added.

### 3. Rule of Persistent-Only Information

- **Knowledge Extraction from Every Session**: Debugging, log analysis, single-issue analysis, and Q&A sessions frequently reveal unwritten invariants, edge cases, and state machine rules. Always synthesize and extract that knowledge into
  substantive `okf/` documents (e.g. concept files, ADRs, or reference specifications).
- **Invariant Focus**: Write only about the current stable architecture, constraints, API protocols, and guidelines. Express documentation in terms of high-level concepts and system dynamics—never include code implementation details, or
  internal variable names.
- **No Bug Diaries**: Never document individual bugs, transient code fixes, specific log filenames (e.g. `scratch_2.log`), line-number stack traces, or session action logs (e.g., "Analyzed error in scratch_2.log", "Formulated suggestions
  during session"). Always phrase updates as timeless documentation of system architecture and component behaviors.
- **Log Scope**: `okf/log.md` must describe what system documentation was created or updated (e.g., "Documented retirement snapshot fallback invariants in `okf/lazy-multi-raft-consensus.md`"), never the history of conversational actions,
  code fixes, or git commits.