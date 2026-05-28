---
name: semantic-naming
description: Core principles for semantic naming, focused on honesty, relationship mapping, and pattern consistency across the codebase.
---

# Semantic Naming Principles

This skill provides the foundational rules for naming entities in a way that is semantically honest, consistent, and architecturally clear.

## 1. Semantic Honesty

Names must accurately describe what a thing **is** or **does** from the perspective of its role and owner.

- Avoid misleading transitive verbs (e.g., use `onEvent` instead of `notifyEvent` when the recipient is the actor).
- Ensure the name aligns with the actual behavior and responsibility of the entity.

## 2. Explicit Relationship Mapping

When entities are related, their names should reflect that relationship, especially if the programming language does not make it explicit.

- **Opposites**: Use paired terms like `enter`/`exit`, `start`/`stop`, or `open`/`close`.
- **Symmetry**: If one side of a relationship follows a pattern, the other side should follow the corresponding inverse pattern.

## 3. Characteristic Differentiation

When names might otherwise collide, use the specific characteristics or contexts of the entities to differentiate them.

- **Example**: Use `handle...` for internal logic processing and `on...` for boundary-crossing event notifications.
- Differentiate by **intent** (what it's for), **direction** (where it's going), or **provenance** (where it came from).

## 4. Pattern Consistency

Use consistent lexical and structural patterns for related things.

- Once a pattern is established for a specific category of entities (e.g., "lifecycle hooks start with `handle`"), it must be applied universally to all members of that category.

## 5. One-to-One Pattern Mapping

There must be a strict, one-to-one relationship between a **naming pattern** and a **kind of thing**.

- Do not use the same pattern for two different concepts.
- Do not use two different patterns for the same concept.
- A specific prefix or suffix should uniquely identify the nature, boundary, or behavior of the named entity.
