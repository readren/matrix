---
type: "Concept"
title: "Common Primitive Wrappers"
description: "Semantics and invariants of low-level wrapper primitives in the common module, including Maybe and Trial."
tags: ["common", "primitives", "maybe", "trial"]
timestamp: "2026-06-26T01:42:00Z"
---

# Common Primitive Wrappers

This document outlines the semantics, design constraints, and known limitations of the low-level value class wrapper primitives defined in the `common` module.

## Maybe[+A]

`Maybe` is a value class representation of `A | Null` that avoids allocating wrapper objects for non-empty values at runtime.

- **Empty State**: Represented internally by wrapping a `null`.
- **Defined State**: Any non-null value of type `A`.
- **Allocation**: Zero allocation when wrapping reference types, though primitive types will box.

## Trial[+A]

`Trial` is a value class designed to represent the outcome of a computation, supporting three states:

1. **Empty**: Internally represented as wrapping `null`.
2. **Success**: Internally represented as wrapping a non-null value that is not an instance of `Trial.Failure`.
3. **Failure**: Internally represented as wrapping `Trial.Failure(exception)`.

### Invariants and Limitations

- **No Nested Failure Collision**: Because `Trial.Failure` is defined as `private` within `object Trial`, it cannot be referenced or wrapped directly as a success value by external code. This prevents the nested collision issue.
- **Comparison/Equality**:
    - `equals` and `isEqualTo` must compare raw values using an `eq null` check combined with `.equals` (instead of `==` which is blocked by strict equality, or direct `.equals` on `raw` which throws `NullPointerException` when empty). This
      supports comparing empty trials and compares failures via case-class equality of `Trial.Failure`.
