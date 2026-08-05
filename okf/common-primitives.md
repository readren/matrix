---
type: "Concept"
title: "Common Primitive Wrappers"
description: "Semantics and invariants of low-level wrapper primitives in the common module, including Maybe and Trial."
tags: ["common", "primitives", "maybe", "trial"]
timestamp: "2026-08-02T13:31:00Z"
---

# Common Primitive Wrappers

This document outlines the semantics, design constraints, and known limitations of the low-level wrapper primitives defined in the `common` module.

## Maybe[+A]

`Maybe` is an `opaque type Maybe[+A] = A | Null` representation that avoids allocating wrapper objects for non-empty values at runtime.

- **Empty State**: Represented internally as `null`.
- **Defined State**: Any non-null value of type `A`.
- **Allocation**: Zero allocation for reference types.
- **Anti-Nesting Guards**: Uses Scala 3 Quote Macros (`MaybeMacros`) to statically block nullable union types (`T | Null`) and nested `Maybe` types at compile time across both factory methods (`apply`, `some`, `liftPartialFunction`) and
  transformation methods (`map`, `flatMap`). Because `Maybe` is an `opaque type`, its underlying union representation (`A | Null`) is hidden outside `object Maybe`, meaning macro pattern matching on `OrType` does not automatically match
  `Maybe[T]` terms unless subtype checks against `Maybe[?]` are explicitly performed.

## Trial[+A]

`Trial` is a value class designed to represent the outcome of a computation, supporting three states:

1. **Empty**: Internally represented as wrapping a dedicated sentinel object `Trial.EMPTY_INTERNAL`.
2. **Success**: Internally represented as wrapping any value (including `null` or `Maybe.empty`) that is not `Trial.EMPTY_INTERNAL` and not an instance of `Trial.Failure_Internal`.
3. **Failure**: Internally represented as wrapping `Trial.Failure_Internal(exception)`.
