---
type: "How-To"
title: "Cluster Binary Serialization Developer Guide"
description: "Practical guide and mental model for cluster binary serializer and deserializer derivation, illustrating synthesized code across type structures, nesting modes, and discrimination criteria."
tags: ["user-guide", "nexus", "serialization", "cluster"]
timestamp: "2026-11-02T15:05:00Z"
---

# Cluster Binary Serialization Developer Guide

The cluster serialization module provides compile-time derivation for binary serialization and deserialization via `Serializer.derive[T]` and `Deserializer.derive[T]`. This guide walks through each supported type structure, demonstrates how serializers and deserializers are invoked, and provides the equivalent expanded code synthesized by the macros under different configurations. For the architectural specification and wire format invariants, see [Cluster Binary Serialization & Macro Derivation](cluster-serialization.md).

## 1. Quick Start & Basic Usage

Derivation requires importing the type classes and writer/reader primitives:

```scala
import cluster.serialization.{Serializer, Deserializer, Writer, Reader}

case class Ping(sequence: Long) derives Serializer, Deserializer
```

Alternatively, serializers and deserializers can be explicitly derived using inline methods:

```scala
given Serializer[Ping] = Serializer.derive[Ping]
given Deserializer[Ping] = Deserializer.derive[Ping]
```

## 2. Singletons (`case object` and Parameterless `enum` Cases)

Singletons carry no state. Serializing a singleton emits zero payload bytes, and deserializing it returns the singleton reference directly without reading from the stream.

```scala
sealed trait Signal
case object Ack extends Signal
case object Nack extends Signal
```

When deriving a serializer or deserializer directly for a singleton type (e.g., `Ack.type`), the macro synthesizes no-op operations:

```scala
// Synthesized Serializer[Ack.type]
new Serializer[Ack.type] {
  def serialize(message: Ack.type, writer: Writer): Unit = ()
}

// Synthesized Deserializer[Ack.type]
new Deserializer[Ack.type] {
  def deserialize(reader: Reader): Ack.type = Ack
}
```

## 3. Concrete Product Types

Concrete product types include standard `case class`es as well as standard classes whose primary constructor parameter names match accessible public fields or zero-argument getters. Fields are serialized sequentially in constructor parameter declaration order.

```scala
case class NodeEndpoint(host: String, port: Int)
```

The macro synthesizes field serialization using in-scope given serializers for each field type:

```scala
// Synthesized Serializer[NodeEndpoint]
new Serializer[NodeEndpoint] {
  def serialize(message: NodeEndpoint, writer: Writer): Unit = {
    summon[Serializer[String]].serialize(message.host, writer)
    summon[Serializer[Int]].serialize(message.port, writer)
  }
}

// Synthesized Deserializer[NodeEndpoint]
new Deserializer[NodeEndpoint] {
  def deserialize(reader: Reader): NodeEndpoint = {
    val host = summon[Deserializer[String]].deserialize(reader)
    val port = summon[Deserializer[Int]].deserialize(reader)
    new NodeEndpoint(host, port)
  }
}
```

Non-`case` classes with accessible getters are also supported:

```scala
class CustomMetric(val name: String, val count: Long)

// Supported because `name` and `count` are accessible public getters matching the primary constructor.
given Serializer[CustomMetric] = Serializer.derive[CustomMetric]
given Deserializer[CustomMetric] = Deserializer.derive[CustomMetric]
```

## 4. Parameterized Product Types

When deriving for an applied generic product type such as `Envelope[Payload]`, the macro substitutes the class's type parameters with the provided type arguments and summons serializers for the specialized field types:

```scala
case class Envelope[T](trackingId: Long, payload: T)

given Serializer[Envelope[String]] = Serializer.derive[Envelope[String]]
given Deserializer[Envelope[String]] = Deserializer.derive[Envelope[String]]
```

The synthesized code resolves the field serializer for the applied type argument (`String`):

```scala
// Synthesized Serializer[Envelope[String]]
new Serializer[Envelope[String]] {
  def serialize(message: Envelope[String], writer: Writer): Unit = {
    summon[Serializer[Long]].serialize(message.trackingId, writer)
    summon[Serializer[String]].serialize(message.payload, writer)
  }
}
```

## 5. Self-Recursive Types

Recursive data structures require special handling to avoid infinite macro expansion at compile time and excessive allocation at runtime. The derivation macro binds a local `given self = this` inside the method body.

### Direct Recursion

In directly recursive types, fields matching the enclosing type bypass implicit search and invoke `self` directly:

```scala
case class IntTree(value: Int, left: Option[IntTree], right: Option[IntTree])
```

```scala
// Synthesized Serializer[IntTree]
new Serializer[IntTree] {
  def serialize(message: IntTree, writer: Writer): Unit = {
    given self: Serializer[IntTree] = this
    summon[Serializer[Int]].serialize(message.value, writer)
    summon[Serializer[Option[IntTree]]].serialize(message.left, writer)
    summon[Serializer[Option[IntTree]]].serialize(message.right, writer)
  }
}
```

### Indirect Recursion via Containers

When self-recursion occurs through collections such as `List[RoseTree]` or `Option[RoseTree]`, the container serializer summons the local `self` instance in scope:

```scala
case class RoseTree(id: String, children: List[RoseTree])
```

```scala
// Synthesized Deserializer[RoseTree]
new Deserializer[RoseTree] {
  def deserialize(reader: Reader): RoseTree = {
    given self: Deserializer[RoseTree] = this
    val id = summon[Deserializer[String]].deserialize(reader)
    val children = summon[Deserializer[List[RoseTree]]].deserialize(reader) // uses `self`
    new RoseTree(id, children)
  }
}
```

## 6. Sealed Sum Types & Nesting Modes

For sealed hierarchies, the serializer prepends a variable-length quantity (VLQ) discriminator integer identifying the concrete variant, and the deserializer uses this discriminator to dispatch to the correct constructor.

### Flat Nesting Mode (Default: `NestedSumMatchMode.FLAT`)

By default, intermediate sealed traits are flattened into a single-level match with a single leading discriminator:

```scala
sealed trait Command
sealed trait AdminCommand extends Command
case class Shutdown(graceful: Boolean) extends AdminCommand
case class Rebalance(targetNode: Int) extends AdminCommand
case class UserAction(actionId: String) extends Command
```

Under `NestedSumMatchMode.FLAT`, the macro collapses the hierarchy into a single dispatch table:

```scala
// Synthesized Serializer[Command] in FLAT mode
new Serializer[Command] {
  def serialize(message: Command, writer: Writer): Unit = {
    given self: Serializer[Command] = this
    message match {
      case variant0: Shutdown =>
        writer.putUnsignedIntVlq(0)
        summon[Serializer[Boolean]].serialize(variant0.graceful, writer)
      case variant1: Rebalance =>
        writer.putUnsignedIntVlq(1)
        summon[Serializer[Int]].serialize(variant1.targetNode, writer)
      case variant2: UserAction =>
        writer.putUnsignedIntVlq(2)
        summon[Serializer[String]].serialize(variant2.actionId, writer)
    }
  }
}

// Synthesized Deserializer[Command] in FLAT mode
new Deserializer[Command] {
  def deserialize(reader: Reader): Command = {
    given self: Deserializer[Command] = this
    reader.getUnsignedIntVlq match {
      case 0 =>
        val graceful = summon[Deserializer[Boolean]].deserialize(reader)
        new Shutdown(graceful)
      case 1 =>
        val targetNode = summon[Deserializer[Int]].deserialize(reader)
        new Rebalance(targetNode)
      case 2 =>
        val actionId = summon[Deserializer[String]].deserialize(reader)
        new UserAction(actionId)
      case other =>
        throw new SerializationException(s"Unknown discriminator $other for Command")
    }
  }
}
```

### Diamond Inheritance Deduplication in Flat Mode

When a variant extends multiple intermediate sum traits, flattened traversal deduplicates variants by type equality (`=:=`), guaranteeing that each leaf type receives exactly one discriminator and one reachable branch:

```scala
sealed trait ProtocolMessage
sealed trait Request extends ProtocolMessage
sealed trait NonResponse extends ProtocolMessage

case class LeadRequest(term: Long) extends Request, NonResponse
case class Heartbeat(senderId: Int) extends NonResponse
```

`LeadRequest` appears only once in the synthesized match expression:

```scala
// Synthesized Serializer[ProtocolMessage] with diamond deduplication
message match {
  case variant0: LeadRequest =>
    writer.putUnsignedIntVlq(0)
    summon[Serializer[Long]].serialize(variant0.term, writer)
  case variant1: Heartbeat =>
    writer.putUnsignedIntVlq(1)
    summon[Serializer[Int]].serialize(variant1.senderId, writer)
}
```

### Tree Nesting Mode (`NestedSumMatchMode.TREE`)

When derived with `NestedSumMatchMode.TREE`, the macro emits a discriminator at each level of the inheritance tree:

```scala
import cluster.serialization.NestedSumMatchMode

given Serializer[Command] = Serializer.derive[Command](NestedSumMatchMode.TREE)
given Deserializer[Command] = Deserializer.derive[Command](NestedSumMatchMode.TREE)
```

The wire format produces nested discriminators corresponding to hierarchy depth:

```scala
// Synthesized Serializer[Command] in TREE mode
message match {
  case admin: AdminCommand =>
    writer.putUnsignedIntVlq(0) // outer discriminator for AdminCommand
    admin match {
      case variant0: Shutdown =>
        writer.putUnsignedIntVlq(0) // inner discriminator for Shutdown
        summon[Serializer[Boolean]].serialize(variant0.graceful, writer)
      case variant1: Rebalance =>
        writer.putUnsignedIntVlq(1) // inner discriminator for Rebalance
        summon[Serializer[Int]].serialize(variant1.targetNode, writer)
    }
  case user: UserAction =>
    writer.putUnsignedIntVlq(1) // outer discriminator for UserAction
    summon[Serializer[String]].serialize(user.actionId, writer)
}
```

## 7. Custom Discriminators with `DiscriminationCriteria`

### Default Ordinal Assignment

In the absence of a `DiscriminationCriteria` in scope, discriminators are assigned sequentially (`0, 1, 2, ...`) based on encounter order during hierarchy traversal.

### Custom Match-Type Assignment

To maintain backwards wire compatibility or assign explicit numerical codes to variants, define a `DiscriminationCriteria` using a Scala 3 match type:

```scala
import cluster.serialization.DiscriminationCriteria

sealed trait Status
case object Pending extends Status
case object Active extends Status
case object Suspended extends Status

given DiscriminationCriteria[Status] {
  type Discriminator[P <: Status] <: Int = P match {
    case Pending.type => 10
    case Active.type => 20
    case Suspended.type => 30
  }
}

given Serializer[Status] = Serializer.derive[Status]
given Deserializer[Status] = Deserializer.derive[Status]
```

The macro evaluates the match type at compile time to resolve the constant for each branch:

```scala
// Synthesized Deserializer[Status] with custom criteria
reader.getUnsignedIntVlq match {
  case 10 => Pending
  case 20 => Active
  case 30 => Suspended
  case other => throw new SerializationException(s"Unknown discriminator $other for Status")
}
```

### Contravariant Criteria Inheritance

Because `DiscriminationCriteria[-S]` is contravariant, criteria defined for a root trait or wildcard parameterized type automatically apply to subtypes and specialized instantiations unless explicitly overridden:

```scala
sealed trait BaseEvent
given DiscriminationCriteria[BaseEvent] {
  type Discriminator[P <: BaseEvent] <: Int = P match {
    case Ping => 1
    case Pong => 2
  }
}

// Any subtype or sub-hierarchy inherits the base criteria
given Serializer[BaseEvent] = Serializer.derive[BaseEvent]
```

### Criteria Interaction with `NestedSumMatchMode`

Whether intermediate sealed traits require discriminator entries in `DiscriminationCriteria` depends directly on `NestedSumMatchMode`:

- **Flat Mode (`NestedSumMatchMode.FLAT`)**: Intermediate sum traits are transparent and do not emit a discriminator. Only concrete leaf variants (`Product` and `Singleton`) require discriminators. Any `DiscriminationCriteria` defined at the outer level must match the leaf variants directly, not intermediate traits. If an intermediate trait has its own `given DiscriminationCriteria[NestedSum]`, the macro uses it for the nested trait's descendants, overriding the outer criteria.
- **Tree Mode (`NestedSumMatchMode.TREE`)**: Every level in the inheritance hierarchy emits a discriminator byte. Therefore, both intermediate sum traits and leaf variants require discriminators. You can satisfy this requirement using a unified outer criteria that matches both intermediate traits and leaf types, or by defining separate criteria for each hierarchy level.

```scala
sealed trait Thing
case class Rare(description: String) extends Thing
sealed trait CarPart extends Thing
case class Wheel(diameter: Int) extends CarPart
case class Motor(power: String) extends CarPart

// Unified criteria covering both levels under TREE mode:
given DiscriminationCriteria[Thing] {
  type Discriminator[V <: Thing] <: Int = V match {
    case Rare => 10
    case CarPart => 11   // Consumed by outer discriminator match in TREE mode
    case Wheel => 111    // Consumed by inner discriminator match in TREE mode
    case Motor => 112    // Consumed by inner discriminator match in TREE mode
  }
}
```

### Composing Mixed Modes via Intermediate Type-Class Instances

When an intermediate sum trait has an explicit `given Serializer` and `Deserializer` in scope, flattened traversal halts at that intermediate trait, regardless of the outer derivation's `NestedSumMatchMode`. This provides a clean composition boundary where the outer and inner sum types can each configure their own nesting mode and their own `DiscriminationCriteria`:

1. **Outer sum discriminator**: The outer `DiscriminationCriteria[OuterSum]` must assign a discriminator to the intermediate sum trait itself (e.g., `CarPart`), since the outer sum treats `CarPart` as an opaque variant.
2. **Inner sum delegation**: When serializing an instance of `CarPart`, the outer serializer writes the outer discriminator for `CarPart` and delegates to the in-scope `Serializer[CarPart]`.
3. **Independent inner mode**: The inner `Serializer[CarPart]` executes according to its own configured `NestedSumMatchMode` (e.g., `TREE`) and queries its own `DiscriminationCriteria[CarPart]`.

```scala
import cluster.serialization.NestedSumMatchMode.{FLAT, TREE}

// 1. Inner sum type configured with TREE mode and its own criteria:
given DiscriminationCriteria[CarPart] {
  type Discriminator[V <: CarPart] <: Int = V match {
    case Wheel => 1
    case Motor => 2
  }
}
given Serializer[CarPart] = Serializer.derive[CarPart](TREE)
given Deserializer[CarPart] = Deserializer.derive[CarPart](TREE)

// 2. Outer sum criteria assigns a discriminator to CarPart itself:
given DiscriminationCriteria[Thing] {
  type Discriminator[V <: Thing] <: Int = V match {
    case Rare => 10
    case CarPart => 20   // Discriminator identifying the CarPart branch to the outer serializer
  }
}

// 3. Outer sum type configured with FLAT mode:
given Serializer[Thing] = Serializer.derive[Thing](FLAT)
given Deserializer[Thing] = Deserializer.derive[Thing](FLAT)
```

In this configuration, serializing `Rare("sample")` emits the single discriminator `10` followed by the string payload. Serializing `Wheel(18)` emits the outer discriminator `20` (written by `Thing`'s serializer) followed immediately by the inner discriminator `1` (written by `CarPart`'s tree serializer) and the diameter integer `18`. Deserialization follows the exact reverse path, dispatching on `20` to invoke `Deserializer[CarPart]`.