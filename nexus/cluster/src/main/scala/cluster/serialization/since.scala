package readren.nexus
package cluster.serialization

/** Indicates the [[ProtocolVersion]] at which a constructor parameter of a product class was introduced.
 *
 * Product fields annotated with `@since(V)` are only serialized when [[Serializer.Writer.governingVersion]] is at least `V`.
 * During deserialization, if [[Deserializer.Reader.governingVersion]] is older than `V`, the field is not read from the wire
 * and its default constructor argument value (`$lessinit$greater$default$n`) is substituted instead. */
final class since(val version: Int) extends scala.annotation.StaticAnnotation
