---
name: scaladoc-formatting
description: Rules and formatting conventions for writing Scaladoc comments in this repository.
---

# Scaladoc Formatting Rules

Follow these rules for writing and formatting Scaladoc comments in this repository:

1. Main Description: Place the main method description on a single line immediately following the opening `/**`.
2. Parameter Tag Continuation: If a `@param` description spans multiple lines, end the first line of the parameter tag with a backslash `\` to indicate continuation.
3. No Indentation Alignment: Do not indent subsequent/continuation lines of a parameter description to align with the parameter name; start them directly after the ` * ` prefix (asterisk followed by a single space).
4. Inline Closing Tag: Place the closing tag `*/` inline at the end of the final comment line rather than on a new line.
5. References: Always use double square brackets `[[Symbol]]` (e.g., `[[List]]`) to reference classes, traits, methods, or types instead of backticks. If the referenced member belongs to another class or extension, prefix it with the
   defining class name (e.g., `[[List.empty]]` instead of `[[empty]]`) so that compilers and IDEs can resolve the link.
