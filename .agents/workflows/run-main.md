---
description: Automatically discover and run a @main method in the current project.
---

1. List all `@main` methods in the project by scanning for the annotation.
2. Filter by the module relevant to the current project or context.
3. Prompt the user for which one to run if multiple exist.
// turbo
4. Execute `sbt "{subproject} / runMain {fullClassName}.{methodName}"` or `sbt "{subproject} / runMain {mainClassName}"` depending on the Scala 3 entry point style.
