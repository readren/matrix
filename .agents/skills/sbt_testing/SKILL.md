---
name: sbt-interactive-testing
description: Provides instructions for executing tests in sbt interactive mode. Use this skill when the user explicitly commands to 'run tests' or 'test'. Do NOT trigger for general questions about Scala testing.
---
# sbt Interactive Testing

Use this skill when you need to execute Scala or Java tests efficiently within an active sbt session to avoid JVM startup overhead.

## Activation Criteria
- **Restriction**: This skill MUST NOT be used automatically or as a background task.
- **Permission**: Only activate this skill if the user provides an explicit imperative command.
- **Trigger Phrases**: Direct commands including "test", "run tests", "execute tests", or "run sbt tests".
- **Confirmation**: If the user's intent is ambiguous, ask for permission before running tests.

## Environment
- SBT + Scala 3 (`scalaVersion := 3.8.3`, `sbt.version=1.11.5`).

## 1. Entering Interactive Mode
To start the sbt shell, execute `sbt` in the project root without any arguments.
- **Trigger**: The prompt will change to `> ` or `[project-name] $ `.
- Keep the sbt shell open to reuse it until you finish whatever you are doing.

## 2. Testing Command
Once inside the shell, use this command to run a test:
- `testOnly <classFullName> -- -z "*<testName>"`: Runs a specific test defined in a test suite.

## 3. Shell Navigation Tips
- **Tab Completion**: Hit `<TAB>` to see available test class names.
- **History**: Use the Up/Down arrows to cycle through previous commands, or type `!` to see command history.
- **Exit**: Type `exit` or use `Ctrl+D` to leave the shell.

## Examples
- `testOnly *MyTestSuite -- z "*All invariants special case"` (Runs the test named "All invariants special case" in test suite classes whose name ends with "MyTestSuite").
