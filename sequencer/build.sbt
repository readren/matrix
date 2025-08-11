ThisBuild / version := "0.3.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.1"

val AkkaVersion = "2.10.1"

// Core library project without Akka dependencies
lazy val core = (project in file("core"))
	.settings(
		name := "sequencer-core",
		organization := "readren",
		idePackagePrefix := Some("readren.sequencer"),
		libraryDependencies ++= Seq(
			"org.scalatest" %% "scalatest" % "3.2.19" % Test,
			"org.scalacheck" %% "scalacheck" % "1.18.1" % Test,
			"org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
			"org.typelevel" %% "scalacheck-effect" % "1.0.4" % Test,
			"org.typelevel" %% "scalacheck-effect-munit" % "1.0.4" % Test
		)
	)

// Akka integration project that depends on `kernel` and includes Akka dependencies
lazy val akkaIntegration = (project in file("akka-integration"))
	.dependsOn(core)
	.settings(
		name := "sequencer-akka-integration",
		organization := "readren",
		idePackagePrefix := Some("readren.sequencer.akka"),
		resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
		libraryDependencies ++= Seq(
			"ch.qos.logback" % "logback-classic" % "1.5.17",
			"com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
			"com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
			"com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
		)
	)

// Root project to aggregate both subprojects
lazy val root = (project in file("."))
	.aggregate(core, akkaIntegration)
	.settings(
		name := "sequencer",
		organization := "readren",
		idePackagePrefix := Some("readren.sequencer")
	)

ThisBuild / scalacOptions ++= Seq(
	"-preview",
	"-experimental", // required because "ToolsMacro.scala" uses the method [[Symbol.info]] which is experimental.
	"-deprecation",
	"-feature",
	"-explain",
	"-language:strictEquality",
	"-Xcheck-macros", // This flag enables extra runtime checks that try to find ill-formed trees or types as soon as they are created.
	"-Ycheck:all", // This flag checks all compiler invariants for tree well-formedness. These checks will usually fail with an assertion error.
	// "-Xprint:macro", // Prints all compilation phases (including macro transformations)
)
//scalacOptions ++= Seq(
//    "-Xprint:typer", // Prints the state of the code after type checking
//    "-Xprint:all"    // Prints all compilation phases (including macro transformations)
//)


