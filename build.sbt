
ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "3.8.2"

ThisBuild / libraryDependencies ++= Seq(
	// "readren" %% "sequencer-core" % "0.3.0-SNAPSHOT",

	"com.outr" %% "scribe" % "3.18.0",
	"com.outr" %% "scribe-file" % "3.18.0",

	"org.typelevel" %% "scalacheck-effect" % "2.1.0" % Test,
	"org.typelevel" %% "scalacheck-effect-munit" % "2.1.0" % Test,
)

ThisBuild / scalacOptions ++= Seq(
	"-preview",
//	"-source:future",
//	"-language:strictEquality",
	"-experimental",
//	"-deprecation",
	"-feature",
	"-explain",
	//	"-Xcheck-macros",			// This flag enables extra runtime checks that try to find ill-formed trees or types as soon as they are created.
	// "-Yexplicit-nulls"
)

// These two lines are required for the "-ea" VM option to work when running with SBT. Note that this does not affect IntelliJ's run configurations. Use add "-ea" to the run-configurations' VM options to achieve the same.
ThisBuild / fork := true
ThisBuild / javaOptions ++= Seq("-ea")

// Avoids the key not used warning for `idePackagePrefix`.
Global / excludeLintKeys += idePackagePrefix

// registers the test framework
// ThisBuild / testFrameworks += new TestFramework("munit.Framework")

lazy val common = (project in file("common"))
	.settings(
		name := "common",
		idePackagePrefix := Some("readren.common"),
		scalacOptions ++= Seq("-source:future", "-language:strictEquality")
	)

lazy val sequencerCore = (project in file("sequencer/core"))
	.dependsOn(common)
	.settings(
		name := "sequencer_core",
		idePackagePrefix := Some("readren.sequencer"),
		scalacOptions ++= Seq("-language:strictEquality")
	)

lazy val sequencerProviders = (project in file("sequencer/providers"))
	.dependsOn(sequencerCore % "compile->compile;test->test")
	.settings(
		name := "sequencer_providers",
		idePackagePrefix := Some("readren.sequencer"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val sequencerProvidersManager = (project in file("sequencer/providers-manager"))
	.dependsOn(sequencerProviders)
	.settings(
		name := "sequencer_providers-manager",
		idePackagePrefix := Some("readren.sequencer"),
		scalacOptions ++= Seq("-source:future")
	)

val AkkaVersion = "2.10.9"

ThisBuild / resolvers += "akka-secure-mvn" at "https://repo.akka.io/etrfSax3No5yDclhqKsWorQ2woYHeQyiMUw8j2voy0hIYsT2/secure"
ThisBuild / resolvers += Resolver.url("akka-secure-ivy", url("https://repo.akka.io/etrfSax3No5yDclhqKsWorQ2woYHeQyiMUw8j2voy0hIYsT2/secure"))(Resolver.ivyStylePatterns)

lazy val sequencerAkkaIntegration = (project in file("sequencer/akka-integration"))
	.dependsOn(sequencerCore, common)
	.settings(
		name := "sequencer_akka-integration",
		idePackagePrefix := Some("readren.sequencer.akka"),
		scalacOptions ++= Seq("-language:strictEquality", "-source:future"),
		resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
		libraryDependencies ++= Seq(
			"ch.qos.logback" % "logback-classic" % "1.5.18",
			"com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
			"com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
			"com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
		)
	)

lazy val nexusCore = (project in file("nexus/core"))
	.dependsOn(sequencerCore, sequencerProviders, sequencerProvidersManager)
	.settings(
		name := "nexus_core",
		idePackagePrefix := Some("readren.nexus"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val checkedSpuron = (project in file("nexus/checked-spuron"))
	.dependsOn(nexusCore)
	.settings(
		name := "nexus_checked-spuron",
		idePackagePrefix := Some("readren.nexus"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val cluster = (project in file("nexus/cluster"))
	.dependsOn(common % "compile->compile;test->test", sequencerCore, sequencerProviders)
	.settings(
		name := "nexus_cluster",
		idePackagePrefix := Some("readren.nexus"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val consensus = (project in file("consensus"))
	.dependsOn(sequencerCore, sequencerProviders % Test)
	.settings(
		name := "consensus",
		idePackagePrefix := Some("readren.consensus"),
		scalacOptions ++= Seq("-source:future")
	)

// Root project - aggregates all subprojects
lazy val root = (project in file("."))
	.aggregate(common, sequencerCore, sequencerProviders, sequencerAkkaIntegration, nexusCore, checkedSpuron, cluster, consensus)
	.settings(
		name := "matrix",
	)

enablePlugins(DockerPlugin)

docker / dockerfile := {
	val jarFile: File = (Compile / packageBin / sbt.Keys.`package`).value
	val classpath = (Compile / managedClasspath).value
	val mainclass = (Compile / packageBin / mainClass).value.getOrElse(sys.error("Expected exactly one main class"))
	val jarTarget = s"/app/${jarFile.getName}"
	// Make a colon separated classpath with the JAR file
	val classpathString = classpath.files.map("/app/" + _.getName).mkString(":") + ":" + jarTarget
	new Dockerfile {
		// Base image
		from("amazoncorretto:17-alpine")
		// Add all files on the classpath
		copy(classpath.files, "/app/")
		// Add the JAR file
		copy(jarFile, jarTarget)
		// On launch run Java with the classpath and the main class
		entryPoint("java", "-cp", classpathString, mainclass)
		expose(5000, 5001, 5002) // Expose ports
	}
}
