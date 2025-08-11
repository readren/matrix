
ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.1"

ThisBuild / libraryDependencies ++= Seq(
	// "readren" %% "sequencer-core" % "0.3.0-SNAPSHOT",

	"com.outr" %% "scribe" % "3.17.0",
	"com.outr" %% "scribe-file" % "3.17.0",

	"org.typelevel" %% "scalacheck-effect" % "1.0.4" % Test,
	"org.typelevel" %% "scalacheck-effect-munit" % "1.0.4" % Test
)

ThisBuild / scalacOptions ++= Seq(
	"-preview",
//	"-source:future",
//	"-language:strictEquality",
	"-experimental",
//	"-deprecation",
	"-feature",
	"-explain",
	"-Xcheck-macros",			// This flag enables extra runtime checks that try to find ill-formed trees or types as soon as they are created.
)

lazy val sequencerCore = (project in file("sequencer/core"))
	.settings(
		name := "sequencer-core",
		idePackagePrefix := Some("readren.sequencer"),
		scalacOptions ++= Seq("-language:strictEquality")
	)

val AkkaVersion = "2.10.1"

lazy val sequencerAkkaIntegration = (project in file("sequencer/akka-integration")).dependsOn(sequencerCore)
	.settings(
		name := "sequencer-akka_integration",
		idePackagePrefix := Some("readren.sequencer"),
		scalacOptions ++= Seq("-language:strictEquality"),
		resolvers += "Akka library repository".at("https://repo.akka.io/maven"),
		libraryDependencies ++= Seq(
			"ch.qos.logback" % "logback-classic" % "1.5.18",
			"com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
			"com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
			"com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
		)
	)

lazy val common = (project in file("common"))
	.settings(
		name := "matrix-common",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val doerAssistantProviders = (project in file("dap")).dependsOn(common, sequencerCore)
	.settings(
		name := "matrix-dap",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val cluster = (project in file("cluster")).dependsOn(common % "compile->compile;test->test", sequencerCore, doerAssistantProviders)
	.settings(
		name := "matrix-cluster",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val core = (project in file("core")).dependsOn(common, sequencerCore, doerAssistantProviders)
	.settings(
		name := "matrix-core",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val checkedReactant = (project in file("checked")).dependsOn(core, sequencerCore)
	.settings(
		name := "matrix-checked_reactant",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

lazy val consensus = (project in file("consensus"))
	.dependsOn(sequencerCore,doerAssistantProviders % Test)
	.settings(
		name := "matrix-consensus",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
	)

// Root project - aggregates all subprojects
lazy val root = (project in file("."))
	.aggregate(sequencerCore, sequencerAkkaIntegration, common, doerAssistantProviders, core, checkedReactant, cluster, consensus)
	.settings(
		name := "matrix",
		idePackagePrefix := Some("readren.matrix"),
		scalacOptions ++= Seq("-source:future")
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
