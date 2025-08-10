
ThisBuild / version := "0.1.1-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.1"

ThisBuild / libraryDependencies ++= Seq(
	"readren" %% "sequencer-core" % "0.3.0-SNAPSHOT",

	"com.outr" %% "scribe" % "3.17.0",
	"com.outr" %% "scribe-file" % "3.17.0",

	"org.typelevel" %% "scalacheck-effect" % "1.0.4" % Test,
	"org.typelevel" %% "scalacheck-effect-munit" % "1.0.4" % Test
)

ThisBuild / scalacOptions ++= Seq(
	"-preview",
	"-source:future",
//	"-language:strictEquality",
	"-experimental",
//	"-deprecation",
	"-feature",
	"-explain",
	"-Xcheck-macros",
//	"-Xprint:macros",         // Show macro expansions
//	"-Ydebug",               // Additional debug info
//	"-Yshow-tree-ids",       // Show tree identifiers
//	"-Yprint-debug",         // Debug printing
//	"-Ylog:macro-expansions" // Log macro expansions
)
lazy val common = (project in file("common"))
	.settings(
		name := "matrix-common",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val doerAssistantProviders = (project in file("dap")).dependsOn(common)
	.settings(
		name := "matrix-dap",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val cluster = (project in file("cluster")).dependsOn(common, doerAssistantProviders)
	.settings(
		name := "matrix-cluster",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val core = (project in file("core")).dependsOn(common, doerAssistantProviders)
	.settings(
		name := "matrix-core",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val checkedReactant = (project in file("checked")).dependsOn(core)
	.settings(
		name := "matrix-checked_reactant",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val consensus = (project in file("consensus"))
	.dependsOn(doerAssistantProviders % Test)
	.settings(
		name := "matrix-consensus",
		idePackagePrefix := Some("readren.matrix")
	)

// Root project - aggregates all subprojects
lazy val root = (project in file("."))
	.aggregate(common, doerAssistantProviders, core, checkedReactant, cluster, consensus)
	.settings(
		name := "matrix",
		idePackagePrefix := Some("readren.matrix")
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
