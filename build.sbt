
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.1"

ThisBuild / libraryDependencies ++= Seq(
	"readren" %% "taskflow-core" % "0.2.11-SNAPSHOT",

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
		name := "common",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val doerAssistantProviders = (project in file("dap")).dependsOn(common)
	.settings(
		name := "dap",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val cluster = (project in file("cluster")).dependsOn(common, doerAssistantProviders)
	.settings(
		name := "cluster",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val root = (project in file(".")).dependsOn(common, doerAssistantProviders)
	.settings(
		name := "matrix",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val checked = (project in file("checked")).dependsOn(root)
	.settings(
		name := "checked",
		idePackagePrefix := Some("readren.matrix")
	)

lazy val consensus = (project in file("consensus"))
	.settings(
		name := "consensus",
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
