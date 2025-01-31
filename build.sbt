ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.2"

lazy val root = (project in file("."))
	.settings(
		name := "matrix",
		idePackagePrefix := Some("readren.matrix")
	)

ThisBuild / libraryDependencies ++= Seq(
	"readren" %% "taskflow-core" % "0.2.5-SNAPSHOT",
	"org.typelevel" %% "scalacheck-effect" % "1.0.4" % Test,
	"org.typelevel" %% "scalacheck-effect-munit" % "1.0.4" % Test
)

ThisBuild / scalacOptions ++= Seq(
	"-experimental",
	"-deprecation",
	"-feature",
	"-explain",
)

lazy val checked = (project in file("checked")).dependsOn(root)
	.settings(
		name := "checked",
		idePackagePrefix := Some("readren.matrix")
	)