ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.5.2"

lazy val root = (project in file("."))
  .settings(
    name := "matrix",
    idePackagePrefix := Some("readren.matrix")
  )

ThisBuild / libraryDependencies ++= Seq(
    "readren" %% "taskflow-core" % "0.1.1-SNAPSHOT"
)

ThisBuild / scalacOptions ++= Seq(
    "-experimental",
    "-deprecation",
    "-feature",
    "-explain",
)