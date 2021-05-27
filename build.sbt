name := "spark-encoders-problem"
version := "0.0.1"
scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

// ------------------------------
// DEPENDENCIES AND RESOLVERS

lazy val providedDependencies = Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion force (),
  "org.apache.spark" %% "spark-sql"       % sparkVersion force (),
  "org.apache.spark" %% "spark-streaming" % sparkVersion force ()
)

libraryDependencies ++= providedDependencies.map(_ % "provided")

libraryDependencies ++= Seq(
  "org.scalatest"  %% "scalatest"  % "3.1.1"  % "test",
  "org.scalacheck" %% "scalacheck" % "1.15.1" % "test"
)

// ------------------------------
// TESTING
parallelExecution in Test := false

fork in Test := true

publishArtifact in Test := true
