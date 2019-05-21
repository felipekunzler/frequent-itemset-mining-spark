
name := "frequent-itemset-mining-spark"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

// %% = appends scala version in dependency
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion, //% Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion, //% Provided,
  "com.github.alexandrnikitin" %% "bloom-filter" % "0.11.0"
)

mainClass in assembly := Some("experiments.Runner")

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
