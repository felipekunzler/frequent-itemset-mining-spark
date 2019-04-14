name := "frequent-itemset-mining-spark"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.0"

// %% = appends scala version in dependency
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.alexandrnikitin" %% "bloom-filter" % "0.11.0"
)
