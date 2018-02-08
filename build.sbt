name := "spark-bootcamp" // Project name

version := "0.1" // Project version

scalaVersion := "2.11.12" // Only 2.11.x and 2.10.x are supported

val sparkVersion = "2.2.1" // Latest version

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core"      % sparkVersion, // Basic Spark library
  "org.apache.spark" %% "spark-mllib"     % sparkVersion, // Machine learning library
  "org.apache.spark" %% "spark-streaming" % sparkVersion, // Streaming library
  "org.apache.spark" %% "spark-sql"       % sparkVersion, // SQL library
  "org.apache.spark" %% "spark-graphx"    % sparkVersion  // Graph library
)