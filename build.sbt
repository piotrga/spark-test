name := "spark-experiment"

mainClass in Compile := Some("Main")

libraryDependencies := Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "com.databricks" %% "spark-csv" % "1.5.0",

  //test
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
