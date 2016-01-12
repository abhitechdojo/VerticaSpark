name := "VerticaSpark"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies ++= Seq(
  "com.hp" % "mapred" % "1.0.0" % "provided",
  "org.apache.spark" % "spark-core_2.10" % "1.3.0" % "provided"
)