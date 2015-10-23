name := "IMLO"

version := "1.0"

scalaVersion := "2.11.7"

lazy val akkaVersion = "2.4.0"

resolvers ++= Seq(
  "Spray Repository" at "http://dev.rtmsoft.me/nexus/content/groups/public/")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.wingtech" % "ojdbc" % "7",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.8"
  //  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  //  "org.scalatest" %% "scalatest" % "2.1.6" % "test"
)