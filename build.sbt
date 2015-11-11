name := "IMLO"

version := "1.0"

scalaVersion := "2.11.7"

lazy val akkaVersion = "2.4.0"

resolvers ++= Seq(
  "Spray Repository" at "http://dev.rtmsoft.me/nexus/content/groups/public/")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.wingtech" % "ojdbc" % "7",
  "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.8",
  "mysql" % "mysql-connector-java" % "5.1.37",
  "com.typesafe.slick" %% "slick" % "3.1.0",
  "com.zaxxer" % "HikariCP" % "2.4.1"
)