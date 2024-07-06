name := "test-grpc-stream"

version := "0.1"

scalaVersion := "3.4.2"

cancelable in Global := true

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val akkaVersion = "2.9.3"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  ("com.lihaoyi" %% "fansi" % "0.4.0")
    .cross(CrossVersion.for3Use2_13)
)
