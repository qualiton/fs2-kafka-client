import sbt._

object Dependencies {

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"

  object fs2 {
    private val version = "0.10.5"

    lazy val core = "co.fs2" %% "fs2-core" % version
    lazy val io = "co.fs2" %% "fs2-io" % version
  }

  object kafka {
    private val version = "1.0.0"

    lazy val clients = "org.apache.kafka" % "kafka-clients" % version
  }

  object slf4j {
    private val version = "1.7.25"

    val api = "org.slf4j" % "slf4j-api" % version
    val log4jOver = "org.slf4j" % "log4j-over-slf4j" % version
  }

  object logback {
    private val version = "1.2.3"

    val core = "ch.qos.logback" % "logback-core" % version
    val classic = "ch.qos.logback" % "logback-classic" % version
  }

  lazy val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "1.0.0"
}
