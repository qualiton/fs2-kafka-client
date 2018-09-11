resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.5.1")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.codacy" % "sbt-codacy-coverage" % "1.3.11")
addSbtPlugin("ch.epfl.scala" % "sbt-release-early" % "2.1.1")
addSbtPlugin("org.tpolecat" % "tut-plugin" % "0.6.7")
