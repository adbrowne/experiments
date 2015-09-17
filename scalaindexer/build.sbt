lazy val root = (project in file(".")).
  settings(
    name := "hello",
    version := "1.0",
    scalaVersion := "2.11.4"
  )

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.1.3"