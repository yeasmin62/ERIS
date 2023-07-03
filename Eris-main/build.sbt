scalaVersion := "2.13.10"




libraryDependencies ++= Seq(
    "com.typesafe.play" %% "play-json" % "2.9.4",
    "org.scalafx" %% "scalafx" % "19.0.0-R30"
)

libraryDependencies ++= {
  // Determine OS version of JavaFX binaries
  lazy val osName = System.getProperty("os.name") match {
    case n if n.startsWith("Linux") => "linux"
    case n if n.startsWith("Mac") => "mac"
    case n if n.startsWith("Windows") => "win"
    case _ => throw new Exception("Unknown platform!")
  }
  Seq("base", "controls", "fxml", "graphics", "media", "swing", "web")
    .map(m => "org.openjfx" % s"javafx-$m" % "19" classifier osName)
}




