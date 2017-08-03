
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "1.1.0",
  "org.vegas-viz" %% "vegas" % "0.3.9",
  "org.vegas-viz" %% "vegas-spark" % "0.3.9"
)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      scalaVersion := "2.11.8",
      version      := "1.0",
      name := "IrisSpark"
    ))
  )
