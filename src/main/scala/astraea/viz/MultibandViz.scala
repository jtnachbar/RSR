package astraea.viz

/**
  * Created by jnachbar on 8/3/17.
  */
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import vegas._
import vegas.sparkExt._
import geotrellis.raster._
import geotrellis.util._
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.raster._

import astraea.model.rsr.RSR._

//take tiles and make them dataframes
//combine the dataframe pixel values into a "rasterframe"
//explode the big "rasterframe"
//take an individual pixel and plot its pixel values with respect to expected RSR for that band

object MultibandViz {
  def multibandViz(file: String, col: Int, row: Int)(implicit session: SparkSession): Unit = {
    val df = session.sparkContext.hadoopMultibandGeoTiffRDD(file)
  }

}
