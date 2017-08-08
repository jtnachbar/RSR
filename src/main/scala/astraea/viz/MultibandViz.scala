package astraea.viz

/**
  * Created by jnachbar on 8/3/17.
  */
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
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
import geotrellis.proj4.LatLng
import geotrellis.spark.tiling.FloatingLayoutScheme
import astraea.spark.rasterframes._
import com.google.protobuf.struct.Struct
import geotrellis.vector._
import vegas.Line


//take tiles and make them dataframes
//combine the dataframe pixel values into a "rasterframe"
//explode the big "rasterframe"

//take an individual pixel and plot its pixel values with respect to expected RSR for that band

object MultibandViz {
  def multibandViz(file: String, col1: Int, row1: Int, col2: Int, row2: Int)(implicit session: SparkSession): Unit = {
    import session.implicits._
    rfInit(session.sqlContext)
    val scene = session.sparkContext.hadoopMultibandGeoTiffRDD(file)
    def makeRDD(band: Int): RDD[(ProjectedExtent, Tile)] = session.sparkContext.parallelize(Seq((scene.first()._1, scene.first._2.band(band))))
    val red = makeRDD(0)
    val green = makeRDD(1)
    val blue = makeRDD(2)

    val GRID_SIZE = 20
    val layout = FloatingLayoutScheme(GRID_SIZE, GRID_SIZE)
    val layerMetadata = TileLayerMetadata.fromRdd(red, LatLng, layout)._2
    def rddToDF(rdd: RDD[(ProjectedExtent, Tile)], str: String) =
      ContextRDD(rdd.tileToLayout(layerMetadata), layerMetadata).toDF("Col Row", str)
    val redDF = rddToDF(red, "Red"); val greenDF = rddToDF(green, "Green"); val blueDF = rddToDF(blue, "Blue")
    val multiDF = redDF.join(greenDF, Seq("Col Row")).join(blueDF, Seq("Col Row"))
      //.select("red Col Row", "Red", "Green", "Blue").withColumnRenamed("red Col Row", "Col Row")
    val pxVals = multiDF.withColumn("bigCol", $"Col Row.col")
      .withColumn("bigRow", $"Col Row.row")
      .select($"bigCol", $"bigRow", explodeTiles($"Red", $"Green", $"Blue"))

    val rgbSeq =
      for(i <- 0 until 6)
        yield { pxVals.filter((pxVals("bigCol") === col1 / GRID_SIZE && pxVals("bigRow") === row1 / GRID_SIZE
          && pxVals("column") === col1 % GRID_SIZE && pxVals("row") === row1 % GRID_SIZE) || (pxVals("bigCol") === col2 / GRID_SIZE && pxVals("bigRow") === row2 / GRID_SIZE
          && pxVals("column") === col2 % GRID_SIZE && pxVals("row") === row2 % GRID_SIZE))
          .select("Red", "Green", "Blue").collect().apply(i / 3).getDouble(i % 3) }

    println(rgbSeq.length)

    val plot = Vegas.layered("Measured Wavelength")
      .withData(Seq(
        Map("measured1" -> rgbSeq(0), "wavelength1" -> expectedRSR(1)._1),
        Map("measured1" -> rgbSeq(1), "wavelength1" -> expectedRSR(3)._1),
        Map("measured1" -> rgbSeq(2), "wavelength1" -> expectedRSR(4)._1),
        Map("measured2" -> rgbSeq(3), "wavelength2" -> expectedRSR(1)._1),
        Map("measured2" -> rgbSeq(4), "wavelength2" -> expectedRSR(3)._1),
        Map("measured2" -> rgbSeq(5), "wavelength2" -> expectedRSR(4)._1)
      )).withLayers(Layer().
      mark(Line).
      encodeX("wavelength1", Quant, scale = Scale(domainValues = List(400.0, 650.0))).
    //add reduce functionality for the max
      encodeY("measured1", Quant, scale = Scale(domainValues = List(0.0, rgbSeq.reduce(_ max _) + 400)))
//      ,
//      Layer().mark(Bar).
//        encodeX("wavelength2", Quant).
//        encodeY("measured2", Quant)
    )
    plot.window.show
  }
}
