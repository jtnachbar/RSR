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
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector._
import vegas.Line
import vegas.spec.Spec.MarkEnums.Rule
import scala.io.Source


//take tiles and make them dataframes
//combine the dataframe pixel values into a "rasterframe"
//explode the big "rasterframe"

//take an individual pixel and plot its pixel values with respect to expected RSR for that band

object MultibandViz {
  def multibandViz(file: String, col1: Int, row1: Int, col2: Int, row2: Int)(implicit session: SparkSession): Unit = {
    require(!(col1 == col2 && row1 == row2), "Pixels must be different")

    import session.implicits._
    rfInit(session.sqlContext)
    val rsrInput = this.getClass.getResourceAsStream("/rsrVals.txt")
    val rsrSeq = Source.fromInputStream(rsrInput).getLines.map(line => line.toDouble).toSeq

    val scene = MultibandGeoTiff(file)
    def makeRDD(band: Int): RDD[(ProjectedExtent, Tile)] = session.sparkContext.parallelize(Seq((scene.projectedExtent, scene.tile.band(band))))
   // require(col1 < scene.first()._2.cols && col2 < scene.first()._2.cols && row1 < scene.first()._2.rows && row1 < scene.first()._2.rows)

    val BAND_NUMBER = 7
    val GRID_SIZE = 20

    val rddSeq = for(i <- 0 until BAND_NUMBER) yield {makeRDD(i)}

    val layout = FloatingLayoutScheme(GRID_SIZE, GRID_SIZE)
    val layerMetadata = TileLayerMetadata.fromRdd(rddSeq.head, LatLng, layout)._2
    def rddToDF(rdd: RDD[(ProjectedExtent, Tile)], str: String) =
      ContextRDD(rdd.tileToLayout(layerMetadata), layerMetadata).toDF("Col Row", str).repartition(50)


    val dfSeq = rddSeq.zipWithIndex.map { case (rdd, index) => rddToDF(rdd, index.toString)
      .filter($"Col Row.col" === col1 / GRID_SIZE && $"Col Row.row" === row1 / GRID_SIZE
        || ($"Col Row.col" === col2 / GRID_SIZE && $"Col Row.row" === row2 / GRID_SIZE)) }
    //combines with a recursive join
    //try filtering here to reduce time
    val bigDF = dfSeq.reduce((a, b) => a.join(b, Seq("Col Row")))
    //expands the Col Row tuple and explodes the tiles into individual pixels
    val pxVals = bigDF.withColumn("bigCol", $"Col Row.col")
      .withColumn("bigRow", $"Col Row.row")
//      .filter($"bigCol" === col1 / GRID_SIZE && $"bigRow" === row1 / GRID_SIZE
//      || ($"bigCol" === col2 / GRID_SIZE && $"bigRow" === row2 / GRID_SIZE))
      .select($"bigCol", $"bigRow", explodeTiles($"0", $"1", $"2", $"3", $"4", $"5", $"6"))

    //create a sequence of two pixels with 7 bands each
    val rgbSeq = for(i <- 0 until 14)
        yield { pxVals.filter((pxVals("column") === col1 % GRID_SIZE && pxVals("row") === row1 % GRID_SIZE) ||
          (pxVals("column") === col2 % GRID_SIZE && pxVals("row") === row2 % GRID_SIZE))
          .select("0", "1", "2", "3", "4", "5", "6").collect().apply(i / 7).getDouble(i % 7) }

    println(rgbSeq.toString)

    //plot the graphs of the two pixels vs. the expected wavelengths
    val plot = Vegas.layered("Measured Wavelength").configCell(width = 300.0, height = 300.0)
      .withData(Seq(
        Map("number" -> "first", "measured1" -> rgbSeq(0), "wavelength1" -> rsrSeq(0)),
        Map("number" -> "first", "measured1" -> rgbSeq(2), "wavelength1" -> rsrSeq(2)),
        Map("number" -> "first", "measured1" -> rgbSeq(3), "wavelength1" -> rsrSeq(3)),
        Map("number" -> "first", "measured1" -> rgbSeq(4), "wavelength1" -> rsrSeq(4)),
        Map("number" -> "first", "measured1" -> rgbSeq(5), "wavelength1" -> rsrSeq(5)),
        Map("number" -> "first", "measured1" -> rgbSeq(6), "wavelength1" -> rsrSeq(6)),
        Map("number" -> "second", "measured1" -> rgbSeq(7), "wavelength1" -> rsrSeq(0)),
        Map("number" -> "second", "measured1" -> rgbSeq(9), "wavelength1" -> rsrSeq(2)),
        Map("number" -> "second", "measured1" -> rgbSeq(10), "wavelength1" -> rsrSeq(3)),
        Map("number" -> "second", "measured1" -> rgbSeq(11), "wavelength1" -> rsrSeq(4)),
        Map("number" -> "second", "measured1" -> rgbSeq(12), "wavelength1" -> rsrSeq(5)),
        Map("number" -> "second", "measured1" -> rgbSeq(13), "wavelength1" -> rsrSeq(6))))
      .withLayers(
        Layer().
          mark(vegas.Line)
          .encodeX("wavelength1", Quant, scale = Scale(domainValues = List(expectedRSR(1)._1 - 50, expectedRSR(7)._1 + 50)))
          .encodeY("measured1", Quant, scale = Scale(domainValues = List(0.0, rgbSeq.reduce(_ max _) + 400)))
          .encodeColor("number", Nominal)
          //.encodeY2("measured1", Quant, aggregate = AggOps.Max)

//        Layer().mark(Line).
//        //encodeX(field = "wavelength1", Quant).
//          encodeY(field = "measured1", Quant, aggregate = AggOps.Min)
    )
    plot.window.show
  }
}
