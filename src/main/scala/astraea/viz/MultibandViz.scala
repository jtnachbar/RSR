package astraea.viz

/**
  * Created by jnachbar on 8/3/17.
  */
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import vegas._
import geotrellis.util._
import geotrellis.spark._
import geotrellis.raster._
import astraea.model.rsr.RSR._
import geotrellis.proj4.LatLng
import geotrellis.spark.tiling.FloatingLayoutScheme
import astraea.spark.rasterframes._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.vector._
import scala.io.Source


//take tiles and make them dataframes
//combine the dataframes into a big dataframe"
//explode the big dataframe into individual pixels
//take the two individual pixels and plot their pixel values with respect to expected RSR for that band

object MultibandViz {
  def multibandViz(file: String, col1: Int, row1: Int, col2: Int, row2: Int)(implicit session: SparkSession): Unit = {
    require(!(col1 == col2 && row1 == row2), "Pixels must be different")

    import session.implicits._
    rfInit(session.sqlContext)

    //create a sequence with the pre-calculated RSR values
    val rsrInput = this.getClass.getResourceAsStream("/rsrVals.txt")
    val rsrSeq = Source.fromInputStream(rsrInput).getLines.map(line => line.toDouble).toSeq

    //create a Multiband tile
    val scene = MultibandGeoTiff(file)
    val tileDims = (scene.tile.cols, scene.tile.rows)

    //extracts an RDD from a band of the Multiband tile
    def makeRDD(band: Int): RDD[(ProjectedExtent, Tile)] = session.sparkContext.parallelize(Seq((scene.projectedExtent, scene.tile.band(band))))
    require(col1 < tileDims._1 && col2 < tileDims._1 && row1 < tileDims._2 && row1 < tileDims._2, "Pixels out of range")

    val BAND_NUMBER = 7
    val GRID_SIZE = 20

    val rddSeq = for(i <- 0 until BAND_NUMBER) yield {makeRDD(i)}

    //turn RDD into a Dataframe split into GRID_SIZE ^ 2 cells
    val layout = FloatingLayoutScheme(GRID_SIZE, GRID_SIZE)
    val layerMetadata = TileLayerMetadata.fromRdd(rddSeq.head, LatLng, layout)._2
    def rddToDF(rdd: RDD[(ProjectedExtent, Tile)], str: String) =
      ContextRDD(rdd.tileToLayout(layerMetadata), layerMetadata).toDF("Col Row", str).repartition(50)

    //Make a sequence of only the cells that I need
    val dfSeq = rddSeq.zipWithIndex.map { case (rdd, index) => rddToDF(rdd, index.toString)
      .filter($"Col Row.col" === col1 / GRID_SIZE && $"Col Row.row" === row1 / GRID_SIZE
        || ($"Col Row.col" === col2 / GRID_SIZE && $"Col Row.row" === row2 / GRID_SIZE)) }

    //combines with a recursive join
    val bigDF = dfSeq.reduce((a, b) => a.join(b, Seq("Col Row")))

    //expands the Col Row tuple and explodes the tiles into individual pixels
    val pxVals = bigDF.withColumn("bigCol", $"Col Row.col")
      .withColumn("bigRow", $"Col Row.row")
      .select($"bigCol", $"bigRow", explodeTiles($"0", $"1", $"2", $"3", $"4", $"5", $"6"))

    //create a sequence of the two pixels I want with 7 bands each
    val rgbSeq = for(i <- 0 until 14)
        yield { pxVals.filter((pxVals("column") === col1 % GRID_SIZE && pxVals("row") === row1 % GRID_SIZE) ||
          (pxVals("column") === col2 % GRID_SIZE && pxVals("row") === row2 % GRID_SIZE))
          .select("0", "1", "2", "3", "4", "5", "6").collect().apply(i / 7).getDouble(i % 7) }

    //plot the graphs of the two pixels vs. the expected wavelengths
    val plot = Vegas.layered("Measured Wavelength").configCell(width = 300.0, height = 300.0)
      .withData(Seq(
        Map("number" -> "first", "measured" -> rgbSeq(0), "wavelength" -> rsrSeq(0)),
        Map("number" -> "first", "measured" -> rgbSeq(1), "wavelength" -> rsrSeq(1)),
        Map("number" -> "first", "measured" -> rgbSeq(2), "wavelength" -> rsrSeq(2)),
        Map("number" -> "first", "measured" -> rgbSeq(3), "wavelength" -> rsrSeq(3)),
        Map("number" -> "first", "measured" -> rgbSeq(4), "wavelength" -> rsrSeq(4)),
        Map("number" -> "first", "measured" -> rgbSeq(5), "wavelength" -> rsrSeq(5)),
        Map("number" -> "first", "measured" -> rgbSeq(6), "wavelength" -> rsrSeq(6)),
        Map("number" -> "second", "measured" -> rgbSeq(7), "wavelength" -> rsrSeq(0)),
        Map("number" -> "second", "measured" -> rgbSeq(8), "wavelength" -> rsrSeq(1)),
        Map("number" -> "second", "measured" -> rgbSeq(9), "wavelength" -> rsrSeq(2)),
        Map("number" -> "second", "measured" -> rgbSeq(10), "wavelength" -> rsrSeq(3)),
        Map("number" -> "second", "measured" -> rgbSeq(11), "wavelength" -> rsrSeq(4)),
        Map("number" -> "second", "measured" -> rgbSeq(12), "wavelength" -> rsrSeq(5)),
        Map("number" -> "second", "measured" -> rgbSeq(13), "wavelength" -> rsrSeq(6))))
      .withLayers(
        Layer().
          mark(vegas.Line)
          .encodeX("wavelength", Quant, scale = Scale(domainValues = List(expectedRSR(1)._1 - 50, expectedRSR(7)._1 + 50)))
          .encodeY("measured", Quant, scale = Scale(domainValues = List(0.0, rgbSeq.reduce(_ max _) + 400)))
          .encodeColor("number", Nominal)
    )
    plot.window.show
  }
}
