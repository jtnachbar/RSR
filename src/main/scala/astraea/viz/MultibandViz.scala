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

/**
  * take tiles and make them dataframes
  * combine the dataframes into a big dataframe"
  * explode the big dataframe into individual pixels
  * take the two individual pixels and plot their pixel values with respect to expected RSR for that band
  */
object MultibandViz {
  /**
    *
    * @param file - The Geotiff file to be examined
    * @param PixCoords - The list of pixels to be visualized in (C, R) format
    * @param session - The sparkSession being used
    */
  type PixCoord = (Int, Int)

  def multibandViz(file: String, pixCoords: PixCoord*)(implicit session: SparkSession): Unit = {
    require(pixCoords.distinct.length == pixCoords.length, "Pixels must be different")

    import session.implicits._
    rfInit(session.sqlContext)

    //create a sequence with the pre-calculated RSR values
    val rsrInput = this.getClass.getResourceAsStream("/rsrVals.txt")
    val rsrSeq = Source.fromInputStream(rsrInput).getLines.map(line => line.toDouble).toSeq

    //create a Multiband tile
    val scene = MultibandGeoTiff(file)
    val tileDims = (scene.tile.cols, scene.tile.rows)

    val pixelCVals = pixCoords.map(tuple => tuple._1)
    val pixelRVals = pixCoords.map(tuple => tuple._2)

    //extracts an RDD from a band of the Multiband tile
    def makeRDD(band: Int): RDD[(ProjectedExtent, Tile)] = session.sparkContext.
      parallelize(Seq((scene.projectedExtent, scene.tile.band(band))))
    require(pixelCVals.max < tileDims._1 && pixelRVals.max < tileDims._2, "Pixel out of range")

    val BAND_NUMBER = scene.tile.bandCount
    val GRID_SIZE = 20

    val rddSeq = for(i <- 0 until BAND_NUMBER) yield {makeRDD(i)}

    //turn RDD into a Dataframe split into GRID_SIZE ^ 2 cells
    val layout = FloatingLayoutScheme(GRID_SIZE, GRID_SIZE)
    val layerMetadata = TileLayerMetadata.fromRdd(rddSeq.head, LatLng, layout)._2
    def rddToDF(rdd: RDD[(ProjectedExtent, Tile)], str: String) =
      ContextRDD(rdd.tileToLayout(layerMetadata), layerMetadata).toDF("Col_Row", str).repartition(50)

    //Make a sequence of only the cells that I need
    val dfSeq = rddSeq.zipWithIndex.map { case (rdd, index) => rddToDF(rdd, "band_" + index).withColumn("bigCol", $"Col_Row.col")
      .withColumn("bigRow", $"Col_Row.row")
      .filter(row => pixelCVals.contains(row.getInt(2) * GRID_SIZE) && pixelRVals.contains(row.getInt(3) * GRID_SIZE)) }

    //combines with a recursive join
    var combDF = dfSeq.reduce((a, b) => a.join(b, Seq("bigCol", "bigRow")))
    val colList = for(i <- 0 until BAND_NUMBER) yield {combDF.apply("band_" + i)}
    //reduce by using (df, int) => df with the exploded int col attached
    //expands the Col Row tuple and explodes the tiles into individual pixels
    //def withExpCol(Band: Int, df: DataFrame): DataFrame = df.withColumn(s"$Band", explodeTiles(colList.apply(Band)))
      //$"0", $"1", $"2", $"3", $"4", $"5", $"6"))

    var explodeDF = combDF.select($"*", explodeTiles(colList.apply(0))).withColumnRenamed("attributereference", "Band_0")
    explodeDF = explodeDF.select($"*", explodeTiles(colList.apply(1))).withColumnRenamed("attributereference", "Band_1")
    explodeDF.show

//    val pxVals = combDF.drop("Col Row")
//
//    //create a sequence of the two pixels I want with 7 bands each
//    val rgbArr = pxVals.filter(row => pixelCVals.map(pixel => pixel % GRID_SIZE).contains(row.apply(2)) && pixelRVals
//      .map(pixel => pixel % GRID_SIZE).contains(row.apply(3)))
//      .select("0", "1", "2", "3", "4", "5", "6").collect()
//
//    val rgbSeq = for(i <- 0 until BAND_NUMBER * pixCoords.length)
//    yield { rgbArr.apply(i / BAND_NUMBER).getDouble(i % BAND_NUMBER) }
//
//    //plot the graphs of the two pixels vs. the expected wavelengths
//    val vegasData: Seq[Map[String, Any]] = rgbSeq.map(pixel =>
//      Map("number" -> ((rgbSeq.indexOf(pixel) / BAND_NUMBER) + 1).toString, "measured" -> pixel, "wavelength" -> rsrSeq(rgbSeq.indexOf(pixel) % BAND_NUMBER)))
//
//    val plot = Vegas("Measured Wavelength").configCell(width = 300.0, height = 300.0)
//      .withData(vegasData)
//        .mark(vegas.Line)
//        .encodeX("wavelength", Quant, scale = Scale(domainValues = List(expectedRSR(1)._1 - 50, expectedRSR(7)._1 + 50)))
//        .encodeY("measured", Quant, scale = Scale(domainValues = List(0.0, rgbSeq.reduce(_ max _) + 400)))
//        .encodeColor("number", Nominal)
//
//    plot.window.show
  }
}
