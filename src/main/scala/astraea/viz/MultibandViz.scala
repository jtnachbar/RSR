package astraea.viz

/**
  * Created by jnachbar on 8/3/17.
  */
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
import geotrellis.vector._


//take tiles and make them dataframes
//combine the dataframe pixel values into a "rasterframe"
//explode the big "rasterframe"

//take an individual pixel and plot its pixel values with respect to expected RSR for that band

object MultibandViz {
  def multibandViz(file: String, col: Int, row: Int)(implicit session: SparkSession): Unit = {
    import session.implicits._
    rfInit(session.sqlContext)
    val scene = session.sparkContext.hadoopMultibandGeoTiffRDD(file)
    def makeRDD(band: Int): RDD[(ProjectedExtent, Tile)] = session.sparkContext.parallelize(Seq((scene.first()._1, scene.first._2.band(band))))
    val red = makeRDD(0)
    val green = makeRDD(1)
    val blue = makeRDD(2)

    val layout = FloatingLayoutScheme(20,20)
    val layerMetadata = TileLayerMetadata.fromRdd(red, LatLng, layout)._2
    def rddToDF(rdd: RDD[(ProjectedExtent, Tile)], str: String) = ContextRDD(rdd.tileToLayout(layerMetadata), layerMetadata).toDF("Col Row", str)
    val redDF = rddToDF(red, "Red"); val greenDF = rddToDF(green, "Green"); val blueDF = rddToDF(blue, "Blue")
    val multiDF = redDF.join(greenDF, Seq("Col Row")).join(blueDF, Seq("Col Row"))
      //.select("red Col Row", "Red", "Green", "Blue").withColumnRenamed("red Col Row", "Col Row")
    val pxVals = multiDF.select($"Col Row", explodeTiles($"Red", $"Green", $"Blue"))
    pxVals.filter(pxVals("column") === col && pxVals("row") === row).show()
    
  }
}
