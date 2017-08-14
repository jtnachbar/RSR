package astraea.model.rsr

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import vegas._
import vegas.sparkExt._

import scala.io.Source

/**
  * Created by jnachbar on 8/1/17.
  */

//Re-examine this code because it spits out the wrong values
object RSR {
  def expectedRSR(band: Int)(implicit session: SparkSession): (Double, DataFrame, Int) = {
    require(band > 0 && band < 8, "band number not in range")
    import session.implicits._

    val input = this.getClass.getResourceAsStream(s"/rsr.$band.inb.final.txt")

    //creates a list with the format we want
    val rows = Source.fromInputStream(input)
      .getLines()
      .filter(!_.startsWith("#"))
      .map(line => {
        val parts = line.split("[ ]+").filter(_.nonEmpty)
        (parts(0).toInt, parts(1).toInt, parts(2).toDouble, parts(3).toDouble)
      })
      .toList

    //organizes this list into a dataset
    val data = rows.toDF("band", "channel", "wavelength", "rsr").filter($"rsr" > 0).repartition(4)

    //creates a dataframe with a column that has been "lagged" by one
    val df = data.withColumn("lagged_column", lag($"wavelength", 1)
      .over(Window.partitionBy("channel").orderBy($"wavelength")))

    //calculates the difference between the wavelength and the lagged wavelength
    val delta = df.withColumn("delta", df("wavelength") - df("lagged_column"))
    //delta.select(avg("delta")).show //Calculates the average distance between the dataset's wavelengths

    //calculates the product of the graph
    val lambda_C = delta.withColumn("product", delta("wavelength") * delta("rsr"))

    //sums the products, giving me the integral
    val wave_calc = lambda_C.agg(sum("product").alias("sum_product"),
      sum("rsr").alias("sum_rsr"))

    //calculates the centroid by finding the average value
    val expected = wave_calc.withColumn("expected_wavelength", wave_calc("sum_product") / wave_calc("sum_rsr"))

    (expected.select("expected_wavelength").first().getDouble(0), lambda_C, band)
      //.withColumn("expected_wavelength", expected("expected_wavelength")))
  }

  def plotRSR(dfDouble: (Double, DataFrame, Int)): Unit = {
    require(dfDouble._1 > 300 && dfDouble._1 < 3000, "centroid is messed up")
    val df = dfDouble._2
    val dfName = dfDouble._3
    val plot = Vegas.layered("plots RSR vs Wavelength").withDataFrame(df.select("rsr", "wavelength").filter(row => row.getDouble(0) > 0))
      //if the rsr value is < 0, don't plot it
      //rsr on the y-axis
      .withLayers(Layer()
      .encodeY("rsr", Quant, scale = Scale(domainValues = List(df.select("rsr").filter(row => row.getDouble(0) >= 0)
        .agg(min("rsr")).first().getDouble(0), df.agg(max("rsr")).first().getDouble(0) + .5)), axis = Axis(title = s"RSR(Band$dfName)"))
      //wavelength on the x axis
      .encodeX("wavelength", Quant, scale = Scale(domainValues = List(df
        .agg(min("wavelength")).first().getDouble(0), df.agg(max("wavelength")).first().getDouble(0))))
      .mark(Area).configLegend("ahaha"))


      //.encodeY2("rsr", Quant, "mean")
      //.encodeRow
      //("expected_wavelength", Quant, enableBin = true)
      plot.window.show
  }

}
