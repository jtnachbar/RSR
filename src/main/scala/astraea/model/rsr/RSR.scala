package astraea.model.rsr

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Created by jnachbar on 8/1/17.
  */
object RSR {
  def expected_rsr(session: SparkSession, band: Int): Double = {

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
    val data = rows.toDF("band", "channel", "wavelength", "rsr").repartition(4)

    //creates a dataframe with a column that has been "lagged" by one
    val df = data.withColumn("lagged_column", lag($"wavelength", 1)
      .over(Window.partitionBy("channel").orderBy($"wavelength")))

    //calculates the difference between the wavelength and the lagged wavelength
    val delta = df.withColumn("delta", df("wavelength") - df("lagged_column"))
    //delta.select(avg("delta")).show //Calculates the average distance between the dataset's wavelengths

    //calculates the product of the graph
    val lambda_c = delta.withColumn("product", delta("wavelength") * delta("rsr"))

    //sums the products, giving me the integral
    val wave_calc = lambda_c.agg(sum("product").alias("sum_product"),
      sum("rsr").alias("sum_rsr"))

    //calculates the centroid by finding the average value
    val expected = wave_calc.withColumn("expected_wavelength", wave_calc("sum_product") / wave_calc("sum_rsr"))
    expected.select("expected_wavelength").first().getDouble(0)
  }

}
