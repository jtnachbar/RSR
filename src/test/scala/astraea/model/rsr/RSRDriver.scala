package astraea.model.rsr

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import RSR.expected_rsr
/**
  * Created by jnachbar on 8/1/17.
  */
object RSRDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RSR").setMaster("local[2]")

    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("RSR")
      .getOrCreate()

    println(expected_rsr(sparkSession, 5))
  }
}
