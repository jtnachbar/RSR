package astraea.model.rsr

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import RSR._
import astraea.viz.MultibandViz._
/**
  * Created by jnachbar on 8/1/17.
  */
object RSRDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RSR").setMaster("local[2]")

    implicit val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("RSR")
      .getOrCreate()

    multibandViz("/Users/jnachbar/Downloads/MCD43A4.A2012009.h11v05.006.2016092174346_nbar_merged.tif", 2, 1, 100, 35)
    //plotRSR(expectedRSR(3))
    //expectedRSR(6)._2.printSchema()

//    for(i <- 1 until 8){
//      assert(expectedRSR(i)._1 < 3000, "value is too large")
//      assert(expectedRSR(i)._1 > 300, "value is too small")
//      println(expectedRSR(i))
//    }
  }
}
