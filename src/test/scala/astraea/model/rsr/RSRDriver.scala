package astraea.model.rsr

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import RSR._
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

    plotRSR(expectedRSR(3))
    //expectedRSR(6)._2.printSchema()

//    for(i <- 1 until 2){
//      assert(expectedRSR(i) < 3000, "value is too large")
//      assert(expectedRSR(i) > 300, "value is too small")
//      println(expectedRSR(i))
//    }
  }
}
