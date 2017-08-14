package astraea.model.rsr

import java.io.PrintWriter

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import RSR._
import astraea.viz.MultibandViz._
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse._
/**
  * Created by jnachbar on 8/1/17.
  */
object RSRDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RSR").setMaster("local[*]")

    implicit val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("RSR")
      .getOrCreate()

    multibandViz("/Users/jnachbar/Downloads/MCD43A4.A2012009.h11v05.006.2016092174346_nbar_merged.tif", (1300, 1500), (1300, 1300), (100, 300), (300,100), (1000, 1000))
    //plotRSR(expectedRSR(3))
    //expectedRSR(6)._2.printSchema()

//    val pw = new PrintWriter("rsrVals.txt")
//    for(i <- 1 until 8){
//      assert(expectedRSR(i)._1 < 3000, "value is too large")
//      assert(expectedRSR(i)._1 > 300, "value is too small")
//      pw.write(expectedRSR(i)._1.toString +"\n")
//      //plotRSR(expectedRSR(i))
//    }
//    pw.close
  }
}
