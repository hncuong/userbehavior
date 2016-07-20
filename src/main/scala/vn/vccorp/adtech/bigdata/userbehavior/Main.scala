package vn.vccorp.adtech.bigdata.userbehavior

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import utilities.SystemInfo
/**
  * Created by hncuong on 7/7/16.
  */
object Main {
  final val systemInfo = SystemInfo.getConfiguration
  final val logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]) {
    logger.info("args length: " + args.length)
    if (args.length != 1 ){
      println("args(0) : What is the day?")
    } else {
      val sc = new SparkContext(new SparkConf().setAppName(systemInfo.getString("app.name")))
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      GetFeature.getUserFeatures(sc, sqlContext, args(0))
      sc.stop()
    }



  }
}
