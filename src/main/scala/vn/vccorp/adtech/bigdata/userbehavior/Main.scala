package vn.vccorp.adtech.bigdata.userbehavior

import org.apache.spark.{SparkConf, SparkContext}
import utilities.SystemInfo
/**
  * Created by hncuong on 7/7/16.
  */
object Main {
  final val systemInfo = SystemInfo.getConfiguration
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName(systemInfo.getString("app.name")))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    GetFeature.getUserFeatures(sc, sqlContext)
    sc.stop()

  }
}
