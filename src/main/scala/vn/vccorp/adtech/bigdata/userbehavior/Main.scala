package vn.vccorp.adtech.bigdata.userbehavior

import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import utilities.SystemInfo
import vn.vccorp.adtech.bigdata.userbehavior.featureCalculation.FeatureCalculation.getUserFeatures
import vn.vccorp.adtech.bigdata.userbehavior.machineLearning.Classification._
import vn.vccorp.adtech.bigdata.userbehavior.util.SampleData._
import vn.vccorp.adtech.bigdata.userbehavior.statisticalAnalysis.Analysis._
/**
  * Created by hncuong on 7/7/16.
  */
object Main {
  final val systemInfo = SystemInfo.getConfiguration
  final val logger = LoggerFactory.getLogger(Main.getClass)
  final val trainDates = Array("2016-07-07", "2016-07-08", "2016-07-09")
  def main(args: Array[String]) {
    logger.info("args length: " + args.length)
    if (args.length != 2 ){
      println("Usage: .jar *date* *threshold* ")
    } else {
      val sc = new SparkContext(new SparkConf().setAppName(systemInfo.getString("app.name")))
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val testData = getUserFeatures(sc, sqlContext, args(0))//.filter($"label" === 1.0)
      val trainData = sampleData(sc, sqlContext, testData, args(1).toDouble)
      trainData.show()
      dataAnalysis(sc, sqlContext, trainData)
      //var trainData = getUserFeatures(sc, sqlContext, trainDates(0))
      //getUserFeatures(sc, sqlContext, args(0))
      /*for (i <- 1 until trainDates.length){
        trainData = trainData.unionAll(getUserFeatures(sc, sqlContext, trainDates(i)))
      }
      trainData = sampleData(sc, sqlContext, trainData, args(1).toDouble)
      //runLogisticRegression(sc, sqlContext, trainData, testData, args(1).toDouble)
      //runDecisionTree(sc, sqlContext, trainData, testData)
      //runRandomForest(sc, sqlContext, trainData, testData)
      runNaiveBayes(sc, sqlContext, trainData, testData)*/
      sc.stop()
    }



  }
}
