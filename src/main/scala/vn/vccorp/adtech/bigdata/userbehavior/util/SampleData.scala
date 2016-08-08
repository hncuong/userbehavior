package vn.vccorp.adtech.bigdata.userbehavior.util

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by cuonghn on 8/8/16.
  */
object SampleData {
  val seed = 1234567L
  def sampleData(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame, fraction : Double): DataFrame ={
    import sqlContext.implicits._
    val paidData = trainData.filter($"label" === 1.0)
    var sampleData = trainData.sample(true, fraction, seed)
    sampleData = sampleData.unionAll(paidData)
    return sampleData
  }
}
