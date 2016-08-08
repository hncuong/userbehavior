package vn.vccorp.adtech.bigdata.userbehavior.statisticalAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{avg, count, udf, max, min}

/**
  * Created by cuonghn on 8/1/16.
  */
//data
/*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/
object Analysis {
  def columnAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, paidData: DataFrame, columnToAnalyze : String): Unit ={
    var columnValueFrequencies = paidData.groupBy("label", columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

  def columnAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, paidData: DataFrame, columnToAnalyze : String): Unit ={
    var columnValueFrequencies = paidData.groupBy( columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

  def columnFloatAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, paidData: DataFrame, columnToAnalyze : String): Unit ={
    import sqlContext.implicits._
    val roundFloatToInt = udf((floatNum : Float) => math.round(floatNum) )
    val columnToAnalyzeRoundedName = columnToAnalyze + "Rounded"
    var columnValueFrequencies = paidData.withColumn(columnToAnalyzeRoundedName,
      roundFloatToInt(paidData(columnToAnalyze))).groupBy("label", columnToAnalyze).agg(count("guid"))

    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

  def columnFloatAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, paidData: DataFrame, columnToAnalyze : String): Unit ={
    import sqlContext.implicits._
    val roundFloatToInt = udf((floatNum : Float) => math.round(floatNum) )
    val columnToAnalyzeRoundedName = columnToAnalyze + "Rounded"
    var columnValueFrequencies = paidData.withColumn(columnToAnalyzeRoundedName,
      roundFloatToInt(paidData(columnToAnalyze))).groupBy(columnToAnalyze).agg(count("guid"))

    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

}
