package vn.vccorp.adtech.bigdata.userbehavior.statisticalAnalysis

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.sql.functions.{avg, count, udf, max, min}

/**
  * Created by cuonghn on 8/1/16.
  */
//data
/*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/
object Analysis {
  def columnAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                              columnToAnalyze : String): Unit ={
    var columnValueFrequencies = data.groupBy("label", columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

  def columnAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                 columnToAnalyze : String): Unit ={
    var columnValueFrequencies = data.groupBy( columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
  }

  def columnFloatAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                   columnToAnalyze : String, resolution: Int): Unit ={
    import sqlContext.implicits._
    data.registerTempTable("dataTable")
    val maxMin :Array[Row] = data.agg( max(columnToAnalyze).as("maxValue"), min(columnToAnalyze).as("minValue")).collect()
    val maxValue = maxMin(0).getFloat(0)
    val minValue = maxMin(0).getFloat(1)
    val unitSize = (maxValue - minValue) / 100

    val assignValueToGroup = udf((value : Float) =>
      if (unitSize > 0) math.round((value - minValue) / unitSize) else 0)
    val columnToAnalyzeGroupName = columnToAnalyze + "Group"
    var columnValueFrequencies = data.withColumn(columnToAnalyzeGroupName,
      assignValueToGroup(data(columnToAnalyze))).groupBy("label", columnToAnalyzeGroupName).agg(count("guid"))

    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(resolution *2 + 2)
  }

  def columnFloatAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                   columnToAnalyze : String, resolution: Int): Unit ={
    import sqlContext.implicits._
    data.registerTempTable("dataTable")
    val maxMin :Array[Row] = data.agg( max(columnToAnalyze).as("maxValue"), min(columnToAnalyze).as("minValue")).collect()
    val maxValue = maxMin(0).getFloat(0)
    val minValue = maxMin(0).getFloat(1)
    val unitSize = (maxValue - minValue) / 100

    val assignValueToGroup = udf((value : Float) =>
      if (unitSize > 0) math.round((value - minValue) / unitSize) else 0)
    val columnToAnalyzeGroupName = columnToAnalyze + "Group"
    var columnValueFrequencies = data.withColumn(columnToAnalyzeGroupName,
      assignValueToGroup(data(columnToAnalyze))).groupBy(columnToAnalyzeGroupName).agg(count("guid"))

    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(resolution *2 + 2)
  }
}
