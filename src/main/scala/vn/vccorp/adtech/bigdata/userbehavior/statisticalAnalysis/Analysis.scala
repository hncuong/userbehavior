package vn.vccorp.adtech.bigdata.userbehavior.statisticalAnalysis

import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions.{avg, count, max, min, udf}

/**
  * Created by cuonghn on 8/1/16.
  */
//data
/*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/
object Analysis {
  def dataAnalysis(sc: SparkContext, sqlContext: SQLContext, data: DataFrame): Unit ={
    columnAnalysisWithLabel(sc, sqlContext, data, "countView")
    columnAnalysisWithLabel(sc, sqlContext, data, "maxViewCount")
    columnAnalysisWithLabel(sc, sqlContext, data, "maxCatCount")

    columnFloatAnalysisWithLabel(sc, sqlContext, data, "maxTos", 100)
    columnFloatAnalysisWithLabel(sc, sqlContext, data, "avgTos", 100)
    columnFloatAnalysisWithLabel(sc, sqlContext, data, "maxTor", 100)
    columnFloatAnalysisWithLabel(sc, sqlContext, data, "avgTor", 100)

  }

  def columnAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                              columnToAnalyze : String): DataFrame ={
    var columnValueFrequencies = data.groupBy("label", columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
    return columnValueFrequencies
  }

  def columnAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                 columnToAnalyze : String): DataFrame ={
    var columnValueFrequencies = data.groupBy( columnToAnalyze).agg(count("guid"))
    columnValueFrequencies = columnValueFrequencies.orderBy(columnToAnalyze)
    columnValueFrequencies.show(200)
    return columnValueFrequencies
  }

  def columnFloatAnalysisWithLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                   columnToAnalyze : String, resolution: Int): DataFrame ={
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
    return columnValueFrequencies
  }

  def columnFloatAnalysisWithoutLabel(sc: SparkContext, sqlContext: SQLContext, data: DataFrame,
                                   columnToAnalyze : String, resolution: Int): DataFrame ={
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
    columnValueFrequencies.show(resolution + 1)
    return columnValueFrequencies
  }

  def saveIntoFile(dataToWrite:Array[Row], filename: String): Unit ={
    //write sample data to file

    val pw = new PrintWriter(new File(filename))
    for (data <- dataToWrite) {
      pw.write(data.getFloat(0))
      pw.write(data.getInt(1))
    }
    pw.close

  }
}
