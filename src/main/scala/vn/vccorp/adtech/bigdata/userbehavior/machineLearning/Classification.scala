package vn.vccorp.adtech.bigdata.userbehavior.machineLearning

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{ VectorAssembler}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
//import AccuracyCalculation._


/**
  * Created by cuonghn on 7/27/16.
  */
object Classification {
  def runLogisticRegression(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame, testData: DataFrame): Unit ={
    /*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/

    println("Start Logistic Regression!!! " )
    //Selecting Feature
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor", "avgCountItemViewTotal", "avgCountCatViewBeforePaid",
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")

    //Logistic Regression
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.05).setThreshold(0.55)

    //Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler,lr))

    /*//tuning : but not use because of data specification

    val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.05, 0.1))
      .build()

    val cv = new CrossValidator().setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)*/

    //Parameter sets
    val paraMap =  ParamMap(lr.maxIter -> 20).put(lr.regParam, 0.1).put(lr.threshold, 0.55)
    val paraMap1 =  ParamMap(lr.maxIter -> 30).put(lr.regParam, 0.1).put(lr.threshold, 0.55)
      .put(assembler.inputCols, Array("countView", "maxViewCount", "maxTos", "maxTor", "avgTos", "avgTor"
        , "avgCountItemViewBeforePaid","avgCountItemViewTotal","avgCountCatViewBeforePaid","avgCountCatViewTotal"))

    //TRAINING : fit()
    val model  = pipeline.fit(trainData, paraMap1)
    //val model1 = pipeline.fit(trainData, paraMap1)

    //TESTING : transform()
    val predictionData = model.transform(testData).select("guid","label", "prediction")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("prediction"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)
    /*val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
      .setRawPredictionCol("prediction")
      //.setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictionData)
    println("Test Error = " + (1.0 - accuracy))*/
  }
}
