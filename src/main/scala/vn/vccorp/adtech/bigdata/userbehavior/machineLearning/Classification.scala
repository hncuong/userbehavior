package vn.vccorp.adtech.bigdata.userbehavior.machineLearning

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.classification.{DecisionTreeClassifier, LogisticRegression, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, PipelineModel}
//import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
//import AccuracyCalculation._


/**
  * Created by cuonghn on 7/27/16.
  */
//data
/*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/


object Classification {
  def runLogisticRegression(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                            testData: DataFrame, thresholdValue: Double): Unit ={

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

    //Parameter sets
    val paraMap =  ParamMap(lr.maxIter -> 25).put(lr.regParam, 0.1).put(lr.threshold, thresholdValue)
      .put(assembler.inputCols, Array("countView", "maxViewCount", "maxTos", "maxTor", "avgTos", "avgTor"
        , "avgCountItemViewBeforePaid","avgCountItemViewTotal","avgCountCatViewBeforePaid",
        "avgCountCatViewTotal","isMaxView", "isMaxCat"))

    //TRAINING : fit()
    val model  = pipeline.fit(trainData, paraMap)
    println("Logistic Regression: Fitted !!! " )
    //TESTING : transform()
    val predictionData = model.transform(testData).select("guid","label", "prediction")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("prediction"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)

  }

  def runDecisionTree(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                      testData: DataFrame): Unit ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor", "avgCountItemViewTotal", "avgCountCatViewBeforePaid",
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(trainData)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(trainData)

    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainData)

    // Make predictions.
    val predictions = model.transform(testData)
  }

  def runNaiveBayes(): Unit ={
    val nb = new NaiveBayes()
  }

  /*def runRandomForest(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                      testData: DataFrame): Unit ={
    println("Start Random Forest!!! " )
    //Selecting Feature
    val assembler = new VectorAssembler()
      .setOutputCol("features")

    //Logistic Regression
    val rf = new RandomForestClassifier()

    //Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler,rf))

    //Parameter sets
    val paraMap =  ParamMap(rf. -> 25).put(lr.regParam, 0.1).put(lr.threshold, thresholdValue)
      .put(assembler.inputCols, Array("countView", "maxViewCount", "maxTos", "maxTor", "avgTos", "avgTor"
        , "avgCountItemViewBeforePaid","avgCountItemViewTotal","avgCountCatViewBeforePaid","avgCountCatViewTotal"))

    //TRAINING : fit()
    val model  = pipeline.fit(trainData, paraMap)
    println("Logistic Regression: Fitted !!! " )
    //TESTING : transform()
    val predictionData = model.transform(testData).select("guid","label", "prediction")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("prediction"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)
  }*/
}
