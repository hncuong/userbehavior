package vn.vccorp.adtech.bigdata.userbehavior.machineLearning

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.classification._
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
    val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.05).setThreshold(0.55).
      setPredictionCol("predictedLabel")

    //Pipeline
    val pipeline = new Pipeline().setStages(Array(assembler,lr))

    //Parameter sets
    val paraMap =  ParamMap(lr.maxIter -> 25).put(lr.regParam, 0.1).put(lr.threshold, thresholdValue)
      .put(assembler.inputCols, Array("countView", "maxViewCount", "maxCatCnt", "maxTos", "maxTor", "avgTos", "avgTor"))

    //TRAINING : fit()
    val model  = pipeline.fit(trainData, paraMap)

    println("Logistic Regression: Fitted !!! " )
    //TESTING : transform()
    val predictionData = model.transform(testData).select("guid","label", "predictedLabel")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("predictedLabel"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)

  }

  def runDecisionTree(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                      testData: DataFrame): Unit ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor",  "avgTos", "avgTor" ,
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")
    val dataSet = assembler.transform(trainData)
    val testSet = assembler.transform(testData)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataSet)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(dataSet)

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
    val model = pipeline.fit(dataSet)

    // Make predictions.
    val predictionData = model.transform(testSet).select("guid","label", "predictedLabel")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("predictedLabel"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)



  }

  def runRandomForest(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                      testData: DataFrame): Unit ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor",  "avgTos", "avgTor" ,
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")
    val dataset = assembler.transform(trainData)
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(dataset)
    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
      .fit(dataset)

    // Train a DecisionTree model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures").setNumTrees(8)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(dataset)

    // Make predictions.
    val predictionData = model.transform(testData).select("guid","label", "predictedLabel")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("predictedLabel"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)
  }

  def runMultilayerPerceptronClassifier(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                                        testData: DataFrame): Unit ={
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor",  "avgTos", "avgTor" ,
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")
    val layers = Array[Int](7, 5, 4, 2)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    // train the model
    val model = trainer.fit(trainData)
    // compute accuracy on the test set
    val predictionData = model.transform(testData)
    val accuracy = predictionData.select("prediction", "label").agg(AccuracyCalculation(predictionData("prediction"),
      predictionData("label") ).as("accuracy"))
  }

  def runNaiveBayes(sc: SparkContext, sqlContext: SQLContext, trainData: DataFrame,
                    testData: DataFrame): Unit ={
    val assembler = new VectorAssembler()
      .setInputCols(Array("countView", "maxTos", "maxTor",  "avgTos", "avgTor" ,
        "maxViewCount", "maxCatCnt"))
      .setOutputCol("features")

    val nb = new NaiveBayes().setPredictionCol("predictedLabel")
    val pipeline = new Pipeline()
      .setStages(Array(assembler, nb))
    val model = pipeline.fit(trainData)

    // Select example rows to display.
    val predictionData = model.transform(testData).select("guid","label", "predictedLabel")
    val accuracy = predictionData.agg( AccuracyCalculation(predictionData("predictedLabel"),
      predictionData("label") ).as("accuracy"))
    accuracy.show(false)

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
