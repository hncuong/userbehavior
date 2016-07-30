package vn.vccorp.adtech.bigdata.userbehavior.machineLearning

/**
  * Created by cuonghn on 7/18/16.
  */
import java.util
import java.util.{ArrayList, Collections, Comparator}

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}

//import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object AccuracyCalculation extends UserDefinedAggregateFunction{

  // Schema you get as an input
  def inputSchema = new StructType().
    add("prediction", DoubleType).add("label", DoubleType)


  // Schema of the row which is used for aggregation

  def bufferSchema = new StructType().add("TP", LongType )
    .add("FN", LongType).add("TN", LongType).add("FP", LongType)

  // Returned type
  def dataType = DataTypes.createArrayType(LongType)

  // Self-explaining
  def deterministic = true

  // zero value

  // Similar to seqOp in aggregate
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0) && !input.isNullAt(1)){
      if (input.getDouble(0) == 1.0){
        if (input.getDouble(1) == 1.0) buffer(0) = buffer.getLong(0) + 1  //
        else buffer(3) = buffer.getLong(3) + 1
      } else {
        if (input.getDouble(1) == 0.0) buffer(2) = buffer.getLong(2) + 1
        else buffer(1) = buffer.getLong(1) + 1
      }
    }
  }

  // true and false -> true
  // Similar to combOp in aggregate
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    buffer1(2) = buffer1.getLong(2) + buffer2.getLong(2)
    buffer1(3) = buffer1.getLong(3) + buffer2.getLong(3)

  }

  // def initialize(buffer: MutableAggregationBuffer) = buffer.update(0,  Map[Int,Int]())
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0L
    buffer(1) = 0L
    buffer(2) = 0L
    buffer(3) = 0L

  }

  // Called on exit to get return value
  // def evaluate(buffer: Row) = buffer.getMap(0)
  def evaluate(buffer: Row): ArrayList[Long] = {
    /*val idList = new util.ArrayList[Double]()
    val recall = buffer.getLong(0).toDouble / (buffer.getLong(0) + buffer.getLong(1))
    val precision = buffer.getLong(0).toDouble / (buffer.getLong(0) + buffer.getLong(3))
    idList.add(recall)
    idList.add(precision)*/
    val accuracy = new util.ArrayList[Long]()
    for (i <- 0 until 4){
      accuracy.add(buffer.getLong(i))
    }
    return accuracy
  }
}
