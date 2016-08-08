package vn.vccorp.adtech.bigdata.userbehavior.featureCalculation

/**
  * Created by cuonghn on 7/18/16.
  */
import java.util
import java.util.{ArrayList, Collections, Comparator}

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import MuachungPathParse._

//import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object GuidsIdMilisUDAF extends UserDefinedAggregateFunction{
  val encodeRatio = MuachungPathParse.idMaxRange / 1000
  //milis = 1466687415000 + id = 10138543 = > encodeIdMilis = 146668741510138543 if idMaxRange = 100m
  def encodeIdMilis(id : Int, milis : Long): Long={
    return id + encodeRatio * milis
  }
  // Schema you get as an input
  def inputSchema = new StructType().
    add("viewId", IntegerType).add("milis", LongType)


  // Schema of the row which is used for aggregation

  def bufferSchema = new StructType().add("idMilisList", DataTypes.createArrayType(LongType))

  // Returned type
  def dataType = DataTypes.createArrayType(IntegerType)

  // Self-explaining
  def deterministic = true

  // zero value

  // Similar to seqOp in aggregate
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0) && !input.isNullAt(1)){
      val longList = new ArrayList[Long](buffer.getList(0))
      val encodedIdMilis : Long = encodeIdMilis(input.getInt(0), input.getLong(1))
      longList.add(encodedIdMilis)
      buffer.update(0, longList)
    }
  }

  // true and false -> true
  // Similar to combOp in aggregate
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    val longList1 = new ArrayList[Long](buffer1.getList(0))
    val longList2 = new ArrayList[Long](buffer2.getList(0))

    longList1.addAll(longList2)
    buffer1.update(0, longList1)
  }

  // def initialize(buffer: MutableAggregationBuffer) = buffer.update(0,  Map[Int,Int]())
  def initialize(buffer: MutableAggregationBuffer) = buffer.update(0,  new ArrayList[Long]())

  // Called on exit to get return value
  // def evaluate(buffer: Row) = buffer.getMap(0)
  def evaluate(buffer: Row): ArrayList[Int] = {


    val idMilisList = new ArrayList[Long](buffer.getList(0))
    //Collections.sort(list)
    Collections.sort(idMilisList, new Comparator[Long]() {
      def compare(o1 : Long, o2 : Long):Int = {
        return o1.compareTo(o2);
      }
    })
    val idList = new util.ArrayList[Int]()
    val iter = idMilisList.iterator()
    while(iter.hasNext() ) {
      val id: Int = (iter.next() % MuachungPathParse.idMaxRange).toInt
      idList.add(id)
    }
    return idList
  }
}
