package vn.vccorp.adtech.bigdata.userbehavior

/**
  * Created by cuonghn on 7/18/16.
  */
import java.util
import java.util.{ArrayList, Collections, Comparator}

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.expressions.MutableAggregationBuffer

//import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object GroupToMapGuids extends UserDefinedAggregateFunction{
  val encodeRatio = MuachungPathParse.idMaxRange / 1000L
  //milis = 1466687415000 + id = 10138543 = > encodeIdMilis = 146668741510138543 if idMaxRange = 100m
  def encodeIdMilis(id : Long, milis : Long): Long={
    return id + encodeRatio * milis
  }
  // Schema you get as an input
  def inputSchema = new StructType().
    add("idMilis", LongType).add("milis", LongType)


  // Schema of the row which is used for aggregation

  def bufferSchema = new StructType().add("longList", DataTypes.createArrayType(LongType))

  // Returned type
  def dataType = DataTypes.createArrayType(LongType)

  // Self-explaining
  def deterministic = true

  // zero value

  // Similar to seqOp in aggregate
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0)){
      val longList = new ArrayList[Long](buffer.getList(0))
      val encodedIdMilis : Long = encodeIdMilis(input.getLong(0), input.getLong(1))
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
  def evaluate(buffer: Row): ArrayList[Long] = {


    val idMilisList = new ArrayList[Long](buffer.getList(0))
    //Collections.sort(list)
    Collections.sort(idMilisList, new Comparator[Long]() {
      def compare(o1 : Long, o2 : Long):Int = {
        return o1.compareTo(o2);
      }
    })
    var idList = new util.ArrayList[Long]()
    val iter = idMilisList.iterator()
    while(iter.hasNext() ) {
      val id: Long = iter.next() % MuachungPathParse.idMaxRange
      idList.add(id)
    }
    return idList
  }
}
