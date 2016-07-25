package vn.vccorp.adtech.bigdata.userbehavior

import scala.collection.mutable.{WrappedArray, Map}

/**
  * Created by cuonghn on 7/25/16.
  */
object ViewListAnalysis {
  def maxView(viewList : WrappedArray[Int]): Int ={
    var maxCount = 0
    var itemIdCount  = Map[Int, Int]()
    for (i <- viewList.indices){
      val itemId = getItemIdFromViewId(viewList(i))
      if (itemId > 0  ){
        if (!itemIdCount.contains(itemId)) itemIdCount += (itemId -> 0)
        val countId = itemIdCount(itemId) + 1
        itemIdCount(itemId) = countId
      }
    }
    val iter = itemIdCount.valuesIterator
    while (iter.hasNext){
      val nextCount = iter.next()
      if (nextCount > maxCount) maxCount = nextCount
    }

    return maxCount
  }

  def getItemIdFromViewId(viewId : Int): Int ={
    if (viewId > MuachungPathParse.itemIdMinRange && viewId < MuachungPathParse.idPaidMinRange ){
      return viewId % MuachungPathParse.itemIdMaxRange
    }
    return -1
  }
}
