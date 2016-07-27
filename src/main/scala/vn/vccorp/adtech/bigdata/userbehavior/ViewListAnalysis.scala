package vn.vccorp.adtech.bigdata.userbehavior

import scala.collection.mutable.{WrappedArray, Map}

import vn.vccorp.adtech.bigdata.userbehavior.PaidItemViewCategoryCount._
/**
  * Created by cuonghn on 7/25/16.
  */
object ViewListAnalysis {
  def maxViewCount(viewList : WrappedArray[Int]): Int ={
    var maxCount = 0
    var itemIdCount  = Map[Int, Int]()
    for (i <- viewList.indices){
      val itemId = getItemIdFromViewId(viewList(i))
      if (itemId > 0  ){
        if (!itemIdCount.contains(itemId)) itemIdCount += (itemId -> 0)
        val count = itemIdCount(itemId) + 1
        itemIdCount(itemId) = count
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
      val itemId = viewId % MuachungPathParse.itemIdMaxRange
      if (itemId != 0) return itemId
    }
    return -1
  }



  def isPaidMaxView(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Boolean ={
    if (paidList.length == 0) return false
    val maxViewCnt = maxViewCount(viewList)
    for (i <- paidList.indices){
      if (countViewTotal(paidList(i), viewList) == maxViewCnt) return true
    }
    return false
  }

  //Category


  def maxCategoryCount(viewList : WrappedArray[Int]): Int ={
    var maxCount = 0
    var catIdCount  = Map[Int, Int]()
    for (i <- viewList.indices){
      val catId = getViewCategory(viewList(i))
      if (catId >= 0  ){
        if (!catIdCount.contains(catId)) catIdCount += (catId -> 0)
        val count = catIdCount(catId) + 1
        catIdCount(catId) = count
      }
    }
    val iter = catIdCount.valuesIterator
    while (iter.hasNext){
      val nextCount = iter.next()
      if (nextCount > maxCount) maxCount = nextCount
    }

    return maxCount
  }

  def isPaidMaxCategoryView(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Boolean ={
    if (paidList.length == 0) return false
    val maxCatCnt = maxCategoryCount(viewList)
    for (i <- paidList.indices){
      if (countCategoryTotal(paidList(i), viewList) == maxCatCnt) return true
    }
    return false
  }
}
