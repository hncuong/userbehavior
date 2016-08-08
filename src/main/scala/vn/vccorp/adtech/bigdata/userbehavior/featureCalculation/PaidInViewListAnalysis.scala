package vn.vccorp.adtech.bigdata.userbehavior.featureCalculation

import MuachungPathParse._

import scala.collection.mutable.WrappedArray

/**
  * Created by cuonghn on 7/25/16.
  */
object PaidInViewListAnalysis {

  /**
    * view : itemId = paidId
    */
  // paid : 10 ; view :catId [0 - 9]; else -1
  def checkIfItemIsView(paidItem : Int, viewId : Int): Int ={
    val diffId = viewId - paidItem
    if (diffId % MuachungPathParse.itemIdMaxRange == 0 ){
      return diffId / MuachungPathParse.itemIdMaxRange
    }
    return -1
  }

  def countViewBeforePaid(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    var count = 0
    for (i <- viewList.indices){
      val checkViewFlag = checkIfItemIsView(paidItem, viewList(i))
      if (checkViewFlag == 10) return count
      if (checkViewFlag >= 0)  count += 1
    }
    return count
  }

  def countViewBeforePaidAverage(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Float ={
    val paidListLength = paidList.length
    if (paidListLength == 0 ) return 0.toFloat
    var totalCount = 0
    for (i <- paidList.indices){
      totalCount += countViewBeforePaid(paidList(i), viewList)
    }
    return totalCount.toFloat / paidListLength
  }

  def countViewTotal(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    var count = 0
    for (i <- viewList.indices){
      val checkViewFlag = checkIfItemIsView(paidItem, viewList(i))
      if (checkViewFlag >= 0 && checkViewFlag < 10) count += 1
    }
    return count
  }



  def countViewTotalAverage(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Float ={
    val paidListLength = paidList.length
    if (paidListLength == 0 ) return 0.toFloat
    var totalCount = 0
    for (i <- paidList.indices){
      totalCount += countViewTotal(paidList(i), viewList)
    }
    return totalCount.toFloat / paidListLength
  }

  /**
    * view : cat(viewId) = cat(paidId)
    */

  def getCategoryOfPaidItem(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    for (i <- viewList.indices){
      val checkViewFlag = checkIfItemIsView(paidItem, viewList(i))
      if (checkViewFlag >= 0 && checkViewFlag < 10) return checkViewFlag
    }
    return -1
  }

  def getViewCategory(viewId : Int): Int ={
    if (viewId > MuachungPathParse.itemIdMinRange && viewId <= MuachungPathParse.idPaidMinRange){
      return (viewId - 1) / MuachungPathParse.itemIdMaxRange
    }
    return -1
  }

  def countCategoryBeforePaid(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    val categoryId = getCategoryOfPaidItem(paidItem, viewList)
    if (categoryId == -1) return 0
    var count = 0
    for (i <- viewList.indices){
      if (checkIfItemIsView(paidItem, viewList(i)) == 10) return count
      if (getViewCategory(viewList(i)) == categoryId) count += 1
    }
    return count
  }

  def countCategoryBeforePaidAverage(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Float ={
    val paidListLength = paidList.length
    if (paidListLength == 0 ) return 0.toFloat
    var totalCount = 0
    for (i <- paidList.indices){
      totalCount += countCategoryBeforePaid(paidList(i), viewList)
    }
    return totalCount.toFloat / paidListLength
  }

  def countCategoryTotal(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    val categoryId = getCategoryOfPaidItem(paidItem, viewList)
    if (categoryId == -1) return 0
    var count = 0
    for (i <- viewList.indices){
      if (getViewCategory(viewList(i)) == categoryId) count += 1
    }
    return count
  }


  //what about session? separate by paid or?

  def countCategoryTotalAverage(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Float ={
    val paidListLength = paidList.length
    if (paidListLength == 0 ) return 0.toFloat
    var totalCount = 0
    for (i <- paidList.indices){
      totalCount += countCategoryTotal(paidList(i), viewList)
    }
    return totalCount.toFloat / paidListLength
  }

  /**
    * view all : before paid
    */
  def countAllViewBeforePaid(paidItem : Int, viewList : WrappedArray[Int]): Int ={
    var count = 0
    for (i <- viewList.indices){
      if (checkIfItemIsView(paidItem, viewList(i)) == 10) return count
      if (viewList(i) > MuachungPathParse.idPaidMinRange) count = 0 //reset by sessions : separated by paid
      if (viewList(i) > MuachungPathParse.itemIdMinRange && viewList(i) <= MuachungPathParse.idPaidMinRange) count += 1
    }
    return count
  }


  def countAllViewBeforePaidAverage(paidList : WrappedArray[Int], viewList : WrappedArray[Int]): Float ={
    val paidListLength = paidList.length
    if (paidListLength == 0 ) return 0.toFloat
    var totalCount = 0
    for (i <- paidList.indices){
      totalCount += countAllViewBeforePaid(paidList(i), viewList)
    }
    return totalCount.toFloat / paidListLength
  }

}
