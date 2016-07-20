package vn.vccorp.adtech.bigdata.userbehavior
import java.util

import utilities.NumericString.isNumeric
import java.util.ArrayList
import scala.collection.mutable.WrappedArray

//import scala.List

/**
  * Created by cuonghn on 7/20/16.
  */
object MuachungPathParse {
  final val timkiem = "tim-kiem.html"
  final val danhmuc = "danh-muc"
  final val canhan = "ca-nhan"
  final val thongbao = "thong-bao"
  final val thanhtoan = "thanh-toan.html"
  final val hanoi = "ha-noi"
  final val tphochiminh = "tp-ho-chi-minh"
  final val danang = "da-nang"
  final val haiphong = "hai-phong"
  final val itemPaymentRegex = "item_id=\\d{3,6}".r
  final val pathSplitedChar = "[?]"
  final val realpathSplitedChar = "[/]"
  final val itemPathSplitedChar = "[-.]"
  final val itemIdFindedSplitedChar = "[=)]"


  final val categoryList : List[String] = List("am-thuc-nha-hang"/*0*/, "spa-lam-dep"/*1*/, "gia-dung-noi-that"/*2*/,
    "thoi-trang-nu"/*3*/, "thoi-trang-nam"/*4*/, "phu-kien-my-pham"/*5*/, "dien-tu-cong-nghe"/*6*/, "thuc-pham"/*7*/
    , "me-be"/*8*/, "dao-tao-giai-tri"/*9*/)

  final val idMaxRange = 1000000L
  final val idPaidMinRange = 10000000L
  final val idEditedMaxRange = 100000000L

  def  getEditedIdFromPath(path: String): Long = {

    val splitedPath = path.split(pathSplitedChar)
    val realPath = splitedPath(0) // realPath == /danh-muc
    val arrayRealPath = realPath.split(realpathSplitedChar)
    if (arrayRealPath.length == 0) return 1 // homepage : path = "/"

    if (arrayRealPath.length == 3){
      val categoryIndex = getCategoryIndex(arrayRealPath(1))
      if (categoryIndex >= 0){
        val itemPath = arrayRealPath(2).split(itemPathSplitedChar)
        if (itemPath.length >= 2){
          //  /am-thuc-nha-hang/thoa-thich-an-buffet-cao-cap-ngam-ho-guom-138543.html
          if (isNumeric(itemPath(itemPath.length - 2))){
            val itemId = itemPath(itemPath.length - 2).toLong
            if (itemId < idMaxRange ){
              return itemId + categoryIndex * idMaxRange // 131493
            }
          }
        }
      }
    }

    // arrayRealPath(0) == ""
    if (arrayRealPath.length >= 2){
      //val path1 = arrayRealPath(1)
      if (arrayRealPath(1) == haiphong || arrayRealPath(1) == hanoi || arrayRealPath(1) == tphochiminh) return 1
      if (arrayRealPath(1) == timkiem) return 2 //tim-kiem
      if (arrayRealPath(1) == danhmuc) {
        if (arrayRealPath.length >=4) {
          val categoryName = arrayRealPath(3).split("[.]")(0)
          val categoryIndex = getCategoryIndex(categoryName)
          if (categoryIndex >= 0) return (categoryIndex + 1) * idMaxRange
        }
      } //category
      if (arrayRealPath(1) == thanhtoan) {
        val pattern = itemPaymentRegex.findFirstIn(splitedPath(1)).toString()
        val splitedPattern = pattern.split(itemIdFindedSplitedChar)

        val itemPaymentId = splitedPattern(splitedPattern.length - 1)
        if (isNumeric(itemPaymentId)){
          return itemPaymentId.toLong + idPaidMinRange
        }
      } //category

      if (arrayRealPath(1) == canhan ||  arrayRealPath(1) == thongbao) return 0
    }
    //canhan ; thongbao

    return 0
  }
  def getCategoryIndex(categoryName : String):Int={
    var index = 0
    for ( index <- 0 until categoryList.length){
      if (categoryList(index) == categoryName) return index
    }
    return -1
  }
  def getPaidListFromViewList(viewList : WrappedArray[Long] ): Array[Long]= {
    val paidList : List[Long]= Nil
    var editedId = 0L
    for (i <- viewList.indices){
      editedId = viewList(i)
      if (editedId > idPaidMinRange){
        editedId -= idPaidMinRange
        paidList :+ editedId
      }
    }

    //var viewId
    /*val iter = viewList.
    while (iter.hasNext){
      var editedId = iter.next()
      if (editedId > idPaidMinRange){
        editedId -= idPaidMinRange
        paidList.add(editedId)
      }
    }*/
    val re = paidList.toArray
    return re
  }
}
