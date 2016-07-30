package vn.vccorp.adtech.bigdata.userbehavior.featureCalculation

import utilities.NumericString.isNumeric

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


  final val itemIdMinRange = 1000 // < realitemId
  final val itemIdMaxRange = 1000000 // > realitemId
  final val idPaidMinRange = 10000000 // > editedItemId
  final val idMaxRange = 100000000 // <
  /**
    * Edited :
    *   hometypeID : 1
    *   timkiemID : 2
    *   danhmucID : (catId + 1) * itemIdMaxRange(1000000L)
    *   viewItemID : itemId + catId *  itemIdMaxRange
    *   paidItemID : itemId + idPaidMinRange(=max catId)
    *   others : 0
    */


  def  getEditedIdFromPath(path: String): Int = {

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
            val itemId = itemPath(itemPath.length - 2).toInt
            if (itemId < itemIdMaxRange ){
              return itemId + categoryIndex * itemIdMaxRange // 131493
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
          if (categoryIndex >= 0) return (categoryIndex + 1) * itemIdMaxRange
        }
      } //category
      if (arrayRealPath(1) == thanhtoan) {
        val pattern = itemPaymentRegex.findFirstIn(splitedPath(1)).toString()
        val splitedPattern = pattern.split(itemIdFindedSplitedChar)

        val itemPaymentId = splitedPattern(splitedPattern.length - 1)
        if (isNumeric(itemPaymentId)){
          return itemPaymentId.toInt + idPaidMinRange
        }
      } //category

      if (arrayRealPath(1) == canhan ||  arrayRealPath(1) == thongbao) return 0
    }
    //canhan ; thongbao

    return 0
  }
  def getCategoryIndex(categoryName : String):Int={
    for ( index <- 0 until categoryList.length){
      if (categoryList(index) == categoryName) return index
    }
    return -1
  }
  def getPaidListFromViewList(viewList : WrappedArray[Int] ):Array[Int]= {
    var paidList : List[Int]= Nil
    var editedId = 0
    //val seq : Seq[Long] = viewList
    for (i <- viewList.indices){
      editedId = viewList(i)
      if (editedId > idPaidMinRange){
        editedId -= idPaidMinRange
        paidList = paidList :+ editedId
      }
    }
    return paidList.toArray
  }
  /* AIL : return type for Array in Row: Schema for type java.util.ArrayList[Long] is not supported
  should use Array[Long], cause ArrayType is represented in a Row as a scala.collection.mutable.WrappedArray
  http://stackoverflow.com/questions/33390925/fail-to-get-an-arraydouble-in-sparks-dataframe

  def getPaidListFromViewListTest(viewList : WrappedArray[Long] ): util.ArrayList[Long]= {
    val paidList = new util.ArrayList[Long]()
    var editedId = 0L
    for (i <- viewList.indices){
      editedId = viewList(i)
      if (editedId > idPaidMinRange){
        editedId -= idPaidMinRange
        paidList.add(editedId)
      }
    }
    return paidList
  }*/

  def getLabelPaidOrNot(paidList: WrappedArray[Int] ) : Double={
    if (paidList.length > 0) return 1.toDouble
    return 0.toDouble
  }

  def getCategoryList(viewList : WrappedArray[Int]):Array[Int]={
    var categoryList  : List[Int]= Nil
    var editedId = 0
    var categoryId = 0
    for (i <- viewList.indices){
      editedId = viewList(i)
      if (editedId > itemIdMinRange && editedId <= idPaidMinRange){
        categoryId = (editedId -1) / itemIdMaxRange
        categoryList = categoryList :+ categoryId
      }
    }
    return categoryList.toArray
  }
}
