package vn.vccorp.adtech.bigdata.userbehavior

import org.apache.spark.SparkContext
import utilities.SystemInfo
import org.apache.spark.sql.{SQLContext, DataFrame, UserDefinedFunction}
import org.apache.spark.sql.functions.{avg, count, udf}
import utilities.TimeMeasurent
import scala.util.matching.Regex
/**
  * Created by hncuong on 7/7/16.
  */
object GetFeature {
  final val systemInfo = SystemInfo.getConfiguration
  final val timkiem = "tim-kiem.html"
  final val danhmuc = "danh-muc"
  final val canhan = "ca-nhan"
  final val thongbao = "thong-bao"
  final val thanhtoan = "thanh-toan.html"
  final val hanoi = "ha-noi"
  final val tphochiminh = "tp-ho-chi-minh"
  final val danang = "da-nang"
  final val haiphong = "hai-phong"
  final val idMaxRange = 1000000L
  case class user(guid: String, domain: String, tos: Int, tor: Int)

  def getUserFeatures(sc: SparkContext, sqlContext: SQLContext): Unit ={
    import sqlContext.implicits._
    val timeMeasurent = new TimeMeasurent()

    println("Start time on site analysis!!")
    //time on site : muachung.vn -> (guid, count, avg(tos), avg(tor))
    val tosPath = systemInfo.getString("timeonsite_path")

    val tos = sc.textFile(tosPath).map(_.split("\t")).map(u => user(u(0), u(2), u(4).toInt, u(5).toInt))
      .toDF().filter($"domain".like(systemInfo.getString("domain"))).groupBy("guid").agg(count("tos").as("count"),avg("tos"), avg("tor"))
    tos.show()
    /*pageview data
    root
 |-- dt: string (nullable = true)
 |-- cookietime: string (nullable = true)
 |-- browser_code: integer (nullable = true)
 |-- browser_ver: string (nullable = true)
 |-- os_code: integer (nullable = true)
 |-- os_version: string (nullable = true)
 |-- ip: long (nullable = true)
 |-- loc_id: integer (nullable = true)
 |-- domain: string (nullable = true)
 |-- siteId: string (nullable = true)
 |-- channelId: string (nullable = true)
 |-- path: string (nullable = true)
 |-- referer: string (nullable = true)
 |-- guid: string (nullable = true)
 |-- geo: integer (nullable = true)
 |-- org_ip: string (nullable = true)
 |-- pageloadId: string (nullable = true)
 |-- d_guid: string (nullable = true)
 |-- screen: string (nullable = true)
 |-- category: string (nullable = true)
 |-- utm_source: string (nullable = true)
 |-- utm_campaign: string (nullable = true)
 |-- utm_medium: string (nullable = true)
 |-- milis: long (nullable = true)
    * */
    timeMeasurent.getDistanceAndRestart()

    println("Start page view analysis!!")
    val pageViewPath = systemInfo.getString("pageview_path")
    //sqlContext.udf.register("getItemIdFromPath", getIdFromPath(_: String))
    val pageView = sqlContext.read.load(pageViewPath).filter($"domain".like(systemInfo.getString("domain")))
      .select("guid", "path", "milis")
    val getItemIdFromPath = udf(getIdFromPath(_: String))
    val pv = pageView.withColumn("itemId", getItemIdFromPath(pageView("path")) )
    //val pvv = pageView.select( getItemIdFromPath(pageView("path")))
    val encodeIdMilis = udf((itemId : Long, milis : Long) => itemId + 10000 * milis)
    val pvEncoded = pv.withColumn("idMilis", encodeIdMilis($"itemId", $"milis")).select("guid", "idMilis")
    val guidViewList = pvEncoded.groupBy("guid").agg(GroupToMapGuids($"idMilis").as("idList"))
    guidViewList.show()
    timeMeasurent.getDistanceAndRestart()
    println("Paiduser analysis!!")

    val paidGuid = pv.filter($"itemId" > idMaxRange)
    paidGuid.show()
    //
    val paidGuidViewList = guidViewList.filter(paidGuid("guid").contains(guidViewList("guid")))
    paidGuidViewList.show(false)

  }
  def getITemIdFromItemPaymentId(itemPaymentId : Long) = itemPaymentId - idMaxRange

  def isNumeric(input: String): Boolean = {
    var isNumeric = false
    if (input.length > 0){
      isNumeric = input.forall(_.isDigit)
    }
    return isNumeric
  }
  // from path : thanhtoan = 0 ; home = 1
  def  getIdFromPath(path: String): Long = {
    val itemPaymentRegex = "item_id=\\d{3,6}".r
    val splitedPath = path.split("[?]")
    val realPath = splitedPath(0) // realPath == /danh-muc
    val arrayRealPath = realPath.split("[/]")
    if (arrayRealPath.length == 0) return 1 // homepage : path = "/"
    // arrayRealPath(0) == ""
    if (arrayRealPath.length >= 2){
      //val path1 = arrayRealPath(1)
      if (arrayRealPath(1) == haiphong || arrayRealPath(1) == hanoi || arrayRealPath(1) == tphochiminh) return 1
      if (arrayRealPath(1) == timkiem) return 2 //tim-kiem
      if (arrayRealPath(1) == danhmuc) return 3 //category
      if (arrayRealPath(1) == thanhtoan) {
        val pattern = itemPaymentRegex.findFirstIn(splitedPath(1)).toString()
        val splitedPattern = pattern.split("[=)]")

        val itemPaymentId = splitedPattern(splitedPattern.length - 1)
        if (isNumeric(itemPaymentId)){
          return itemPaymentId.toLong + idMaxRange
        }
      } //category

      if (arrayRealPath(1) == canhan ||  arrayRealPath(1) == thongbao) return 0
    }
     //canhan ; thongbao
    if (arrayRealPath.length == 3){
      val itemPath = arrayRealPath(2).split("[-.]")
      if (itemPath.length >= 2){
        //  buffet-lau-nuong-khong-khoi-gri-gri-menu-cao-cap-131493.html
        if (isNumeric(itemPath(itemPath.length - 2))){
          val itemId = itemPath(itemPath.length - 2).toLong
          if (itemId < 1000000L ){
            return itemId // 131493
          }
        }
      }
    }
    return 0
  }
}
