package vn.vccorp.adtech.bigdata.userbehavior.featureCalculation

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{avg, count, udf, max, min}
import org.apache.spark.sql.{DataFrame, SQLContext}
import utilities.{SystemInfo, TimeMeasurent, TimeUtils}
import MuachungPathParse._
import PaidInViewListAnalysis._
import MaxViewCalculation._


import scala.collection.mutable.WrappedArray
/**
  * Created by hncuong on 7/7/16.
  */
//data
/*guid|countView|              idList|paidList|label|avgCountItemViewBeforePa
id|avgCountItemViewTotal|avgCountCatViewBeforePaid|avgCountCatViewTotal|maxViewCount|maxCat
Cnt|isMaxView|isMaxCat|  maxTos |        avgTos|       maxTor|   avgTor|*/
object FeatureCalculation {
  final val systemInfo = SystemInfo.getConfiguration

  case class user(guid: String, domain: String, path: String, tos: Int, tor: Int, dt: String)

  def getUserFeatures(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame ={
    import sqlContext.implicits._
    /*val timeMeasurent = new TimeMeasurent()
    println("Start get feature date: " + date)*/

    val viewInfo = getViewInfo(sc, sqlContext, date)

    //pvFeature.filter($"label" === 1.0).groupBy("isMaxCat", "isMaxView").agg(count("guid")).show()
    //guidViewPaidList.show()
    //userFeature.filter($"label" === 1.0).show()
    //guidViewPaidList.printSchema()



    //timeMeasurent.getDistanceAndRestart()
   // println("Start time on site analysis!!")

    //time on site : muachung.vn -> (guid, count, avg(tos), avg(tor))
    //val tos = getTimeOnSiteFeature(sc, sqlContext, date)
    //tos.show()

    //val userFeature = pvFeature.join(tos, "guid")
    viewInfo.groupBy("label").agg(avg("countView"), avg("avgCountItemViewBeforePaid"),
      avg("avgCountItemViewTotal"), avg("avgCountCatViewBeforePaid"),
      avg("avgCountCatViewTotal"), avg("maxViewCount"), avg("maxCatCnt"),
      avg("maxTos"), avg("maxTor"), avg("avgTos"), avg("avgTor")).show(false)
    //userFeature.show()
    /*println("get feature: Done !! date: " + date)
    timeMeasurent.getDistanceAndRestart()*/
    return viewInfo
  }


  def getTimeOnSiteFeature(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    val tosDate = date.replaceAll("-", "")
    val tosPath = systemInfo.getString("timeonsite_path").replace("yyyyMMdd", tosDate)

    //println("Start time on site analysis!! : " + tosPath)

    import sqlContext.implicits._
    val tos = sc.textFile(tosPath).map(_.split("\t")).map(u => user(u(0), u(2), u(3), u(4).toInt, u(5).toInt, u(6)))
      .toDF().filter($"domain".like(systemInfo.getString("domain"))).groupBy("guid").
      agg(max("tos").as("maxTos"),avg("tos").as("avgTos"),max("tor").as("maxTor"), avg("tor").as("avgTor"))
    return tos
  }

  def getViewInfo(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    import sqlContext.implicits._
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
    val tosDate = date.replaceAll("-", "")
    val tosPath = systemInfo.getString("timeonsite_path").replace("yyyyMMdd", tosDate)

    var viewInfo = sc.textFile(tosPath).map(_.split("\t")).map(u => user(u(0), u(2), u(3), u(4).toInt, u(5).toInt, u(6)))
      .toDF().filter($"domain".like(systemInfo.getString("domain")))
    val convertDatetimeToMiliseconds = udf(TimeUtils.stringToMilices(_: String))
    viewInfo = viewInfo.withColumn("milis",convertDatetimeToMiliseconds($"dt"))

    //println("Start pv analysis!! : " + date)
    /*val pageViewPath = systemInfo.getString("pageview_path").replace("yyyy-MM-dd", date)
    var viewInfo = sqlContext.read.load(pageViewPath).filter($"domain".like(systemInfo.getString("domain")))
      .select("guid", "path", "milis")*/

    //split itemId from path
    //sqlContext.udf.register("getItemIdFromPath", getEditedIdFromPath(_: String))
    val getItemIdFromPath = udf(getEditedIdFromPath(_: String))
    viewInfo = viewInfo.withColumn("itemId", getItemIdFromPath(viewInfo("path")) )

    //group by guid, agg count, idList sort by time(milis)
    viewInfo = viewInfo.groupBy("guid").agg(count("itemId").as("countView"),
      GuidsIdMilisUDAF($"itemId", $"milis").as("idList"))

    //get paidList from idList
    val getPaidList = udf(getPaidListFromViewList(_ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("paidList", getPaidList($"idList"))

    //Label : paid - 1, not-paid - 0
    val getLabelOfPaid = udf(getLabelPaidOrNot(_ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("label", getLabelOfPaid($"paidList"))

    // Item view before paid
    val getCountItemViewBeforePaidAverage = udf(countViewBeforePaidAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("avgCountItemViewBeforePaid", getCountItemViewBeforePaidAverage($"paidList", $"idList" ))

    //item view all
    val getCountItemViewTotalAverage = udf(countViewTotalAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("avgCountItemViewTotal", getCountItemViewTotalAverage($"paidList", $"idList" ))

    //category view before paid and all
    val getCountCatViewBeforePaidAverage = udf(countCategoryBeforePaidAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    val getCountCatViewTotalAverage = udf(countCategoryTotalAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("avgCountCatViewBeforePaid", getCountCatViewBeforePaidAverage($"paidList", $"idList" ))
      .withColumn("avgCountCatViewTotal", getCountCatViewTotalAverage($"paidList", $"idList" ))

    //max view count
    val getMaxViewCount = udf(maxViewCount(_ : WrappedArray[Int]))
    val getMaxCatCount = udf(maxCategoryCount(_ : WrappedArray[Int]))
    val getIsMaxView = udf(isPaidMaxView(_ : WrappedArray[Int], _ : WrappedArray[Int]))
    val getIsMaxCat = udf(isPaidMaxCategoryView(_ : WrappedArray[Int], _ : WrappedArray[Int]))
    viewInfo = viewInfo.withColumn("maxViewCount",getMaxViewCount($"idList")).
      withColumn("maxCatCnt", getMaxCatCount($"idList")).
      withColumn("isMaxView", getIsMaxView($"paidList", $"idList")).
      withColumn("isMaxCat", getIsMaxCat($"paidList", $"idList"))

    return viewInfo
  }
}
