package vn.vccorp.adtech.bigdata.userbehavior

import org.apache.spark.SparkContext
import utilities.SystemInfo
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{avg, count, udf}

import utilities.TimeMeasurent
import vn.vccorp.adtech.bigdata.userbehavior.MuachungPathParse._
import vn.vccorp.adtech.bigdata.userbehavior.PaidItemViewCategoryCount._
import vn.vccorp.adtech.bigdata.userbehavior.ViewListAnalysis._

import scala.collection.mutable.WrappedArray
/**
  * Created by hncuong on 7/7/16.
  */
object GetFeature {
  final val systemInfo = SystemInfo.getConfiguration

  case class user(guid: String, domain: String, tos: Int, tor: Int)

  def getUserFeatures(sc: SparkContext, sqlContext: SQLContext, date:String): Unit ={
    import sqlContext.implicits._
    val timeMeasurent = new TimeMeasurent()
    println("Start page view analysis!!")

    val guidViewPaidList = getPageViewUserFeature(sc, sqlContext, date)
    //guidViewPaidList.show()
    guidViewPaidList.filter($"label" === true).show()
    //guidViewPaidList.printSchema()



    timeMeasurent.getDistanceAndRestart()
    println("Start time on site analysis!!")

    //time on site : muachung.vn -> (guid, count, avg(tos), avg(tor))
    val tos = getTimeOnSiteFeature(sc, sqlContext, date)
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

  }


  def getTimeOnSiteFeature(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    val tosDate = date.replaceAll("-", "")
    val tosPath = systemInfo.getString("timeonsite_path").replace("yyyyMMdd", tosDate)

    println("Start time on site analysis!! : " + tosPath)

    import sqlContext.implicits._
    val tos = sc.textFile(tosPath).map(_.split("\t")).map(u => user(u(0), u(2), u(4).toInt, u(5).toInt))
      .toDF().filter($"domain".like(systemInfo.getString("domain"))).groupBy("guid").
      agg(avg("tos"), avg("tor"))
    return tos
  }

  def getPageViewUserFeature(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    import sqlContext.implicits._
    println("Start pv analysis!! : " + date)
    val pageViewPath = systemInfo.getString("pageview_path").replace("yyyy-MM-dd", date)
    var pageView = sqlContext.read.load(pageViewPath).filter($"domain".like(systemInfo.getString("domain")))
      .select("guid", "path", "milis")

    //split itemId from path
    //sqlContext.udf.register("getItemIdFromPath", getEditedIdFromPath(_: String))
    val getItemIdFromPath = udf(getEditedIdFromPath(_: String))
    pageView = pageView.withColumn("itemId", getItemIdFromPath(pageView("path")) )

    //group by guid, agg count, idList sort by time(milis)
    pageView = pageView.groupBy("guid").agg(count("itemId").as("countView"),
      GroupToMapGuidsIdMilis($"itemId", $"milis").as("idList"))

    //get paidList from idList
    val getPaidList = udf(getPaidListFromViewList(_ : WrappedArray[Int]))
    pageView = pageView.withColumn("paidList", getPaidList($"idList"))

    //Label : paid - 1, not-paid - 0
    val getLabelOfPaid = udf(getLabelPaidOrNot(_ : WrappedArray[Int]))
    pageView = pageView.withColumn("label", getLabelOfPaid($"paidList"))

    // Item view before paid
    val getCountItemViewBeforePaidAverage = udf(countViewBeforePaidAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    pageView = pageView.withColumn("avgCountItemViewBeforePaid", getCountItemViewBeforePaidAverage($"paidList", $"idList" ))

    //item view all
    val getCountItemViewTotalAverage = udf(countViewTotalAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    pageView = pageView.withColumn("avgCountItemViewTotal", getCountItemViewTotalAverage($"paidList", $"idList" ))

    //category view before paid and all
    val getCountCatViewBeforePaidAverage = udf(countCategoryBeforePaidAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    val getCountCatViewTotalAverage = udf(countCategoryTotalAverage(_ : WrappedArray[Int],_ : WrappedArray[Int]))
    pageView = pageView.withColumn("avgCountCatViewBeforePaid", getCountCatViewBeforePaidAverage($"paidList", $"idList" ))
      .withColumn("avgCountCatViewTotal", getCountCatViewTotalAverage($"paidList", $"idList" ))

    //max view count
    val getMaxViewCount = udf(maxViewCount(_ : WrappedArray[Int]))
    val getMaxCatCount = udf(maxCategoryCount(_ : WrappedArray[Int]))
    val getIsMaxView = udf(isPaidMaxView(_ : WrappedArray[Int], _ : WrappedArray[Int]))
    val getIsMaxCat = udf(isPaidMaxCategoryView(_ : WrappedArray[Int], _ : WrappedArray[Int]))
    pageView = pageView.withColumn("maxViewCount",getMaxViewCount($"idList")).
      withColumn("maxCatCnt", getMaxCatCount($"idList")).
      withColumn("isMaxView", getIsMaxView($"paidList", $"idList")).
      withColumn("isMaxCat", getIsMaxCat($"paidList", $"idList"))

    return pageView
  }
}
