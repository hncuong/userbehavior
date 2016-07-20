package vn.vccorp.adtech.bigdata.userbehavior

import java.util.ArrayList

import org.apache.spark.SparkContext
import utilities.SystemInfo
import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}
import org.apache.spark.sql.functions.{avg, count, udf}
import utilities.TimeMeasurent
import vn.vccorp.adtech.bigdata.userbehavior.MuachungPathParse.{getEditedIdFromPath, getPaidListFromViewList, getLabelPaidOrNot}

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
    println("Start page view analysis!!")

    val guidViewList = getPageViewUserFeature(sc, sqlContext, date)
    guidViewList.show(false)
    guidViewList.printSchema()

    timeMeasurent.getDistanceAndRestart()
    println("Paiduser analysis!!")

  }


  def getTimeOnSiteFeature(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    val tosDate = date.replaceAll("-", "")
    val tosPath = systemInfo.getString("timeonsite_path").replace("yyyyMMdd", tosDate)

    println("Start time on site analysis!! : " + tosPath)

    import sqlContext.implicits._
    val tos = sc.textFile(tosPath).map(_.split("\t")).map(u => user(u(0), u(2), u(4).toInt, u(5).toInt))
      .toDF().filter($"domain".like(systemInfo.getString("domain"))).groupBy("guid").
      agg(count("tos").as("count"),avg("tos"), avg("tor"))
    return tos
  }

  def getPageViewUserFeature(sc: SparkContext, sqlContext: SQLContext, date:String): DataFrame={
    import sqlContext.implicits._
    println("Start pv analysis!! : " + date)
    val pageViewPath = systemInfo.getString("pageview_path").replace("yyyy-MM-dd", date)
    val pageView = sqlContext.read.load(pageViewPath).filter($"domain".like(systemInfo.getString("domain")))
      .select("guid", "path", "milis")

    //sqlContext.udf.register("getItemIdFromPath", getIdFromPath(_: String))
    val getItemIdFromPath = udf(getEditedIdFromPath(_: String))
    val pv = pageView.withColumn("itemId", getItemIdFromPath(pageView("path")) )

    val guidViewList = pv.groupBy("guid").agg(GroupToMapGuids($"itemId", $"milis").as("idList"))

    val getPaidList = udf(getPaidListFromViewList(_ : WrappedArray[Long]))
    val guidViewPaidList = guidViewList.withColumn("paidList", getPaidList($"idList"))

    val getLabelOfPaid = udf(getLabelPaidOrNot(_ : WrappedArray[Long]))
    val guidViewPaidLabel = guidViewPaidList.withColumn("labelOfPaid", getLabelOfPaid($"paidList"))

    return guidViewPaidLabel
  }
}
