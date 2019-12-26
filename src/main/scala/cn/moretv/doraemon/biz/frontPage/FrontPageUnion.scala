package cn.moretv.doraemon.biz.frontPage

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HdfsPath}
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils, TransformUtils}
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2018/8/16.
  */
object FrontPageUnion extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val portalRecommendDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/rec"))
    val homePageVipDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/vip"))
    val homePageHotDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/hot"))

    BizUtils.getDataFrameInfo(portalRecommendDF, "portalRecommendDF")
    BizUtils.getDataFrameInfo(homePageVipDF, "homePageVipDF")
    BizUtils.getDataFrameInfo(homePageHotDF, "homePageHotDF")

    val portalFoldDF = portalRecommendDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val vipFoldDF = homePageVipDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val hotFoldDF = homePageHotDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")

    BizUtils.getDataFrameInfo(portalFoldDF, "portalFoldDF")
    BizUtils.getDataFrameInfo(vipFoldDF, "vipFoldDF")
    BizUtils.getDataFrameInfo(hotFoldDF, "hotFoldDF")

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> "mix-ACSS")
    val portalPackDF = DataPack.pack(portalFoldDF, dataPackParam1).toDF("key", "portal")

    BizUtils.getDataFrameInfo(portalPackDF, "portalPackDF")

    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> "ALS")
    val vipPackDF = DataPack.pack(vipFoldDF, dataPackParam2).toDF("key", "vip")

    BizUtils.getDataFrameInfo(vipPackDF, "vipPackDF")

    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> "random")
    val hotPackDF = DataPack.pack(hotFoldDF, dataPackParam3).toDF("key", "hot")

    BizUtils.getDataFrameInfo(hotPackDF, "hotPackDF")

    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.keyPrefix = "p:a:"
    dataPackParam4.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val unionDF = portalPackDF.as("a").join(vipPackDF.as("b"), expr("a.key = b.key"), "full").join(hotPackDF.as("c"), expr("a.key = c.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key else c.key end as key", "a.portal as portal", "b.vip as vip", "c.hot as hot")

    val recommend = DataPack.pack(unionDF, dataPackParam4)

    BizUtils.getDataFrameInfo(unionDF, "unionDF")
    BizUtils.getDataFrameInfo(recommend, "recommend")

    val dataWriter = new DataWriter2Kafka
    dataWriter.write(recommend, new CouchbasePath(ConfigUtil.get("couchbase.moretv.topic"), 1728000))
    //dataWriter.write(defaultRecommend, new CouchbasePath("couchbase-moretv", 1728000))
  }
}
