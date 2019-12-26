package cn.moretv.doraemon.biz.frontPage

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.CouchbasePath
import cn.moretv.doraemon.common.util.DateUtils
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2018/8/16.
  * Updated by Edison_Zhou on 2019/5/17
  */
object FrontPageUnionWithGouzheng extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    val portalRecommendDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/rec"))
    val homePageHotDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/hot"))
    val homePageVipDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/vip"))
    val homePageGouzhengDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/gouzheng")).toDF("uid", "id")

    val gouzhengSortedUser = homePageGouzhengDF.select("uid").map(r => r.getLong(0)).collect().sorted
    val gouzhengUserNum = gouzhengSortedUser.length
    val gouzhengUserWithGZAlg = gouzhengSortedUser.take(gouzhengUserNum/2)
    val gouzhengUserWithALS = gouzhengSortedUser.take(gouzhengUserNum-gouzhengUserNum/2)

    val gouzhengWithGZAlgDF = sc.parallelize(gouzhengUserWithGZAlg).toDF("gouzhengUserWithGZAlg").
      selectExpr("cast(gouzhengUserWithGZAlg as string) as uid")
    val gouzhengWithALSDF = sc.parallelize(gouzhengUserWithALS).toDF("gouzhengUserWithALS").
      selectExpr("cast(gouzhengUserWithALS as string) as uid")

    val vipFoldDF = homePageVipDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").
      selectExpr("cast(uid as string) as uid", "id")
    BizUtils.getDataFrameInfo(vipFoldDF, "vipFoldDF")

    val ALSNewAlgFoldDF = vipFoldDF.join(gouzhengWithALSDF, "uid")
    BizUtils.getDataFrameInfo(ALSNewAlgFoldDF, "ALSNewAlgFoldDF")

    val gouzhengFoldDF = homePageGouzhengDF.selectExpr("cast(uid as string) as uid", "id")
    val GZAlgFoldDF = gouzhengFoldDF.join(gouzhengWithGZAlgDF, "uid")
    BizUtils.getDataFrameInfo(GZAlgFoldDF, "GZAlgFoldDF")

    val vipWithoutGouzhengFoldDF = vipFoldDF.as("a").join(gouzhengFoldDF.as("b"), expr("a.uid = b.uid"),
      "left").filter("b.uid is null").selectExpr("a.uid", "a.id")
    BizUtils.getDataFrameInfo(vipWithoutGouzhengFoldDF, "vipWithoutGouzhengFoldDF")

    val portalFoldDF = portalRecommendDF.groupBy("uid").agg(collect_list("sid")).
      toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    val hotFoldDF = homePageHotDF.groupBy("uid").agg(collect_list("sid")).
      toDF("uid", "id").selectExpr("cast(uid as string) as uid", "id")
    BizUtils.getDataFrameInfo(portalFoldDF, "portalFoldDF")
    BizUtils.getDataFrameInfo(hotFoldDF, "hotFoldDF")

    val dataPackParam1 = new DataPackParam
    dataPackParam1.format = FormatTypeEnum.KV
    dataPackParam1.extraValueMap = Map("alg" -> "mix-ACSS")
    val portalPackDF = DataPack.pack(portalFoldDF, dataPackParam1).toDF("key", "portal")
    BizUtils.getDataFrameInfo(portalPackDF, "portalPackDF")
//
    val dataPackParam2 = new DataPackParam
    dataPackParam2.format = FormatTypeEnum.KV
    dataPackParam2.extraValueMap = Map("alg" -> "gouzhengRec")
    val GZAlgPackDF = DataPack.pack(GZAlgFoldDF, dataPackParam2).toDF("key", "vip")
    BizUtils.getDataFrameInfo(GZAlgPackDF, "GZAlgPackDF")

    val dataPackParam2Plus = new DataPackParam
    dataPackParam2Plus.format = FormatTypeEnum.KV
    dataPackParam2Plus.extraValueMap = Map("alg" -> "ALS_New")
    val ALSNewAlgPackDF = DataPack.pack(ALSNewAlgFoldDF, dataPackParam2Plus).toDF("key", "vip")
    BizUtils.getDataFrameInfo(ALSNewAlgPackDF, "ALSNewAlgPackDF")
//
    val dataPackParam3 = new DataPackParam
    dataPackParam3.format = FormatTypeEnum.KV
    dataPackParam3.extraValueMap = Map("alg" -> "ALS")
    val vipWithoutGouzhengPackDF = DataPack.pack(vipWithoutGouzhengFoldDF, dataPackParam3).toDF("key", "vip")
    BizUtils.getDataFrameInfo(vipWithoutGouzhengPackDF, "vipWithoutGouzhengPackDF")
//
    val vipPackDF = GZAlgPackDF.union(ALSNewAlgPackDF).union(vipWithoutGouzhengPackDF)
    BizUtils.getDataFrameInfo(vipPackDF, "vipPackDF")
//
    val dataPackParam4 = new DataPackParam
    dataPackParam4.format = FormatTypeEnum.KV
    dataPackParam4.extraValueMap = Map("alg" -> "random")
    val hotPackDF = DataPack.pack(hotFoldDF, dataPackParam4).toDF("key", "hot")
    BizUtils.getDataFrameInfo(hotPackDF, "hotPackDF")
//
    val dataPackParam5 = new DataPackParam
    dataPackParam5.format = FormatTypeEnum.KV
    dataPackParam5.keyPrefix = "p:a:"
    dataPackParam5.extraValueMap = Map("date" -> DateUtils.getTimeStamp)
    val unionDF = portalPackDF.as("a").join(vipPackDF.as("b"), expr("a.key = b.key"), "full").join(hotPackDF.as("c"), expr("a.key = c.key"), "full")
      .selectExpr("case when a.key is not null then a.key when b.key is not null then b.key else c.key end as key", "a.portal as portal", "b.vip as vip", "c.hot as hot")
//
    val recommend = DataPack.pack(unionDF, dataPackParam5)

    BizUtils.getDataFrameInfo(unionDF, "unionDF")
    BizUtils.getDataFrameInfo(recommend, "recommend")
//
    val dataWriter = new DataWriter2Kafka
    dataWriter.write(recommend, new CouchbasePath(ConfigUtil.get("couchbase.moretv.topic"), 1728000))
  }
}
