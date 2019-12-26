package cn.moretv.doraemon.biz.portalRecommend

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.TransformUtils
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2018/8/24.
  */
object InterestRecommendOffline extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    TransformUDF.registerUDFSS
    val ss = spark
    import ss.implicits._
    //获得als推荐数据
    val alsRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("ALS/recommend")), -1, Constants.ARRAY_OPERATION_RANDOM).persist(StorageLevel.MEMORY_AND_DISK)
    //过滤曝光
    val frontPageExposedLongVideos = BizUtils.getContentImpression(7)
    val userWatchedLongVideos = BizUtils.getUserWatchedLongVideo(120)
    val filteredLongVideos = frontPageExposedLongVideos.union(userWatchedLongVideos).repartition(2000)
    //首页兴趣推荐离线数据，暂时
    val interestRecommend = BizUtils.uidSidFilter(alsRecommend, filteredLongVideos, "left", "black").persist()
    println(s"interestRecommend.count = ${interestRecommend.count()}")
    interestRecommend.printSchema()
    interestRecommend.show(30, false)

    val DF = interestRecommend.rdd.map(r => (r.getLong(0), r.getString(1)))
      .groupByKey().map(e => (e._1, e._2.toArray.take(60))).flatMap(e => e._2.map(x => (e._1, x)))
      .toDF("uid", "sid")

    //将兴趣推荐和首页的结果交换
    BizUtils.outputWrite(DF, "homePage/rec")

    println(s"DF.count = ${DF.count()}")
    DF.printSchema()
    DF.show(30, false)

    val interestDF = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("interest/offline")), 100, Constants.ARRAY_OPERATION_TAKE)
    val useeVideos = DataReader.read(BizUtils.getUsee4SearchMysqlPath("pre")).select("sid")
      .map(r => r.getAs[String]("sid")).collect().take(200)
    val bcUseeVideos = ss.sparkContext.broadcast(useeVideos)

    println(s"interestDF = ${interestDF.count()}")
    println(s"useevideos = ${useeVideos.size}")

    val userRiskPath = new HivePath("select a.user_id, b.dangerous_level " +
      "from dw_dimensions.dim_medusa_terminal_user a left join dw_dimensions.dim_web_location b " +
      "on a.web_location_sk = b.web_location_sk " +
      "where a.dim_invalid_time is null and b.dangerous_level > 0")
    val userRisk = DataReader.read(userRiskPath).map(e=>(TransformUtils.calcLongUserId(e.getString(0)),e.getInt(1))).toDF("uid","userRisk")

    BizUtils.recommend2Kafka4CouchbaseWithUseeVideo("p:i:", interestDF, userRisk, bcUseeVideos, "ALS", ConfigUtil.get("couchbase.moretv.topic"))
  }
}
