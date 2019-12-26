package cn.moretv.doraemon.biz.defaultRecommend

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath, RedisPath}
import cn.moretv.doraemon.common.util.ArrayUtils
import cn.moretv.doraemon.data.writer.DataWriter2Kafka
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.storage.StorageLevel

/**
  * Created by guohao on 2018/12/6.
  * 详情页默认推荐
  * 热门推荐、相似影片、vip推荐之间做去重
  */
object DetailDefaultRecommend extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit ={
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()

    //有效影片数据
    val validSid = BizUtils.getAvailableVideo("all").persist(StorageLevel.MEMORY_AND_DISK)
    val numDaysOfData = new DateRange("yyyyMMdd",30)
    val scoreBaseDF = DataReader.read(new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvLongVideoScoreByDay))

    val validScoreDF = scoreBaseDF.as("a").join(validSid.as("b"),expr("a.sid_or_subject_code = b.sid"),"inner").drop(expr("b.sid")).persist(StorageLevel.MEMORY_AND_DISK)
    //vip影片
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(),-1))
    val vipSidDF = BizUtils.getSearchVideoData(ss,date)(true).selectExpr("sid","content_type","video_score")
    val virVipDF = BizUtils.transferToVirtualSid(vipSidDF,"sid")

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      //获取评分数据
      val scoreSids = BizUtils.getScoreList(validScoreDF,contentType,200)
      import ss.implicits._
      /**
        * 相似影片
        */
      val similarSids = ArrayUtils.takeThenRandom(scoreSids,30)
      val similarDF = similarSids.map(e => ("detail-" + contentType + "_default", e, 1.0))
        .toList.toDF("sid", "item", "similarity")
      /**
        * 热门推荐
        */
      val hotSids = ArrayUtils.takeThenRandom(scoreSids.diff(similarSids),30)
      val hotDF = hotSids.map(e => ("detail-" + contentType + "_default", e, 1.0))
        .toList.toDF("sid", "item", "detailHot")
      /**
        * vip推荐
        */
      val vipSids = virVipDF.where(s"content_type ='$contentType'").
        orderBy(expr("video_score").desc).limit(200).select(expr("sid")).map(r => r.getString(0)).collect()

      val vipDF = ArrayUtils.takeThenRandom(vipSids.diff(hotSids).diff(similarSids),40).map(e => ("detail-" + contentType + "_default", e, 1.0))
        .toList.toDF("sid", "item", "detailVip")
      /**
        * 主题推荐
        */
      val clusterDF = DataReader.read(new HdfsPath("/ai/data/medusa/videoFeatures/" + contentType + "Clusters/Latest"))
      val videoCluster = clusterDF.rdd.map(r => (r.getInt(0), r.getSeq[String](1)))
        .flatMap(e => e._2.map(x => (x, e._1)))
      val validVideos = BizUtils.getValidLongVideoSid()
        .rdd.map(r => (r.getString(0), 1))
      val validVideoCluster = videoCluster.join(validVideos)
        .map(e => (e._2._1, e._1))
        .groupByKey()
        .map(e => (e._1, e._2.toArray))
        .persist(StorageLevel.MEMORY_AND_DISK)

      val clusterIndexes = validVideoCluster.map(e => e._1).collect()
      val cluster2videoMap = validVideoCluster.collectAsMap()

      var themeArray = Array.empty[String]
      ArrayUtils.randomTake(clusterIndexes, 40).foreach(index => {
        themeArray = themeArray ++: ArrayUtils.randomTake(cluster2videoMap.getOrElse(index, Array.empty[String]), 1)
      })
      val themeDefaultDF = themeArray.toList.map(e => ("detail-" + contentType + "_default", e)).toDF("sid", "item")

      val subjectDefaultDF = List.empty[String].map(e => ("detail-" + contentType + "_default", e)).toDF("sid", "id")
      val detailDefaultPackedDF = BizUtils.detailPageUnionPack(vipDF,"hotRanking",hotDF,"hotRanking",similarDF, "hotRanking",
        themeDefaultDF, "randomCluster", subjectDefaultDF, "withoutDefault")

      /**
        * 写入redis
        */
      val dataWriter = new DataWriter2Kafka
      val topic = ConfigUtil.get("default.redis.topic")
      val host = ConfigUtil.get("default.redis.host")
      val port = ConfigUtil.getInt("default.redis.port")
      val dbIndex = ConfigUtil.getInt("default.redis.dbindex")
      val ttl = ConfigUtil.getInt("default.redis.ttl")

      val detailFormatType = FormatTypeEnum.KV
      val detailRedisPath = RedisPath(topic, host, port, dbIndex, ttl, detailFormatType)

      dataWriter.write(detailDefaultPackedDF, detailRedisPath)

    })

    validScoreDF.unpersist()
    validSid.unpersist()

  }

}
