package cn.moretv.doraemon.biz.portalRecommend

import cn.moretv.doraemon.algorithm.seriesChasing.{SeriesChasingAlgorithm, SeriesChasingModel}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * Created by cheng_huan on 2018/7/10.
  */
object SeriesChasingRecommend extends BaseClass{

  override def execute(args: Array[String]): Unit = {
    try {
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      val sc: SparkContext = ss.sparkContext
      import ss.implicits._
      /**
        * DataFrame[(uid, sid, optime, eposideIndex)]
        *
        */
      //读取HDFS数据
      val numDaysOfData = new DateRange("yyyyMMdd",8)
      val scoreDataPath = new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvSeriesScore)
      val scoreData = DataReader.read(scoreDataPath).rdd
        .map(r => (r.getLong(0), r.getString(1),
          r.getString(2).substring(0, 10), r.getString(3), r.getString(4), r.getString(5).toInt, r.getDouble(6)))
        .toDF("userid", "sid", "optime", "content_type", "episodesid", "episode_index", "score")
      val unionScore = BizUtils.scoreMaxToVirtualSid(scoreData, "sid")
      unionScore.show()

      val sql4videoData = "select parent_sid, episode_index, create_time, content_type from tmp " +
        "where video_type = 2 " +
        "and episode_index is not null " +
        "and status = 1 " +
        "and type = 1 " +
        "and parent_sid is not null " +
        "and create_time is not null"
      val videoDataPath = new HdfsPath(PathConstants.pathOfMoretvProgram, sql4videoData)
      val videoData = DataReader.read(videoDataPath).rdd
        .map(r => (r.getString(0), r.getInt(1), r.getTimestamp(2), r.getString(3)))
        .toDF("parent_sid", "episode_index", "create_time", "content_type")

      unionScore.printSchema()
      unionScore.show(10, false)

      videoData.printSchema()
      videoData.show(10, false)

      val seriesChasingAlg = new SeriesChasingAlgorithm()
      val seriesChasingDataMap = Map(seriesChasingAlg.INPUT_SCOREDATA_KEY -> unionScore, seriesChasingAlg.INPUT_VIDEODATA_KEY -> videoData)
      seriesChasingAlg.initInputData(seriesChasingDataMap)
      seriesChasingAlg.run()
      //输出目录为："/ai/output/medusa/seriesChasing/Latest"
      seriesChasingAlg.getOutputModel.output("seriesChasingRecommend")

    }catch {
      case e:Exception => throw e
    }
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
