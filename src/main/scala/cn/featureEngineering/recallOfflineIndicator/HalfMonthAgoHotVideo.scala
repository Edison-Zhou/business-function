package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.algorithm.hotVideo.{HotVideoAlgorithm, HotVideoModel, HotVideoParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import cn.moretv.doraemon.common.util.DateUtils

/*
* 以算法调用者指定的时间单元 基于每日评分数据统计出影片popularity 获取热门影片DF
* Created by Edison_Zhou on 2019/02/28
* Reused on 2019/08/20
* */
object HalfMonthAgoHotVideo extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      //默认以一周为时间单元，得到影片评分、播放时间的DF
      val startDate:String = DateUtils.farthestDayWithOutDelimiter(22)
      val endDate:String = DateUtils.farthestDayWithOutDelimiter(15)
      val numDaysOfData = new DateRange("yyyyMMdd", startDate, endDate)
      val baseScoreDF = DataReader.read(new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvLongVideoScoreByDay,
        s"select sid_or_subject_code as sid, optime, content_type, score from tmp where content_type='$contentType'"))

      //调用热门影片获取的算法
      val hotVideoAlg: HotVideoAlgorithm = new HotVideoAlgorithm
      val hotVideoParameters: HotVideoParameters = new HotVideoParameters
      val hotVideoMap = Map(hotVideoAlg.INPUT_BASESCORE_KEY -> baseScoreDF)
      hotVideoAlg.initInputData(hotVideoMap)
      hotVideoAlg.run()

      val hotVideoDF = hotVideoAlg.getOutputModel.asInstanceOf[HotVideoModel].hotVideoData

/*      val env = BaseParams.getParams(args).env
      val outputPathBase = env.toLowerCase() match {
        case "pro" => Constants.OUTPUT_PATH_BASE
        case "test" => Constants.OUTPUT_PATH_BASE_DEBUG + "test" + "/"
        case "dev" => Constants.OUTPUT_PATH_BASE_DEBUG + "dev" + "/"
        case "pre" => Constants.OUTPUT_PATH_BASE_DEBUG + "pre" + "/"
        case _ => throw new RuntimeException("不合法的环境类型")
      }

      val path = outputPathBase + productLine.toString.toLowerCase() + "/hotVideo/" + contentType
      HdfsUtil.dataFrameUpdate2HDFS(hotVideoDF, path)
      */
      BizUtils.outputWrite(hotVideoDF, s"halfMonthAgoHotVideo/$contentType")
    })
  }
}

