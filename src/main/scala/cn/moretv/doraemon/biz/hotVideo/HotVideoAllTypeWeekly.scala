package cn.moretv.doraemon.biz.hotVideo

import cn.moretv.doraemon.algorithm.hotVideo. {HotVideoAlgorithm, HotVideoParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
* 以算法调用者指定的时间单元 基于每日评分数据统计出影片popularity 获取热门影片DF
* Created by Edison_Zhou on 2019/02/28
* */
object HotVideoAllTypeWeekly extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    //默认以一周为时间单元，得到影片评分、播放时间的DF
    // 如想更改时间周期，在下方初始化DateRange时更改numberDays即可
    val numDaysOfData = new DateRange("yyyyMMdd",7)
    val baseScoreDF = DataReader.read(new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvLongVideoScoreByDay,
      "select sid_or_subject_code as sid, optime, content_type, score from tmp"))

    //调用热门影片获取的算法
    val hotVideoAlg: HotVideoAlgorithm = new HotVideoAlgorithm
    val hotVideoParameters: HotVideoParameters = new HotVideoParameters
    val hotVideoMap = Map(hotVideoAlg.INPUT_BASESCORE_KEY -> baseScoreDF)
    hotVideoAlg.initInputData(hotVideoMap)
    hotVideoAlg.run()
    //结果输出到HDFS
    hotVideoAlg.getOutputModel.output("HotVideoAllType")
  }
}