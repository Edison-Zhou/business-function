package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import org.apache.spark.sql.functions._
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.storage.StorageLevel

/**
  * 在dataframe中可视化P、R和Fscore等指标
  *
  * @author Edison_Zhou
  * @since 2019/08/20
  */
object OfflineIndicator extends BaseClass {
  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    //1.获取用户近半个月内观看的影信息
    val startDate = DateUtils.farthestDayWithDelimiter(15)
    val userWatchedWithinHalfMonth = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))
      .filter(s"latest_optime >= '$startDate'").selectExpr("userid as uid", "sid_or_subject_code as sid")
        .groupBy("uid").agg(collect_set("sid")).toDF("uid", "userWatched")
    BizUtils.getDataFrameInfo(userWatchedWithinHalfMonth, "userWatchedWithinHalfMonth")

    //2.获取半个月前的召回结果
    val recallRes = DataReader.read(BizUtils.getHdfsPathForRead("recall/halfMonthAgoMovie"))
      .groupBy("uid").agg(collect_set("sid")).toDF("uid", "recall")
    BizUtils.getDataFrameInfo(recallRes, "recallRes")

    //3.获取附带算法源的召回结果
    val recallWithSource = DataReader.read(BizUtils.getHdfsPathForRead("recall/halfMonthSourceTrack"))
    BizUtils.getDataFrameInfo(recallWithSource, "recallWithSource")

    //有效数据内联，获得同时有推荐结果和观影行为的记录
    val effectiveData = recallRes.as("a").repartition(1000).join(userWatchedWithinHalfMonth
      .as("b"), "uid").selectExpr("uid", "a.recall as recall", "b.userWatched as userWatched")
    BizUtils.getDataFrameInfo(effectiveData, "effectiveData")

    //生成带有评估指标的dataframe
    val resWithIndicator = effectiveData.map(r => {
      val uid = r.getLong(0)
      val recallVideo = r.getSeq[String](1).toSet
      val watchedVideo = r.getSeq[String](2).toSet
      val FNSet = watchedVideo.diff(recallVideo)
      val TPSet = recallVideo.intersect(watchedVideo)
      val precisionRate = TPSet.size.toDouble/recallVideo.size
      val recallRate = TPSet.size.toDouble/watchedVideo.size
      val FScore = if (precisionRate == 0 && recallRate== 0) 0 else 2*precisionRate*recallRate/(precisionRate+recallRate)
      (uid, recallVideo, watchedVideo, TPSet, TPSet.size, FNSet, precisionRate, recallRate, FScore)
    }).toDF("uid", "recall", "userWatched","recallSuccessfully", "recalledNum", "recallOmitted", "P", "R", "FScore")
        .persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(resWithIndicator, "resWithIndicator")

    val recallOmitted = resWithIndicator.select("recallOmitted").flatMap(r => r.getSeq[String](0)).map(e => (e,1))
      .rdd.reduceByKey((x, y)=> x+y).sortBy(r => r._2, false).toDF("sid", "countOfMissing")
    BizUtils.getDataFrameInfo(recallOmitted, "recallOmitted")

    val userRecalledNum = resWithIndicator.select("uid", "recalledNum").filter("recalledNum > 0")
    BizUtils.getDataFrameInfo(userRecalledNum, "userRecalledNum")

    val successfullyRecalled = resWithIndicator.filter("recalledNum > 0").select("uid", "recallSuccessfully")
        .flatMap(r => r.getSeq[String](1).map(e => (r.getLong(0), e))).toDF("uid", "sid")
    BizUtils.getDataFrameInfo(successfullyRecalled, "successfullyRecalled")

    val recalledSourceTrack = recallWithSource.repartition(1000).join(successfullyRecalled, Seq("uid", "sid"))
    BizUtils.getDataFrameInfo(recalledSourceTrack, "recalledSourceTrack")

    val recalledBySource = recalledSourceTrack.groupBy("uid", "source").agg(count("sid"))
      .map(r => (r.getLong(0) ,r.getString(1), r.getLong(2).toDouble)).toDF("uid", "source", "num")
    BizUtils.getDataFrameInfo(recalledBySource, "recalledBySource")

    val recallSourceRatio = recalledBySource.as("a").join(userRecalledNum.as("b"), "uid")
        .selectExpr("uid", "a.source as source", "a.num as sourceNum", "b.recalledNum as recalledNum").map(r => {
        val uid = r.getLong(0)
        val source = r.getString(1)
        val sourceNum = r.getDouble(2)
        val recalledNum = r.getInt(3)
        val sourceRatio = sourceNum/recalledNum
      (uid, source, sourceNum, recalledNum, sourceRatio)
      }).toDF("uid", "source", "sourceNum", "recalledNum", "sourceRatio")
    BizUtils.getDataFrameInfo(recallSourceRatio, "recallSourceRatio")
    BizUtils.outputWrite(recallSourceRatio, "recallSourceRatio")

    BizUtils.outputWrite(resWithIndicator, "offlineIndicator")

  }
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
