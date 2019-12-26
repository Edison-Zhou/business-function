package cn.moretv.doraemon.biz.recall

import cn.moretv.doraemon.biz.util.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.DateUtils
import org.apache.spark.sql.functions.{collect_set, count}
import org.apache.spark.storage.StorageLevel

object countTest extends BaseClass {

  implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

   override def execute(): Unit = {
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

    val recallOmitted = resWithIndicator.select("recallOmitted").flatMap(r => r.getSeq[String](1)).map(e => (e,1))
      .rdd.reduceByKey((x, y)=> x+y).toDF("sid", "countOfMissing")
    BizUtils.getDataFrameInfo(recallOmitted, "recallOmitted")

  }

}
