package cn.featureEngineering.datasetGen

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/12/17.
  */
object DatasetGen extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    val scoreThreshold = 0.5
    val maxWatchNum = 500
    val minWatchNum = 5
    val trainNum = 50
    val testNum = 1

    val pathSuffix = "_" + scoreThreshold + "_" + maxWatchNum + "_" + minWatchNum + "_" + trainNum + "_" + testNum
    val trainSetPath = "featureEngineering/trainSet" + pathSuffix
    val testSetPath = "featureEngineering/testSet" + pathSuffix

    val score = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))
      .selectExpr("userid as uid", "sid_or_subject_code as sid", "latest_optime as optime", "score")
      .as("a").join(BizUtils.readActiveUser().as("b"), expr("a.uid = b.user"), "inner")
      .withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("score").desc)))
      .filter(s"score >= $scoreThreshold")
      .selectExpr("a.uid as uid", "a.sid as sid", "a.optime as optime", "a.score as score", "rank")

    val invalidUser = score.filter(s"rank >= $maxWatchNum").select("uid").distinct()

    val filteredScore = score.as("a").join(invalidUser.as("b"), expr("a.uid = b.uid"), "leftOuter")
      .filter(expr("b.uid is null"))
      .selectExpr("a.uid as uid", "a.sid as sid", "a.optime as optime", "a.score as score")
      .rdd.map(r => (r.getLong(0), (r.getString(1), r.getString(2), r.getDouble(3))))
      .map(e => (e._1, (e._2._1, e._2._2, if(e._2._3 <= 2) e._2._3 else 2)))
      .groupByKey().map(e => (e._1, e._2.toArray.sortBy(_._2).takeRight(trainNum + testNum)))
      .filter(e => e._2.size >= minWatchNum)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val trainSetDF = filteredScore.map(e => (e._1, e._2.take(e._2.size - testNum)))
      .flatMap(e => e._2.map(x => (e._1, x._1, x._2, x._3)))
      .toDF("uid", "sid", "optime", "score")

    val testSetDF = filteredScore.map(e => (e._1, e._2.takeRight(testNum)))
      .flatMap(e => e._2.map(x => (e._1, x._1, x._2, x._3)))
      .toDF("uid", "sid", "optime", "score")
      .filter("score <= 1")

    println(s"score = ${score.count()}")
    println(s"invalidUser = ${invalidUser.count()}")
    println(s"filteredScore = ${filteredScore.count()}")
    println(s"trainSet = ${trainSetDF.count()}")
    println(s"testSet = ${testSetDF.count()}")

    BizUtils.outputWrite(trainSetDF, trainSetPath)
    BizUtils.outputWrite(testSetDF, testSetPath)
  }

}
