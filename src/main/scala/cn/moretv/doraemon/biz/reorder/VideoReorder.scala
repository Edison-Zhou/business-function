package cn.moretv.doraemon.biz.reorder

import cn.featureEngineering.dataUtils.CalUtils
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.regression.{GBTRegressionModel, LinearRegressionModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/8/2.
  */
object VideoReorder extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._
    val roughSortNum = 120
    val preciseSortNum = 72

    val recallDF = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/recall/movie/Latest"))
      .toDF("uid", "sid").groupBy("uid").agg(collect_list("sid").as("recall"))
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val alg = BizUtils.getAlg(uid)
        val recall = r.getAs[Seq[String]]("recall")
        (uid, alg, recall)
      }).toDF("uid", "alg", "recall").filter("alg = 'reorder'")
    val userFeatureDF = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/featureEngineering/cluster/userNormalizedFeature0.5/Latest"))

    val videoFeatureDF = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/featureEngineering/cluster/videoFeature/Latest")).map(r => {
      val sid = r.getAs[String]("sid")
      val videoFeatures = r.getAs[SparseVector]("features")
      (sid, videoFeatures)
    })

    val bcVideoFeature = ss.sparkContext.broadcast(videoFeatureDF.collect.toMap)
    val lrModel = LinearRegressionModel.load("/ai/tmp/model/pre/medusa/LRModel3072")
    val GBTRModel = GBTRegressionModel.load("/ai/tmp/model/pre/medusa/GBTRModel3072")

    val videoSort = recallDF.repartition(1000).as("a").join(userFeatureDF.repartition(5000).as("b"), expr("a.uid = b.uid"), "inner")
      .selectExpr("a.uid as uid", "b.features as features", "a.recall as recall").toDF("uid", "features", "recall")
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val userFeatures = r.getAs[SparseVector]("features")
        val recall = r.getAs[Seq[String]]("recall")
        (uid, userFeatures, recall)
      }).mapPartitions(partition => {
      val result = partition.flatMap(e => {
        e._3.map(sid => {
          val uid = e._1
          val userFeatures = e._2
          val sidFeatures = bcVideoFeature.value.get(sid)
          val prediction = sidFeatures match {
            case Some(videoFeatures) =>
              val clusterFeatures = CalUtils.vectorPointWiseSqrtProduct(userFeatures, videoFeatures)

              val concatFeatures = CalUtils.vectorConcat(CalUtils.vectorConcat(userFeatures, videoFeatures), clusterFeatures)
              (lrModel.predict(concatFeatures), GBTRModel.predict(concatFeatures))
            case _ => (0.0, 0.0)
          }
          (uid, sid, prediction._1, prediction._2)
        })
      })
      result
    }).toDF("uid", "sid", "LrPrediction", "GBTRPrediction")

    val videoReorder = videoSort.withColumn("tempRank", row_number().over(Window.partitionBy("uid").orderBy(col("LrPrediction").desc)))
      .filter(s"tempRank <= $roughSortNum")
      .withColumn("tempRank", row_number().over(Window.partitionBy("uid").orderBy(col("GBTRPrediction").desc)))
      .filter(s"tempRank <= $preciseSortNum")
      .drop("tempRank").drop("LrPrediction").withColumnRenamed("GBTRPrediction", "score")

    BizUtils.outputWrite(videoReorder, "reorder/movie")
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
