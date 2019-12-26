package cn.featureEngineering.featureSelection

import cn.featureEngineering.dataUtils.{CalUtils, ETLSparse}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.algorithm.similar.SparseVectorComputation
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/5/6.
  */
object FeatureCombination extends BaseClass{

  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val userFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/tag/userFeatureReIndexIDFScalingNormalize/Latest"
    val videoFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/tag/videoFeatureReIndexNormalize/Latest"
    val trainDataPath = "/ai/tmp/output/test/medusa/featureEngineering/trainSet_0.5_500_5_50_1/Latest"
    val testDataPath = "/ai/tmp/output/test/medusa/featureEngineering/testSet_0.5_500_5_50_1/Latest"
    val trainFeaturePath = "featureEngineering/tag/trainFeaturesReIndexNormalize2"
    val testFeaturePath = "featureEngineering/tag/testFeaturesReIndexNormalize2"

    val userFeatureDF = DataReader.read(new HdfsPath(userFeaturePath))
      .toDF("uid", "userFeatures").repartition(1000)
    val videoFeatureDF = DataReader.read(new HdfsPath(videoFeaturePath))
      .toDF("sid", "videoFeatures").repartition(200)

    val trainData = DataReader.read(new HdfsPath(trainDataPath))
    val testData = DataReader.read(new HdfsPath(testDataPath))

    val trainFeatures = trainData.as("a").join(userFeatureDF.as("b"), expr("a.uid = b.uid"), "inner")
      .join(videoFeatureDF.as("c"), expr("a.sid = c.sid"), "inner").repartition(5000)
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val sid = r.getAs[String]("sid")
        val label = r.getAs[Double]("score")

        val userFeatures = r.getAs[SparseVector]("userFeatures")
        val videoFeatures = r.getAs[SparseVector]("videoFeatures")
        val projectionFeatures = CalUtils.vectorPointWiseSqrtProduct(userFeatures, videoFeatures)

        val concatFeatures = CalUtils.vectorConcat(CalUtils.vectorConcat(userFeatures, videoFeatures), projectionFeatures)

        (uid, sid, concatFeatures, label)
      }).toDF("uid", "sid", "features", "label")

    val testFeatures = testData.as("a").join(userFeatureDF.as("b"), expr("a.uid = b.uid"), "inner")
      .join(videoFeatureDF.as("c"), expr("a.sid = c.sid"), "inner").repartition(5000)
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val sid = r.getAs[String]("sid")
        val label = r.getAs[Double]("score")

        val userFeatures = r.getAs[SparseVector]("userFeatures")
        val videoFeatures = r.getAs[SparseVector]("videoFeatures")
        val projectionFeatures = CalUtils.vectorPointWiseSqrtProduct(userFeatures, videoFeatures)

        val concatFeatures = CalUtils.vectorConcat(CalUtils.vectorConcat(userFeatures, videoFeatures), projectionFeatures)

        (uid, sid, concatFeatures, label)
      }).toDF("uid", "sid", "features", "label")

    BizUtils.outputWrite(trainFeatures, trainFeaturePath)
    BizUtils.outputWrite(testFeatures, testFeaturePath)
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
