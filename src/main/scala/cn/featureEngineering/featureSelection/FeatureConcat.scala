package cn.featureEngineering.featureSelection

import cn.featureEngineering.dataUtils.CalUtils
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/9/18.
  */
object FeatureConcat extends BaseClass{

  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val userFeatureTagPath = "/ai/tmp/output/test/medusa/featureEngineering/tag/userFeatureReIndexIDFScalingNormalize0.5/Latest"
    val videoFeatureTagPath = "/ai/tmp/output/test/medusa/featureEngineering/tag/videoFeatureReIndexNormalize/Latest"
    val userFeatureClusterPath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/userNormalizedFeature0.5/Latest"
    val videoFeatureClusterPath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/videoFeature/Latest"
    val tagFeatureSize = 175687
    val clusterFeatureSize = 1024
    val userConcatFeaturePath = "featureEngineering/concat/userFeature"
    val videoConcatFeaturePath = "featureEngineering/concat/videoFeature"


    val userFeatureTag = DataReader.read(new HdfsPath(userFeatureTagPath))
      .toDF("uid", "userFeatures").repartition(1000)
    val userFeatureCluster = DataReader.read(new HdfsPath(userFeatureClusterPath))
      .toDF("uid", "userFeatures").repartition(1000)

    val videoFeatureTag = DataReader.read(new HdfsPath(videoFeatureTagPath))
      .toDF("sid", "videoFeatures").repartition(200)
    val videoFeatureCluster = DataReader.read(new HdfsPath(videoFeatureClusterPath))
      .toDF("sid", "videoFeatures").repartition(200)

    val userFeatureConcat = userFeatureTag.as("a").join(userFeatureCluster.as("b"), expr("a.uid = b.uid"), "leftOuter")
      .drop(expr("b.uid")).toDF("uid", "features1", "features2")
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val features1 = r.getAs[SparseVector]("features1")
        val features2 = r.getAs[SparseVector]("features2")
        val concatFeatures = CalUtils.vectorConcat(features1, tagFeatureSize, features2, clusterFeatureSize)
        (uid, concatFeatures)
      }).toDF("uid", "userFeatures")

    val videoFeatureConcat = videoFeatureTag.as("a").join(videoFeatureCluster.as("b"), expr("a.sid = b.sid"), "leftOuter")
      .drop(expr("b.sid")).toDF("sid", "features1", "features2")
      .map(r => {
        val sid = r.getAs[String]("sid")
        val features1 = r.getAs[SparseVector]("features1")
        val features2 = r.getAs[SparseVector]("features2")
        val concatFeatures = CalUtils.vectorConcat(features1, tagFeatureSize, features2, clusterFeatureSize)
        (sid, concatFeatures)
      }).toDF("sid", "videoFeatures")

    BizUtils.outputWrite(userFeatureConcat, userConcatFeaturePath)
    BizUtils.outputWrite(videoFeatureConcat, videoConcatFeaturePath)
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
