package cn.featureEngineering.featureExtraction

import cn.featureEngineering.dataUtils
import cn.featureEngineering.dataUtils.{ETLSparse}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/8/14.
  */
object TagFeature extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val trainSetPath = "/ai/tmp/output/test/medusa/featureEngineering/trainSet_0.5_500_5_50_1/Latest"
    val videoFeaturePath = "/data_warehouse/dw_normalized/videoTagFeatureTFIDF/current"
    val tagReIndexPath = "featureEngineering/tag/tagReIndex"
    val videoFeatureReIndexPath = "featureEngineering/tag/videoFeatureReIndex"
    val videoFeatureReIndexNormalizePath = "featureEngineering/tag/videoFeatureReIndexNormalize"
    val userFeatureReIndexPath = "featureEngineering/tag/userFeatureReIndex"
    val userFeatureReIndexIDFScalingPath = "featureEngineering/tag/userFeatureReIndexIDFScaling"
    val userFeatureReIndexIDFScalingNormalizePath = "featureEngineering/tag/userFeatureReIndexIDFScalingNormalize"

    val score = DataReader.read(new HdfsPath(trainSetPath))
    val validMovie = DataReader.read(BizUtils.getMysqlPath("movie_valid_sid"))
    val videoFeature = DataReader.read(new HdfsPath(videoFeaturePath))
      .toDF("sid", "features").as("a").join(validMovie.as("b"), expr("a.sid = b.sid"), "inner")
      .selectExpr("a.sid as sid", "features")

    BizUtils.getDataFrameInfo(videoFeature, "videoFeature")

    val tagIndex = dataUtils.Read.getTagIndex
    val tagReIndex = ETLSparse.tagReIndex(tagIndex)

    BizUtils.getDataFrameInfo(tagReIndex, "tagReIndex")

    val tagReIndexMap = tagReIndex.select("tag_id", "index")
      .map(r => (r.getAs[Int]("tag_id"), r.getAs[Int]("index"))).collect().toMap
    val videoFeatureReIndex = ETLSparse.videoTagFeatureReIndex(videoFeature, tagReIndexMap)
    val videoFeatureReIndexNormalized = ETLSparse.featureNormalizationByQuantile(videoFeatureReIndex, 0, 1)

    BizUtils.getDataFrameInfo(videoFeatureReIndex, "videoFeatureReIndex")
    BizUtils.getDataFrameInfo(videoFeatureReIndexNormalized, "videoFeatureReIndexNormalize")

    val userFeatureReIndex = ETLSparse.union2userFeaturesWithDecay(videoFeatureReIndexNormalized, score, 0.999, "avg")
    BizUtils.getDataFrameInfo(userFeatureReIndex, "userFeatureReIndex")
    val userFeatureReIndexIDFScaling = ETLSparse.featureScalingByIDF(userFeatureReIndex, 0.00001)
    BizUtils.getDataFrameInfo(userFeatureReIndexIDFScaling, "userFeatureReIndexIDFScaling")
    val userFeatureReIndexIDFScalingNormalize = ETLSparse.featureNormalizationByQuantile(userFeatureReIndexIDFScaling, 0, 1)

    BizUtils.getDataFrameInfo(userFeatureReIndexIDFScalingNormalize, "userFeatureReIndexIDFScalingNormalize")

    BizUtils.outputWrite(tagReIndex, tagReIndexPath)
    BizUtils.outputWrite(videoFeatureReIndex, videoFeatureReIndexPath)
    BizUtils.outputWrite(videoFeatureReIndexNormalized, videoFeatureReIndexNormalizePath)
    BizUtils.outputWrite(userFeatureReIndex, userFeatureReIndexPath)
    BizUtils.outputWrite(userFeatureReIndexIDFScaling, userFeatureReIndexIDFScalingPath)
    BizUtils.outputWrite(userFeatureReIndexIDFScalingNormalize, userFeatureReIndexIDFScalingNormalizePath)
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
