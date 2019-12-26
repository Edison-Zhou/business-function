package cn.featureEngineering.featureExtraction

import cn.featureEngineering.dataUtils
import cn.featureEngineering.dataUtils.ETLSparse
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
object MetadataFeature extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    Array(0.5).foreach(scoreThreshold => {
      val videoFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/metadata/videoFeature/Latest"
      val userFeaturePath = "featureEngineering/metadata/userFeature" + scoreThreshold

      val score = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))
        .selectExpr("userid as uid", "sid_or_subject_code as sid", "latest_optime as optime", "score")
        .as("a").join(BizUtils.readActiveUser().as("b"), expr("a.uid = b.user"), "inner")
        .withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("score").desc)))
        .filter(s"rank <= 2000 and score > $scoreThreshold")
        .selectExpr("a.uid as uid", "a.sid as sid", "a.optime as optime", "a.score as score")

      val validMovie = DataReader.read(BizUtils.getMysqlPath("movie_valid_sid"))

      val videoFeature = DataReader.read(new HdfsPath("/data_warehouse/dw_normalized/videoTagFeatureTFIDF/current"))
        .toDF("sid", "features").as("a").join(validMovie.as("b"), expr("a.sid = b.sid"), "inner")
        .selectExpr("a.sid as sid", "features")

      videoFeature.show(200)

      val userFeatureReIndex = ETLSparse.union2userFeaturesWithDecay(videoFeature, score, 0.99, "avg")

      val userFeatureReIndexIDFScaling = ETLSparse.featureScalingByIDF(userFeatureReIndex, 0.15)
      val userFeatureReIndexIDFScalingNormalize = ETLSparse.featureNormalizationByQuantile(userFeatureReIndexIDFScaling, 0, 1)

      BizUtils.outputWrite(userFeatureReIndexIDFScaling, "featureEngineering/tag/userFeatureReIndexIDFScaling" + scoreThreshold)
      BizUtils.outputWrite(userFeatureReIndexIDFScalingNormalize, "featureEngineering/tag/userFeatureReIndexIDFScalingNormalize" + scoreThreshold)
    })
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
