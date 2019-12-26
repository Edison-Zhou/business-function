package cn.featureEngineering.featureExtraction

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.featureEngineering.dataUtils
import cn.featureEngineering.dataUtils.{ETLCluster, ETLSparse}
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/4/24.
  */
object ClusterFeature extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    Array(0.5).foreach(scoreThreshold => {
      val score = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))
        .selectExpr("userid as uid", "sid_or_subject_code as sid", "latest_optime as optime", "score")
        .as("a").join(BizUtils.readActiveUser().as("b"), expr("a.uid = b.user"), "inner")
        .withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("score").desc)))
        .filter(s"rank <= 2000 and score > $scoreThreshold")
        .selectExpr("a.uid as uid", "a.sid as sid", "a.optime as optime", "a.score as score")

      val videoFeature = dataUtils.Read.getVideoWord2vecFeature("movie").toDF("sid", "feature")
      val videoCluster = dataUtils.Read.getVideoCluster("movie").toDF("index", "sidArray")

      val videoClusterProperty = ETLCluster.videoClusterProperty(videoCluster, videoFeature)
      BizUtils.outputWrite(videoClusterProperty, "featureEngineering/cluster/videoClusterProperty")

      val videoClusterBetweenSimilarity = ETLCluster.videoClusterBetweenSimilarity(videoCluster, videoFeature, 0.3)
      BizUtils.outputWrite(videoClusterBetweenSimilarity, "featureEngineering/cluster/videoClusterBetweenSimilarity")

      val videoClusterFeature = ETLCluster.videoFeature2(videoClusterProperty, videoClusterBetweenSimilarity, 1024)
      val videoClusterFeatureNormalize = ETLSparse.featureNormalizationByQuantile(videoClusterFeature, 0.001, 0.999)
      BizUtils.outputWrite(videoClusterFeatureNormalize, "featureEngineering/cluster/videoFeature")

      val userClusterFeature = ETLCluster.userFeature2(score, videoClusterProperty, videoClusterBetweenSimilarity, 0.998, 1024)

      val normalizedUserFeature = ETLSparse.featureNormalizationByQuantile(userClusterFeature, 0.001, 0.999)

      BizUtils.outputWrite(normalizedUserFeature, "featureEngineering/cluster/userNormalizedFeature" + scoreThreshold)
    })

  }
}
