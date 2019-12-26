package cn.featureEngineering.featureExtraction


import cn.featureEngineering.dataUtils.{ETL, ETLSparse, Read}
import cn.moretv.doraemon.biz.BaseClass
import cn.featureEngineering.dataUtils
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.params.BaseParams
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, Path}
import cn.moretv.doraemon.data.writer.{DataWriter, DataWriter2Hdfs}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/11/7.
  */
object VideoFeature extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val trainData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/trainSet10/Latest"))
        //.selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
        .selectExpr("cast (uid as string)", "sid", "score")

    val testData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/testSet10_2/Latest"))
      //.selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
      .selectExpr("cast (uid as string)", "sid", "score")

    /*val fillingData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/fillingData/ALS/Latest"))
      .selectExpr("cast (uid as string)", "sid", "case when score >= 1 then 1.0 else score end as score")
      .filter("score >= 0.8")
      .withColumn("score", col("score") * 0.6)*/

      /**
        * 稀疏向量生成
        */
      /*val videoTags = dataUtils.Read.getVideoTag2("movie").persist(StorageLevel.MEMORY_ONLY)

      val tagReIndexDF = ETLSparse.tagReIndex(videoTags.select("tag_id", "tag_name"))

    BizUtils.outputWrite(tagReIndexDF, "featureEngineering/tagReIndex")

      val videoFeaturesDF = ETLSparse.tagReIndex2VideoFeatures(videoTags.select("tag_id", "sid", "rating"), tagReIndexDF)
        .persist(StorageLevel.MEMORY_ONLY)
      //BizUtils.getDataFrameInfo(videoFeaturesDF, "videoFeaturesDF")
      BizUtils.outputWrite(videoFeaturesDF, "featureEngineering/features/videoFeaturesReIndexSparseAll")

      val userFeaturesDF = ETLSparse.union2userFeatures(videoFeaturesDF, trainData)
        .persist(StorageLevel.MEMORY_ONLY)
      //BizUtils.getDataFrameInfo(userFeaturesDF, "userFeaturesDF")
      BizUtils.outputWrite(userFeaturesDF, "featureEngineering/features/userFeaturesReIndexSparseD10")*/

    /*val videoFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/videoFeaturesReIndexSparseAll/Latest"))

    val userFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/userFeaturesReIndexSparseD10/Latest"))

    val projectionFeaturesDF = ETLSparse.featureProjection(userFeaturesDF, videoFeaturesDF, testData)
    //BizUtils.getDataFrameInfo(projectionFeatures, "projectionFeatures")
    //userFeaturesDF.unpersist()
    //videoFeaturesDF.unpersist()

    //BizUtils.getDataFrameInfo(projectionFeaturesDF, "projectionFeaturesDF")
    BizUtils.outputWrite(projectionFeaturesDF, "featureEngineering/features/projectionFeaturesReIndexSparseTestD10_2")*/

    val projectionFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/projectionFeaturesReIndexSparseTestD10_2/Latest"))

    val normalizedFeaturesDF = ETLSparse.normalization(projectionFeaturesDF)
    //BizUtils.getDataFrameInfo(normalizedFeaturesDF, "normalizedFeaturesDF")
    BizUtils.outputWrite(normalizedFeaturesDF, "featureEngineering/features/normalizedFeaturesReIndexSparseTestD10_2")

    /*val intersectedFeaturesDF = ETLSparse.featureIntersection(normalizedFeaturesDF)
      .persist(StorageLevel.MEMORY_ONLY)
    BizUtils.getDataFrameInfo(intersectedFeaturesDF, "intersectedFeaturesDF")
    BizUtils.outputWrite(intersectedFeaturesDF, "featureEngineering/features/intersectedFeaturesSparse6000")*/

    /**
      * tag_id，tag_name映射
      */
    /*val tagDF = dataUtils.Read.getVideoTag2("movie").select("tag_id", "tag_name").distinct()
    BizUtils.outputWrite(tagDF, "featureEngineering/movieTag")*/

  }

}
