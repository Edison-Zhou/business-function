package cn.featureEngineering.featureExtraction

import cn.featureEngineering.dataUtils.ETLSparse
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/11/7.
  */
object ProjectionFeature extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val trunNum = 50000
    val keyWord = "Trunc" + trunNum

    val trainData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/trainSet10/Latest"))
      //.selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
      .selectExpr("cast (uid as string)", "sid", "score")

    val testData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/testSet10_2/Latest"))
      //.selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
      .selectExpr("cast (uid as string)", "sid", "score")

    val videoFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/videoFeatures"+ keyWord +"SparseD10/Latest"))
    val userFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/userFeatures"+ keyWord +"SparseD10/Latest"))

    val projectionFeaturesDF = ETLSparse.featureProjection(userFeaturesDF, videoFeaturesDF, trainData)
    val projectionFeatures4TestDF = ETLSparse.featureProjection(userFeaturesDF, videoFeaturesDF, testData)

    BizUtils.outputWrite(projectionFeaturesDF, "featureEngineering/features/projectionFeatures"+ keyWord +"SparseD10")
    BizUtils.outputWrite(projectionFeatures4TestDF, "featureEngineering/features/projectionFeatures"+ keyWord +"SparseTestD10_2")
  }

}
