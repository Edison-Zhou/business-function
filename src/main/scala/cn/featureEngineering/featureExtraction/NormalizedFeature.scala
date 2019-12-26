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
object NormalizedFeature extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val trunNum = 50000
    val keyWord = "Trunc" + trunNum

    val projectionFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/projectionFeatures"+ keyWord +"SparseD10/Latest"))
    val projectionFeatures4TestDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/projectionFeatures"+ keyWord +"SparseTestD10_2/Latest"))

    val normalizedFeaturesDF = ETLSparse.normalization(projectionFeaturesDF)
    val normalizedFeatures4TestDF = ETLSparse.normalization(projectionFeatures4TestDF)

    BizUtils.outputWrite(normalizedFeaturesDF, "featureEngineering/features/normalizedFeatures"+ keyWord +"SparseD10")
    BizUtils.outputWrite(normalizedFeatures4TestDF, "featureEngineering/features/normalizedFeatures"+ keyWord +"SparseTestD10_2")
  }

}
