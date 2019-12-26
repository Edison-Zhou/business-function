package cn.featureEngineering.featureExtraction

import cn.featureEngineering.dataUtils.ETLSparse
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cheng_huan on 2019/6/25.
  */
@deprecated
object ReductionFeature extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val score = 0.5
    val keyWord = "ConcatNormalizedSqrt" + score
    val newSize = 300

    val featureImportance1 = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/GBRT/featureImportance/ConcatNormalizedSqrt0.5_0.5/Latest"))
    val featureImportance2 = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/GBRT/featureImportance/ConcatNormalizedSqrt0.5_0.6/Latest"))
    val featureImportance3 = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/GBRT/featureImportance/ConcatNormalizedSqrt0.5_0.7/Latest"))
    val featureImportance4 = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/GBRT/featureImportance/ConcatNormalizedSqrt0.5_0.8/Latest"))

    val featureImportance = ETLSparse.featureImportanceUnion(
      ETLSparse.featureImportanceUnion(featureImportance1, featureImportance2),
      ETLSparse.featureImportanceUnion(featureImportance3, featureImportance4))
      .select("value").map(r => r.getDouble(0)).collect().toVector

    val nonZeroDim = featureImportance.filter(e => e > 0).size
    println(s"nonZeroDim = $nonZeroDim")

    val trainFeatures = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/cluster/trainConcatNormalizedSqrt0.5Feature/Latest"))
    val trainReducedFeatures = ETLSparse.featureReduction(trainFeatures, featureImportance, newSize)
    BizUtils.outputWrite(trainReducedFeatures, "featureEngineering/cluster/train" + keyWord +"_" + newSize + "Feature")

    val testFeatures = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/cluster/testConcatNormalizedSqrt0.5Feature/Latest"))
    val testReducedFeatures = ETLSparse.featureReduction(testFeatures, featureImportance, newSize)
    BizUtils.outputWrite(testReducedFeatures, "featureEngineering/cluster/test" + keyWord +"_" + newSize + "Feature")
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
}
