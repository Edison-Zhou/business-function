package cn.featureEngineering.featureSelection

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.BaseParams
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.ml.linalg.{SparseVector}
import org.apache.spark.ml.regression.{GBTRegressor}
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2019/1/23.
  */
object GBTR extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS

    /**
      * 1、数据读取
      */
    val param = BaseParams.getParams(args)
    val ratio = param.supParam1

    val keyWord = "ConcatNormalizedSqrt0.5_300_" + "score0.0_" + s"ratio$ratio"
    val trainFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/train" + "ConcatNormalizedSqrt0.5" + "Feature/Latest"
    val testFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/test" + "ConcatNormalizedSqrt0.5" + "Feature/Latest"

    val trainData = DataReader.read(new HdfsPath(trainFeaturePath))
     // .persist(StorageLevel.MEMORY_ONLY)
    /*val count = trainData.count()
    val ratio = math.min(106000000.0 / count, 1.0)*/
    val train = trainData
      .sample(0.15)
      .map(r => {
        val uid = r.getAs[String]("uid")
        val sid = r.getAs[String]("sid")
        val label = r.getAs[Double]("label")
        (uid, sid, if(label > 1) 1 else label, r.getAs[SparseVector]("features"))
      })
      .toDF("uid", "sid", "label", "features")

    val splits = train.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1).cache()

    val testData = DataReader.read(new HdfsPath(testFeaturePath))
      .persist(StorageLevel.MEMORY_ONLY)

    println(s"training: ${training.count}")
    println(s"validation: ${validation.count}")
    println(s"test: ${testData.count}")

    val GBTR = new GBTRegressor()
    val GBTRModel = GBTR.fit(training.select("label", "features").repartition(1000))

    GBTRModel.save("/ai/tmp/model/pre/medusa/GBTRModel3072")

    println(s"featureImportances: ${GBTRModel.featureImportances.toArray.filter(_ > 0).length}")
    /*val featureImportance = GBTRModel.featureImportances.toArray.zipWithIndex.map(e => (e._2, e._1))
      .toSeq.toDF("index", "value")
    BizUtils.outputWrite(featureImportance, "featureEngineering/GBRT/featureImportance/"+ keyWord)

    val validationResult = validation.map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"),
      GBTRModel.predict(r.getAs("features")), r.getAs[Double]("label")))
      .toDF("uid", "sid", "predict", "label")
    BizUtils.outputWrite(validationResult, "featureEngineering/GBRT/validation"+ keyWord +"Result")

    val testResult = testData.map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"),
      GBTRModel.predict(r.getAs("features")), r.getAs[Double]("label")))
      .toDF("uid", "sid", "predict", "label")
    BizUtils.outputWrite(testResult, "featureEngineering/GBRT/test"+ keyWord +"Result")*/
  }

}
