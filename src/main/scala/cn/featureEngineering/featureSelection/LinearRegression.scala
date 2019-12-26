package cn.featureEngineering.featureSelection

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.BaseParams
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.HdfsUtil
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/10/18.
  */
object LinearRegression extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    TransformUDF.registerUDFSS

    val trunNum = 50000
    val keyWord = "Trunc" + trunNum + "TagSourceNot3"
    /**
      * 1、数据读取
      */
    val trainFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/train" + "ConcatNormalizedSqrt0.5" + "Feature/Latest"
    val testFeaturePath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/test" + "ConcatNormalizedSqrt0.5" + "Feature/Latest"

    val trainData = DataReader.read(new HdfsPath(trainFeaturePath))
      //.sample(0.5)
      .map(r => (r.getAs[Double]("label"), r.getAs[SparseVector]("features")))
      .toDF("label", "features").persist(StorageLevel.MEMORY_ONLY)

    val testData = DataReader.read(new HdfsPath(testFeaturePath))
      //.sample(0.5)
      .map(r => (r.getAs[Double]("label"), r.getAs[SparseVector]("features")))
      .toDF("label", "features").persist(StorageLevel.MEMORY_ONLY)

    BizUtils.getDataFrameInfo(trainData, "trainData")
    //BizUtils.getDataFrameInfo(testData, "testData")

    val splits = trainData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1).cache()

    /**
      * 2、模型训练
      */
    println("=" * 50)
    val LR = new LinearRegression()
    val param = BaseParams.getParams(args)
    val reg = param.supParam1
    val enp = param.supParam2
    val lrModel = LR.setElasticNetParam(enp)
      .setRegParam(reg)
      .setMaxIter(100)
      .fit(training)

    /**
      * 3、模型评估
      */
    val notZeroFeatureLength = lrModel.coefficients.toSparse.values.length
    println("notZeroCoefficients.length")
    println(notZeroFeatureLength)

    println("lamda = " + reg)
    println("alpha = " + enp)
    println("training error")
    println("RMSE = " + lrModel.summary.rootMeanSquaredError)
    println("MAE = " + lrModel.summary.meanAbsoluteError)

    val validationSummary = lrModel.evaluate(validation)

    println("validation error")
    println(s"RMSE = ${validationSummary.rootMeanSquaredError}")
    println(s"MAE = ${validationSummary.meanAbsoluteError}")

    val testSummary = lrModel.evaluate(testData)

    println("test error")
    println(s"RMSE = ${testSummary.rootMeanSquaredError}")
    println(s"MAE = ${testSummary.meanAbsoluteError}")

    val modelPath = "/ai/tmp/model/pre/medusa/LRModel3072"
    HdfsUtil.deleteHDFSFileOrPath(modelPath)
    lrModel.save(modelPath)

    /**
      * 4、特征选择
      */
    /*val featureIndices = trainData.flatMap(r => {
    val sparseFeature = r.getAs[SparseVector]("features")

      sparseFeature.indices
    }).toDF("featureIndex").distinct()

    val coefficients = lrModel.coefficients.toArray.toSeq.zipWithIndex.toDF("coefficient", "index")
    val featuresIndexDF = featureIndices.as("a").join(coefficients.as("b"), expr("featureIndex = index"))
      .filter("coefficient > 0").select("featureIndex", "coefficient")

    val nonZeroFeaturesIndexDF = featuresIndexDF.filter(expr("abs(coefficient) > 0"))
    val zeroFeaturesIndexDF = featuresIndexDF.filter(expr("abs(coefficient) < 0.000001"))

    BizUtils.outputWrite(nonZeroFeaturesIndexDF, "featureEngineering/nonZeroFeaturesIndex/bySelectedFeaturesSparse")
    BizUtils.outputWrite(zeroFeaturesIndexDF, "featureEngineering/zeroFeaturesIndex/bySelectedFeaturesSparse")*/



    /*val nonZeroFeaturesIndexDF = DataReader
      .read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/nonZeroFeaturesIndex/bySelectedFeaturesSparse/Latest"))

    val notZeroFeatureLength = nonZeroFeaturesIndexDF.count()

    val data = DataReader.read(new HdfsPath(featurePath))
    val featuresDF = data.flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val uid = r.getAs[String]("uid")
      val sid = r.getAs[String]("sid")
      val label = r.getAs[Double]("label")

      sparseFeature.indices.map(e => (uid, sid, label, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("uid", "sid", "label", "fieldIndex", "featureRating")

    val featureSize = data.first.getAs[SparseVector]("features").size

    val selectedFeatures = featuresDF.as("a")
      .join(nonZeroFeaturesIndexDF.as("b"), expr("a.fieldIndex = b.featureIndex"))
      .drop(expr("b.featureIndex"))
      .distinct()
      .groupBy("uid", "sid", "label").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[Double]("label"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF("uid", "sid", "label", "features")

    BizUtils.outputWrite(selectedFeatures, "featureEngineering/features/selectedFeaturesSparse" + notZeroFeatureLength)*/

    ss.stop()
  }
}
