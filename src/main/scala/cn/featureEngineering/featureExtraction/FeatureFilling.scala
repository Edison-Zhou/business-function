package cn.featureEngineering.featureExtraction

import breeze.optimize.linear.PowerMethod.BDV
import cn.moretv.doraemon.algorithm.als.{AlsAlgorithm, AlsModel}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/2/12.
  */
@deprecated
object FeatureFilling extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import spark.implicits._
    /**
      * 数据准备
      */
    val data = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/userFeatures/Latest"))

    val userFeatures = data.flatMap(r => r.getAs[Seq[Row]]("features").map(e => (r.getAs[String]("uid").toLong, e.getString(0), e.getDouble(1))))
      .toDF("uid", "featureName", "rating")

    /**
      * 用ALS填充稀疏特征
      */
    val ALSAlg = new AlsAlgorithm()
    val dataMap = Map(ALSAlg.INPUT_DATA_KEY -> userFeatures)

    ALSAlg.initInputData(dataMap)
    ALSAlg.run()

    println("模型结果样例打印:")
    ALSAlg.getOutputModel.asInstanceOf[AlsModel].matrixU.printSchema()
    ALSAlg.getOutputModel.asInstanceOf[AlsModel].matrixU.show
    ALSAlg.getOutputModel.asInstanceOf[AlsModel].matrixV.printSchema()
    ALSAlg.getOutputModel.asInstanceOf[AlsModel].matrixV.show

    val matrixU = ALSAlg.modelOutput.matrixU
    val matrixV = ALSAlg.modelOutput.matrixV

    val calculatedFeatures = matrixU.as("U").join(matrixV.as("V"))
        .selectExpr("U.uid as uid", "V.featureName as featureName", "U.features as Ufeatures", "V.features as Vfeatures")
      .map(r => {
        val uid = r.getAs[Long]("uid")
        val sid = r.getAs[String]("featureName")
        val vectorU = Vectors.dense(r.getAs[Seq[Double]]("Ufeatures").toArray).toDense
        val vectorV = Vectors.dense(r.getAs[Seq[Double]]("Vfeatures").toArray).toDense
        val BV1 = new BDV(vectorU.values)
        val BV2 = new BDV(vectorV.values)
        val rating = BV1.dot(BV2)

        (uid, sid, rating)
      }).toDF("uid", "featureName", "rating")

    val filledFeatureDF = userFeatures.as("a").join(calculatedFeatures.as("b"), expr("a.uid = b.uid and a.featureName = b.featureName"), "inner")
      .selectExpr("a.uid as uid", "a.featureName as featureName", "case when a.rating = 0 and b.rating >= 0 then b.rating else a.rating end as featureRating")
      .groupBy("uid", "featureName").agg(sum("featureRating").as("featureRating"))
      .groupBy("uid").agg(collect_list(concat_ws("%", col("featureName"), col("featureRating"))).as("features"))
      .map(r => {
        val sid = r.getAs[Long]("uid").toString
        val features = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0), s.split("%")(1).toDouble)).sortBy(_._1)

        (sid, features)
      })
      .toDF("uid", "features")
    /**
      * 结果输出
      */
    BizUtils.outputWrite(filledFeatureDF, "featureEngineering/features/filledUserFeatures")
  }

}
