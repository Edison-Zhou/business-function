package cn.moretv.doraemon.biz.reorder

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.reorder.GBTR.{GBTRSortAlgorithm, GBTRSortModel, GBTRSortParameters}
import cn.moretv.doraemon.reorder.linearRegression.{LinearRegressionSortAlgorithm, LinearRegressionSortModel, LinearRegressionSortParameters}
import org.apache.spark.SparkConf
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/7/4.
  */
object MovieReorder extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import spark.implicits._

    val recallFeature = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/featureEngineering/cluster/recallFeature/Latest"))

    val userAlg = BizUtils.readActiveUser().selectExpr("user as uid").distinct().map(r => {
      val uid = r.getAs[Long]("uid")
      val alg = BizUtils.getAlg(uid)
      (uid, alg)
    }).toDF("uid", "alg")

    val recallFeatureFilter = recallFeature.as("a")
      .join(userAlg.repartition(200).as("b"), expr("a.uid = b.uid"), "inner")
      .selectExpr("a.uid as uid", "b.alg as alg", "a.sid as sid", "a.features as features")
      .filter("alg = 'reorder'")
      .persist()

    //println(s"recallFeatureFilter = ${recallFeatureFilter.count()}")

    val roughSortAlg: LinearRegressionSortAlgorithm = new LinearRegressionSortAlgorithm
    val param = roughSortAlg.getParameters.asInstanceOf[LinearRegressionSortParameters]
    param.recommendSize = 120
    param.featureSize = 3072
    param.modelPath = "/ai/tmp/model/pre/medusa/LRModel3072"

    roughSortAlg.initInputData(Map("data" -> recallFeatureFilter))
    roughSortAlg.run()
    val roughSortResult = roughSortAlg.getOutputModel.asInstanceOf[LinearRegressionSortModel].reorderedDataFrame

    /*recallFeatureFilter.unpersist()
    println(s"roughSortResult = ${roughSortResult.count()}")*/

    val roughSortFeature = recallFeatureFilter.as("a").join(roughSortResult.as("b"), expr("a.uid = b.uid and a.sid = b.sid"), "inner")
      .selectExpr("a.uid as uid", "a.sid as sid", "a.features as features")

    /*roughSortResult.unpersist()
    println(s"roughSortFeature = ${roughSortFeature.count()}")*/

    val preciseSortAlg: GBTRSortAlgorithm = new GBTRSortAlgorithm
    val preciseParam = preciseSortAlg.getParameters.asInstanceOf[GBTRSortParameters]
    preciseParam.recommendSize = 72
    preciseParam.featureSize = 3072
    preciseParam.modelPath = "/ai/tmp/model/pre/medusa/GBTRModel3072"

    preciseSortAlg.initInputData(Map("data" -> roughSortFeature))
    preciseSortAlg.run()
    val preciseSortResult = preciseSortAlg.getOutputModel.asInstanceOf[GBTRSortModel].reorderedDataFrame

    recallFeatureFilter.unpersist()
    BizUtils.outputWrite(preciseSortResult, "reorder/movie")

  }
}
