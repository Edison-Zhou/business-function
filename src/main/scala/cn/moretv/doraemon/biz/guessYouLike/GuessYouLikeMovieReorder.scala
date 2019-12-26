package cn.moretv.doraemon.biz.guessYouLike

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HdfsPath, Path}
import cn.moretv.doraemon.common.util.DateUtils
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_list
import cn.moretv.doraemon.common.util.ArrayUtils

/**
  * @author Edison_Zhou
  * @since 2019/7/24
  *
  *        电影猜你喜欢业务B算法--Reorder框架结果产出  测试阶段，当前适用于电视猫电影。
  */
object GuessYouLikeMovieReorder extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config(config).enableHiveSupport().getOrCreate()
    import spark.implicits._

    //定义参数值
    val contentType = "movie"
    val alg = "reorder"
    val kafkaTopic = ConfigUtil.get("couchbase.moretv.topic")

   /*
   val recallFeature = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/featureEngineering/cluster/recallFeature/Latest"))

    val userAlg = recallFeature.select("uid").distinct().map(r => {
      val uid = r.getAs[Long]("uid")
      val alg = BizUtils.getAlg(uid)
      (uid, alg)
    }).toDF("uid", "alg")


    val recallFeatureFilter = recallFeature.as("a")
      .join(userAlg.repartition(200).as("b"), expr("a.uid = b.uid"), "inner")
      .selectExpr("a.uid as uid", "b.alg as alg", "a.sid as sid", "a.features as features")
      .filter("alg = 'reorder'")
      .persist()

    val roughSortAlg: LinearRegressionSortAlgorithm = new LinearRegressionSortAlgorithm
    val param = roughSortAlg.getParameters.asInstanceOf[LinearRegressionSortParameters]
    param.recommendSize = 120
    param.featureSize = 3072
    param.modelPath = "/ai/tmp/model/pre/medusa/LRModel3072"

    roughSortAlg.initInputData(Map("data" -> recallFeatureFilter))
    roughSortAlg.run()
    val roughSortResult = roughSortAlg.getOutputModel.asInstanceOf[LinearRegressionSortModel].reorderedDataFrame

    val roughSortFeature = recallFeatureFilter.as("a").join(roughSortResult.as("b"), expr("a.uid = b.uid and a.sid = b.sid"), "inner")
      .selectExpr("a.uid as uid", "a.sid as sid", "a.features as features")

    val preciseSortAlg: GBTRSortAlgorithm = new GBTRSortAlgorithm
    val preciseParam = preciseSortAlg.getParameters.asInstanceOf[GBTRSortParameters]
    preciseParam.recommendSize = 72
    preciseParam.featureSize = 3072
    preciseParam.modelPath = "/ai/tmp/model/pre/medusa/GBTRModel3072"

    preciseSortAlg.initInputData(Map("data" -> roughSortFeature))
    preciseSortAlg.run()
    val preciseSortResult = preciseSortAlg.getOutputModel.asInstanceOf[GBTRSortModel].reorderedDataFrame
    */

    val preciseSortResult = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/reorder/movie/Latest"))

      //----数据写入部分----
    //重排序后写入couchbase

      val businessPrefix = contentType match {
        case "movie" => "c:m:"
        case "tv" => "c:t:"
        case "yueting_movie" => "c:ytm:"
        case "yueting_tv" => "c:ytt:"
      }

    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = businessPrefix
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> alg)
    val path: Path = CouchbasePath(kafkaTopic)
    val dataWriter: DataWriter = new DataWriter2Kafka
    val result= preciseSortResult.groupBy("uid").agg(collect_list("sid"))
      .map(r => (r.getLong(0), ArrayUtils.randomArray(r.getSeq[String](1).toArray)))
      .toDF("uid","id")
    val dataSource = DataPack.pack(result, param)
    dataWriter.write(dataSource, path)

    }
  }


