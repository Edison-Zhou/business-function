package cn.moretv.doraemon.biz.peoplealsolike

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame

/**
  * Created by guohao on 2018/9/30.
  * 长视频退出推荐
  */
object AlsRecommendMovie extends BaseClass{
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
  override def execute(args: Array[String]): Unit = {
    val ss = spark
    //获取长视频movie
    import ss.implicits._
    TransformUDF.registerUDFSS
    val sids = BizUtils.getAvailableVideo("movie").selectExpr("sid")
      .map(f=>f.getAs[String]("sid"))
      .collect()
    //读取ALS训练后的特征向量(v矩阵)
    val alsModel = new AlsModel()
    //全量数据，需要过滤movie
    alsModel.load("default")
    val itemFactorRaw= alsModel.matrixV
    //过滤长视频movie
    val movieItemFactorRaw = itemFactorRaw.filter(row=>{
      val item = row.getAs[String]("item")
      sids.contains(item)
    }).map(row=>{
      val item = row.getAs[String]("item")
      val features = row.getAs[List[Double]]("features").toArray[Double]
      (item, Vectors.dense(features))
    }).toDF

    matrixFactorizationForItem(movieItemFactorRaw)

  }

  //计算相似度
  def matrixFactorizationForItem(itemFactorRaw:DataFrame): Unit ={
    //相似度计算
    val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
    val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
    similarPara.isSparse = false
    similarPara.topN = 60
    val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> itemFactorRaw)
    similarAlg.initInputData(similarDataMap)
    similarAlg.run()
    similarAlg.getOutputModel.output("similarAls/movie")
  }


}
