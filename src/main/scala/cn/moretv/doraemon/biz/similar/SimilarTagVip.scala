package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.whaley.sdk.utils.TransformUDF

/**
  *
  * created by michael on 2019/2/14
  *
  * 基于标签相似度的vip相似影片推荐
  */
object SimilarTagVip extends BaseClass {
  def execute(args: Array[String]): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    //读入节目对应的标签的数据
    val tagSidPath: HdfsPath = new HdfsPath("/data_warehouse/dw_normalized/videoTagFeature/current",
      "select videoSid as sid, tagFeatures as vector from tmp")
    var tagSidDF = DataReader.read(tagSidPath)
      .selectExpr("sid", "vector")

    tagSidDF = BizUtils.transferToVirtualSid(tagSidDF, "sid")

    tagSidDF.persist()

    contentTypeList.foreach(contentType => {
      //有效影片的数据
      val validSidDF = BizUtils.getAvailableVipVideo(contentType)


      //数据的有效性检查
      val validAlg: ValidationCheckAlgorithm = new ValidationCheckAlgorithm()
      val validPara = validAlg.getParameters.asInstanceOf[ValidationCheckParameters]
      validPara.userOrItem = "item"

      val validDataMap = Map(validAlg.INPUT_DATA_KEY -> tagSidDF, validAlg.INPUT_CHECKLIST_KEY -> validSidDF)
      validAlg.initInputData(validDataMap)
      validAlg.run()

      val validTagSidDF = validAlg.getOutputModel.asInstanceOf[ValidationCheckModel].checkedData

      //相似度计算
      val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
      val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
      similarPara.isSparse = true
      similarPara.topN = 60
      val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> validTagSidDF)
      similarAlg.initInputData(similarDataMap)
      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarTagVip/" + contentType)
    })

  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
