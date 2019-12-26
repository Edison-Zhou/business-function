package cn.moretv.doraemon.biz.peoplealsolike

import cn.moretv.doraemon.algorithm.als.{AlsAlgorithm, AlsModel}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.enum.ProductLineEnum

/**
  *
  * @author guo.hao
  * @since 2018/9/30 下午3:07
  * 基于movie数据生成ALS Model并存储到HDFS
  *
  */
@deprecated
object AlsModelMovie extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    // 用于获取评分数据
    val inputPath = "/user/hive/warehouse/ai.db/dw_base_behavior/product_line=moretv/partition_tag=V1/content_type=movie/*"
    val userScore = BizUtils.readUserScore(inputPath,300)
    // 用于筛选出活跃 
    //val activeUser = BizUtils.readActiveUser
    // 算法部分
    println("算法部分:")
    val alsAlgorithm = new AlsAlgorithm()
    val inputData = userScore
      //.join(activeUser,"user").select(userScore("*"))
    BizUtils.getDataFrameInfo(inputData,"inputData")
    val dataMap = Map(alsAlgorithm.INPUT_DATA_KEY -> inputData)
    alsAlgorithm.initInputData(dataMap)
    alsAlgorithm.run()

    println("模型结果样例打印:")
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.show
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.show

    println("保存模型数据到HDFS:/ai/model/medusa/ALS/movie")
    alsAlgorithm.modelOutput.save("movie")

  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}