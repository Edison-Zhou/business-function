package cn.moretv.doraemon.biz.portalRecommend

import cn.moretv.doraemon.algorithm.als.{AlsAlgorithm, AlsModel}
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils

/**
  *
  * @author wang.baozhi
  * @since 2018/8/5 下午3:07
  * 生成ALS Model并存储到HDFS
  *
  */
object AlsModelGenerate extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    // 用于获取评分数据
    val userScore = BizUtils.readUserScore(PathConstants.pathOfMoretvLongVideoScore,300)

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

    println("保存模型数据到HDFS:/ai/model/medusa/ALS/default")
    alsAlgorithm.modelOutput.save()
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}