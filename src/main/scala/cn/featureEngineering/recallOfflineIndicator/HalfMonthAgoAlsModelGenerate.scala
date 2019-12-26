package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.algorithm.als.{AlsAlgorithm, AlsModel}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.DateUtils

/**
  *
  * @author wang.baozhi
  * @updater Edison_zhou
  * @since 2019/7/15
  * 生成ALS Model并存储到测试环境指定的HDFS
  *
  */
object HalfMonthAgoAlsModelGenerate extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    // 用于获取评分数据
    val endDate = DateUtils.farthestDayWithDelimiter(15)
    val startDate = DateUtils.farthestDayWithDelimiter(180)
    val fullUserScore = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvLongVideoScore))
    val userScore = fullUserScore.filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'").
      selectExpr("userid as user", "sid_or_subject_code as item","score as rating")

    // 算法部分
    println("算法部分:")
    val alsAlgorithm = new AlsAlgorithm()
    BizUtils.getDataFrameInfo(userScore,"inputData")
    val dataMap = Map(alsAlgorithm.INPUT_DATA_KEY -> userScore)
    alsAlgorithm.initInputData(dataMap)
    alsAlgorithm.run()

    println("模型结果样例打印:")
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixU.show
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.printSchema()
    alsAlgorithm.getOutputModel.asInstanceOf[AlsModel].matrixV.show

    alsAlgorithm.modelOutput.save("halfmonthago")
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
