package cn.moretv.doraemon.biz.tabReorder

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.algorithm.kMeans.{KMeansAlgorithm, KMeansParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.KMeansParams
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.enum.ProductLineEnum

/**
  * 此类用于训练用户kMeans聚类模型(基于ALS的用户隐式矩阵)
  *
  * 类输入参数：
  * businessLine:   业务线（helios or medusa）
  * videoType:      视频类型
  * kNum:           kMeans聚类中心点
  * numIter:        kmenas模型迭代次数
  * numOfDays:      用户play日志所需的天数
  * score:          用户观看评分选择
  *
  * 模型输入参数：SparseVector ALS隐式矩阵
  *
  * 模型输出：kMeans模型落盘
  *
  * 数据存储路径：
  * 现网环境: /ai/model/medusa/${modelName}/${instanceName}
  *
  * @author wang.baozhi
  * @since 2018/9/27 下午3:41
  */
object KMeansTrainByALS extends BaseClass {

  def execute(args: Array[String]): Unit = {
    //----读取配置参数----
    val params: KMeansParams = KMeansParams.getParams(args)
    // val businessLine: String = params.businessLine
    val videoType: String = params.videoType
    val kNum: Int = params.kNum
    val numIter: Int = params.numIter
    val numOfDay = params.numOfDays
    val score = params.score

    println("videoType:" + videoType)
    println("kNum:" + kNum)
    println("numIter:" + numIter)
    println("numOfDay:" + numOfDay)
    println("score:" + score)

    //----数据部分----
    //1.从评分矩阵中获取近N天的活跃用户,DataFrame[uid<long type>]
    val activeUserInfo = BizUtils.getActiveUserFromScoreMatrix(numOfDay, score, videoType)

    //2.用户ALS数据,DataFrame
    val alsModel = new AlsModel()
    alsModel.load()

    // 算法部分
    val kMeansAlgorithm = new KMeansAlgorithm()
    val param = kMeansAlgorithm.getParameters.asInstanceOf[KMeansParameters]
    param.kNum = kNum
    param.numIter = numIter

    val dataMap = Map(kMeansAlgorithm.INPUT_ACTIVE_USER -> activeUserInfo,
      kMeansAlgorithm.INPUT_USER_ALS -> alsModel.matrixU)
    kMeansAlgorithm.initInputData(dataMap)
    kMeansAlgorithm.run()
    kMeansAlgorithm.getOutputModel.save(videoType)
  }
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
