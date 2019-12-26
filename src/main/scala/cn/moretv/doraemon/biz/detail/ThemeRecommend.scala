package cn.moretv.doraemon.biz.detail

import cn.moretv.doraemon.algorithm.randomCluster.{RandomClusterAlgorithm, RandomClusterParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath

/**
  * Created by cheng_huan on 2018/8/13.
  * 聚类数据基于评分结果
  */
object ThemeRecommend extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")
    val ss = spark
//    import ss.implicits._

    contentTypeList.foreach(contentType => {
      //视频聚类数据
      val videoCluster = DataReader.read(new HdfsPath(s"/ai/data/medusa/videoFeatures/${contentType}Clusters/Latest"))

      //有效影片数据
      val validSid = BizUtils.getAvailableVideo(contentType)

      //根据聚类结果推荐
      val randomClusterAlg: RandomClusterAlgorithm = new RandomClusterAlgorithm()
      val param = randomClusterAlg.getParameters.asInstanceOf[RandomClusterParameters]
      param.numOfCluster = 120
      param.numOfVideo = 1

      randomClusterAlg.initInputData(
        Map(randomClusterAlg.INPUT_VIDEOCLUSTER_KEY -> videoCluster,
          randomClusterAlg.INPUT_VALIDVIDEO_KEY -> validSid)
      )

      randomClusterAlg.run()

      //输出到HDFS
      randomClusterAlg.getOutputModel.output("theme/" + contentType)
    })
  }
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
