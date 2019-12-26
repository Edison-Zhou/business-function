package cn.moretv.doraemon.biz.detail

import cn.moretv.doraemon.algorithm.relevantSubject.{RelevantSubjectAlgorithm, RelevantSubjectParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2018/8/22.
  */
object SubjectRecommend extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    var videoId2subject = BizUtils.getVideoId2subject //数据库中的sid不限制是真实的还是虚拟的，有虚拟sid的节目也可以把真实sid编排到专题
      .selectExpr("item_sid", "subject_code", "item_contentType")

    //转虚拟id
    videoId2subject = BizUtils.transferToVirtualSid(videoId2subject, "item_sid")

    val availableSubject = BizUtils.getAvailableSubject

    for(contentType <- contentTypeList) {
      //读入节目对应的Word2Vec数据
      val videoFeaturePath: HdfsPath = new HdfsPath(s"/ai/data/medusa/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
      val videoFeature = DataReader.read(videoFeaturePath)
        .rdd.map(line => line.getString(0).split(",")).
        map(e => (e(0), e.takeRight(128))).
        map(e => (e._1, e._2.map(x => x.toDouble)))
        .toDF("sid", "feature")

      val word2VecSidDF =  BizUtils.transferToVirtualSid(videoFeature, "sid")

      val availableVideo = BizUtils.getAvailableVideo(contentType).selectExpr("sid")

      val relevantSubjectAlg: RelevantSubjectAlgorithm = new RelevantSubjectAlgorithm()
      val param: RelevantSubjectParameters = new RelevantSubjectParameters
      param.subjectNum = 10

      relevantSubjectAlg.initInputData(Map(relevantSubjectAlg.INPUT_VIDEOID2SUBJECT_KEY -> videoId2subject,
        relevantSubjectAlg.INPUT_AVAILABLESUBJECT_KEY -> availableSubject,
        relevantSubjectAlg.INPUT_AVAILABLEVIDEO_KEY -> availableVideo,
        relevantSubjectAlg.INPUT_VIDEOFEATURE_KEY -> word2VecSidDF))

      relevantSubjectAlg.run()

      //输出到HDFS
      relevantSubjectAlg.getOutputModel.output("subject/" + contentType)
    }
  }
}
