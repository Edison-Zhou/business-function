package cn.featureEngineering.featureExtraction

import cn.featureEngineering.dataUtils.ETLSparse
import cn.moretv.doraemon.biz.BaseClass
import cn.featureEngineering.dataUtils
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/11/7.
  */
@deprecated
object VideoAndUserFeature extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val trunNum = 50000
    val keyWord = "Trunc" + trunNum

    val trainData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/trainSet10/Latest"))
        //.selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
        .selectExpr("cast (uid as string)", "sid", "score")

//    val videoTags = dataUtils.Read.getVideoTag2("movie")
    val videoTags = dataUtils.Read.getVideoTag4("movie")
    //videoTags -> DF[(tag_id, tag_name, sid, rating)]，其中rating是可调参数
    val truncVideoTags = ETLSparse.tagFrequencyTruncation(videoTags, trunNum).persist(StorageLevel.MEMORY_ONLY)

    val tagReIndexDF = ETLSparse.tagReIndex(truncVideoTags.select("tag_id", "tag_name"))
    BizUtils.outputWrite(tagReIndexDF, "featureEngineering/tagReIndex")

    val videoFeaturesDF = ETLSparse.tagReIndex2VideoFeatures(truncVideoTags.select("tag_id", "sid", "rating"), tagReIndexDF)
      .persist(StorageLevel.MEMORY_ONLY)
    BizUtils.outputWrite(videoFeaturesDF, "featureEngineering/features/videoFeatures"+ keyWord + "SparseD10")

    val userFeaturesDF = ETLSparse.union2userFeatures(videoFeaturesDF, trainData)
    BizUtils.outputWrite(userFeaturesDF, "featureEngineering/features/userFeatures"+ keyWord +"SparseD10")
  }

}
