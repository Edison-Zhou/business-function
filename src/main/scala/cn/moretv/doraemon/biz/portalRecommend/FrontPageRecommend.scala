package cn.moretv.doraemon.biz.portalRecommend

import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.{ArrayUtils, TransformUtils}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.biz.BaseClass
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.storage.StorageLevel

/**
  * 首页今日推荐
  *
  * @author wang.baozhi
  * @since 2018/7/19 下午3:23
  */
object FrontPageRecommend extends BaseClass {

  //从各个推荐结果中取的数据量
  val numOfRecommend = 100
  //每个用户最多推荐sid的数量
  val numOfRecommend2Kafka = 50

  override def execute(args: Array[String]): Unit = {
    TransformUDF.registerUDFSS
    //获得als推荐数据
    val alsRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("ALS/recommend")), -1, Constants.ARRAY_OPERATION_RANDOM).persist(StorageLevel.MEMORY_AND_DISK)
    //获得追剧推荐数据
    val seriesChasingRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("seriesChasingRecommend")), numOfRecommend, Constants.ARRAY_OPERATION_TAKE).persist(StorageLevel.MEMORY_AND_DISK)
    //获得看过的最后一部电影的相似内容推荐
    val similarityRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("similarityRecommend")), numOfRecommend, Constants.ARRAY_OPERATION_TAKE).persist(StorageLevel.MEMORY_AND_DISK)
    //获得长视频聚类推荐
    val longVideoClusterRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("longVideoClusterRecommend")), numOfRecommend, Constants.ARRAY_OPERATION_TAKE_AND_RANDOM).persist(StorageLevel.DISK_ONLY)

    //使用推荐结果合并算法
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm
    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "uid"
    param.recommendItemColumn = "sid"
    //因为各个数据源都没有过滤地域屏蔽，所以这里多取一些数据
    param.recommendNum = 200
    param.outputOriginScore = false
    param.mixMode = MixModeEnum.RATIO
    param.ratio = Array(3, 1, 1, 1)
    mixAlg.initInputData(Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> longVideoClusterRecommend, mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> seriesChasingRecommend, mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> similarityRecommend, mixAlg.INPUT_DATA_KEY_PREFIX + "4" -> alsRecommend))
    mixAlg.run()
    //DataFrame[uid,sid]
    val df = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult.persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(df,"df")

    //不需过滤曝光
    //val frontPageExposedLongVideos = BizUtils.getDataFrameWithDataRange(PathConstants.pathOfMoretvFrontPageExposureLongVideos, 1).select("userid","sid").withColumnRenamed("userid","uid")
    /*val frontPageExposedLongVideos = BizUtils.getContentImpression(0)
    val userWatchedLongVideos = BizUtils.getUserWatchedLongVideo(120)
    val filteredLongVideos = frontPageExposedLongVideos.union(userWatchedLongVideos).repartition(2000)
    val dataFrameRecommend = BizUtils.uidSidFilter(df,filteredLongVideos,"left","black").persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(dataFrameRecommend,"dataFrameRecommend")*/

    //统一过滤地域屏蔽,输入dataFrameRecommend，输出也是dataFrameRecommend，dataFrameRecommend可能包含不止uid,sid的列
    val dataFrameRecommendWithRiskFilter =BizUtils.filterRiskFlag(df)
    BizUtils.getDataFrameInfo(dataFrameRecommendWithRiskFilter,"dataFrameRecommendWithRiskFilter")

    //---首页个性化的推荐，对应首页今日推荐模块最后三个推荐位---
    //存入HDFS 每个uid最多存入10个sid
    val recommend2UserForHdfs=BizUtils.rowNumber(dataFrameRecommendWithRiskFilter,"uid",param.scoreColumn,51,true)
    BizUtils.getDataFrameInfo(recommend2UserForHdfs,"recommend2UserForHdfs")

    //插入kafka 每个uid最多存入50个sid
    val recommend2UserForKafka=BizUtils.rowNumber(dataFrameRecommendWithRiskFilter,"uid",param.scoreColumn,51,true)
    BizUtils.getDataFrameInfo(recommend2UserForKafka,"recommend2UserForKafka")

    //取默认结果
    /*val path = new HdfsPath(PathConstants.pathOfSelectMovies, FileFormatEnum.CSV)
    val editorSelectMovies = DataReader.read(path).rdd.map(x => TransformUtils.transferSid(x.getString(0))).collect()
    val defaultRecommendForKafka = ArrayUtils.randomTake(editorSelectMovies, numOfRecommend2Kafka)
    println("defaultRecommendForKafka.count():"+defaultRecommendForKafka.size)*/


    //---首页会员看看的推荐---
    //uid,sid,score
    val vipALS=DataReader.read(BizUtils.getHdfsPathForRead("ALS/vipRecommend"))
    BizUtils.getDataFrameInfo(vipALS,"vipALS")

    //过滤掉首页今日推荐中推荐的数据
    val vipALSFilterTodayRecommend = BizUtils.uidSidFilter(vipALS,dataFrameRecommendWithRiskFilter,"left","black").persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(vipALSFilterTodayRecommend,"vipALSFilterTodayRecommend")
    //地域屏蔽
    val vipALSRecommendWithRiskFilter =BizUtils.filterRiskFlag(vipALSFilterTodayRecommend)
    BizUtils.getDataFrameInfo(vipALSRecommendWithRiskFilter,"vipALSRecommendWithRiskFilter")

    //取前50个节目
    val vipRecommend2UserForHdfs=BizUtils.rowNumber(vipALSRecommendWithRiskFilter,"uid",param.scoreColumn,51,true)
    BizUtils.getDataFrameInfo(vipRecommend2UserForHdfs,"vipRecommend2UserForHdfs")
    //打乱顺序随机，取前20
    val homePageVip=BizUtils.getUidSidDataFrame(vipRecommend2UserForHdfs, 20, Constants.ARRAY_OPERATION_TAKE_AND_RANDOM)
    BizUtils.getDataFrameInfo(homePageVip,"homePageVip")

    //插入kafka 每个uid最多存入50个sid
    val vipRecommend2UserForKafka=BizUtils.rowNumber(vipALSRecommendWithRiskFilter,"uid",param.scoreColumn,51,true)
    /*val vipSidArray=BizUtils.getVipSid(0).rdd.map(e=>e.getInt(0)).collect()
    val vipDefaultRecommendForKafka=ArrayUtils.randomArray(vipSidArray).take(numOfRecommend2Kafka)
    println("vipDefaultRecommendForKafka.count():"+vipDefaultRecommendForKafka.size)*/

    //插入数据到HDFS
    //for首页今日推荐
    // 将兴趣推荐和首页的结果交换
    BizUtils.outputWrite(recommend2UserForHdfs,"interest/offline")
    //for首页会员看看
    BizUtils.outputWrite(homePageVip,"homePage/vip")

    //插入数据到kafka
    //for默认
    /*BizUtils.defaultRecommend2Kafka4Couchbase("p:v:", vipDefaultRecommendForKafka, "default", ConfigUtil.get("couchbase.moretv.topic"), numOfRecommend2Kafka)
    BizUtils.defaultRecommend2Kafka4Couchbase("p:p:", defaultRecommendForKafka, "default_test", ConfigUtil.get("couchbase.moretv.topic") ,numOfRecommend2Kafka)*/
    //for个性化
    BizUtils.recommend2Kafka4Couchbase("p:v:", vipRecommend2UserForKafka,"ALS", ConfigUtil.get("couchbase.moretv.topic"))

    val homePageRecDF = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("homePage/rec")), numOfRecommend2Kafka, Constants.ARRAY_OPERATION_TAKE)
    BizUtils.recommend2Kafka4Couchbase("p:p:", homePageRecDF,"original.cluster", ConfigUtil.get("couchbase.moretv.topic"))
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa


}
