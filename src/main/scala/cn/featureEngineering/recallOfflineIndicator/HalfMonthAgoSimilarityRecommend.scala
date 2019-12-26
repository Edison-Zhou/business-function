package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.algorithm.similar.last4offlineindicator.{Last4OfflineIndicatorAlgorithm, Last4OfflineIndicatorParameter}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.DateUtils

/**
  *
  * @author Edison_Zhou
  * @since 2019/8/12 下午7:38
  *
  *        用于给用户推荐半个月以前及更早时候看过的最后一部电影的相似内容
  */
object HalfMonthAgoSimilarityRecommend extends BaseClass {
  //原来生产路径 val pathOfMoretvSimilarityRecommend = "/ai/data/dw/moretv/SimilartityRecommend"
  //临时路径
  //val pathOfMoretvSimilarityRecommend = "/ai/data/dw/moretv_tmp"+File.separator+"SimilartityRecommend"

  override def execute(args: Array[String]): Unit = {
    //1.获取用户观看的最后一部电影，用于推荐
    val userWatchedLastMovies = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))

    //2.获取相似影片数据
    val similarMovies =DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/similarMix/movie/Latest"))

    //3.获取用户近期看过的电影，用于过滤
    val userWatchedMovies = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))

    //4.获取首页曝光给用户的长视频
    val frontPageExposureLongVideos = BizUtils.getContentImpression(22, DateUtils.farthestDayWithOutDelimiter(15))

    //算法部分
    //todo 待将新的算法包部署至线上集群后调用
    val similarLatestAlgorithm = new Last4OfflineIndicatorAlgorithm
    val longVideoClusterParameters = similarLatestAlgorithm.getParameters.asInstanceOf[Last4OfflineIndicatorParameter]
    val similarLatestDataMap = Map(similarLatestAlgorithm.INPUT_USER_WATCHED_LAST_MOVIES->userWatchedLastMovies,
      similarLatestAlgorithm.INPUT_SIMILAR_MOVIE -> similarMovies,
      similarLatestAlgorithm.INPUT_USER_WATCHED_MOVIES -> userWatchedMovies,
      similarLatestAlgorithm.INPUT_FRONT_PAGE_EXPOSED_LONG_VIDEOS -> frontPageExposureLongVideos)

    similarLatestAlgorithm.initInputData(similarLatestDataMap)
    similarLatestAlgorithm.run()
    //输出目录为："/ai/output/medusa/similarityRecommend/Latest"
    similarLatestAlgorithm.getOutputModel.output("similarityRecommendHalfMonthAgo")
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
