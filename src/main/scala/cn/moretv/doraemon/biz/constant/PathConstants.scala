package cn.moretv.doraemon.biz.constant

/**
  *
  *  用于生产环境,整理后的的路径
  * Created by baozhiwang on 2018/6/20.
  * 规范输出目录后
  //http://doc.moretv.com.cn/pages/viewpage.action?pageId=17577415
  */

object PathConstants {

  //LongVideoClusterRecommend  used
  val pathOfMoretvUserLongVideoClusterFeatures = "/ai/data/medusa/base/word2vec/user/longVideoClusterFeature"
  val pathOfMoretvFrontPageExposureLongVideos = "/ai/etl/ai_base_behavior_display/product_line=moretv/biz=portalRecommend_ex/key_time=#{date}"
  val pathOfLongVideoClusters = "/ai/data/medusa/videoFeatures/longVideoClusters"

  /**
    * 评分的全量聚合数据。
    * 作用：1.计算活跃用户
    * */
  val pathOfMoretvLongVideoScore = "/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=V1/content_type={movie,tv,zongyi,jilu,comic,kids}/*"

  //每日的评分数据，使用时候hdfsPath需要输入 dateRanger
  val pathOfMoretvLongVideoScoreByDay = "/ai/etl/ai_base_behavior_raw/product_line=moretv/partition_tag=#{date}/score_source=play/*"


  //SimilarityRecommend used
  val pathOfMoretvMovieScore = "/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=V1/content_type=movie/*"
  val pathOfMoretvSimilarMovie = "/ai/dws/moretv/biz/detailPage/similar/mixedTagAndCluster/movie"

  //FrontPageRecommend used
  //追剧暂时用原来的计算结果
  val pathOfMoretvSeriesChasingRecommend = "/ai/data/dw/moretv/seriesChasingRecommend"
  //byContentType暂时用原来的计算结果,应该使用ALSRecommend的计算结果来代替
  val pathOfMoretvALSResultByContentType = "/ai/data/dw/moretv/ALSResult/byContentType"
  val pathOfSelectMovies = "/ai/dw/moretv/similar/default/movie/defaultSimilarMovies.csv"

  //ALSRecommend used
  val weightOfSelectMovies = 1.1
  val pathOfMoretvLongVideoHistory = "/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=longVideoHistory/content_type={movie,tv,zongyi,jilu,comic,kids}/*"
  val pathOfMoretvMovieHistory = "/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=longVideoHistory/content_type=movie"

  //SeriesChasingRecommend used
  val pathOfMoretvSeriesScore = s"/ai/etl/ai_base_behavior_raw_episode/product_line=moretv/score_source=play/partition_tag=#{date}"
  val pathOfMoretvProgram = "/data_warehouse/dw_dimensions/dim_medusa_program"

  //ALS模型生成使用
  val pathOfMoretvActiveUser = "/ai/data/dw/moretv/userBehavior/activeUser"

  //猜你喜欢使用
  val pathOfScore="/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=V1/content_type=contentTypeVar/*"
  val pathOfScoreAllContentType="/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=V1/*"

  //mysql info
  val moretvMetaData: String = "moretv_recommend_mysql"
  val heliosMetaData: String = "helios_recommend_mysql"

  //站点树重排序使用
  val MEDUSA_ABTEST_URL = "http://10.19.43.203:3456/config/abTest?userId=uid&version=moretv"

  //用户操作日志,[uid,optNum]，用于生成pathOfMoretvActiveUser数据使用
  val pathOfMoretvUserDailyOperation = "/ai/data/dw/moretv/userBehavior/userDailyOperation/"


}
