package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.storage.StorageLevel

/**
  * 为活跃用户生成其兴趣召回结果
  *
  * @author Edison_Zhou
  * @since 2019/6/25
  */
object HalfMonthAgoRecallWithSource extends BaseClass {

  //从数据库中取的近期更新的节目数量
  val numOfLatestUpdate = 5
  //数据库中取的当年的电影数量
  val numOfThisYearMovie = 15
  //从hot算法中获取的节目数量(热度从高到低)
  val numOfHotVideo = 40
  //从数据中获取的豆瓣高评分节目数量
  val numOfHighScore = 30
  //分数阈值设定为8
  val doubanScoreThreshold = 8
  //上个月观看的电影个数选取上限
  val numOflastMonthWatched = 5
  //搜索点击表的节目相似节目的数量
  val numOfSearchSimilar = 20
  //期望在召回模块中为每个用户推荐的影片总量
  val numOfRecallRec = 200

  //有效电影的指定字段信息
  val validMovieInfo = DataReader.read(new MysqlPath("bigdata-appsvr-130-4", 3306,
    "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
    Array("sid", "updateTime", "year", "score", "risk_flag"), "contentType = 'movie' and videoType = 0" +
      " and type = 1 and status = 1")).persist(StorageLevel.MEMORY_AND_DISK)


  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS

    BizUtils.getDataFrameInfo(validMovieInfo, "validMovieInfo")
    //有效电影sid
    val validMovie = validMovieInfo.select("sid")
    //电影风险等级
    val movieRisk = validMovieInfo.select("sid", "risk_flag")

    //通用推荐部分
    // 获取数据库中近期更新的电影数据
    val latestUpdateMovie = validMovieInfo.filter(s"updateTime >= '${DateUtils.lastDayWithDelimiter}'")
        .select("sid", "updateTime")
    BizUtils.getDataFrameInfo(latestUpdateMovie, "latestUpdateMovie")

    val latestUpdateMovieArr = ArrayUtils.randomTake(latestUpdateMovie.map(r => r.getString(0)).collect(), numOfLatestUpdate)
    latestUpdateMovieArr.foreach(println)
    val latestUpdateMovieMap = latestUpdateMovieArr.map(e => (e, "latestUpdate")).toMap

    //获取今年上映的电影节目
    val thisYearMovie = validMovieInfo.filter(s"year >= ${DateUtils.todayWithDelimiter.substring(0,4)}")
      .select("sid", "year")
    BizUtils.getDataFrameInfo(thisYearMovie, "thisYearMovie")

    val thisYearMovieArr = ArrayUtils.randomTake(thisYearMovie.map(r => r.getString(0)).collect(), numOfThisYearMovie)
    thisYearMovieArr.foreach(println)
    val thisYearMovieMap = thisYearMovieArr.map(e => (e, "thisYear")).toMap

    //获取近期高频次被播放的热门影片
    val hotPlayMovie = DataReader.read(BizUtils.getHdfsPathForRead("halfMonthAgoHotVideo/movie")).select("sid", "popularity")
    BizUtils.getDataFrameInfo(hotPlayMovie, "hotPlayMovie")

    val hotPlayMovieArr = hotPlayMovie.map(r => r.getAs[String]("sid")).collect().take(numOfHotVideo)
    for (i <- 0 until 10) println(hotPlayMovieArr(i))
    val hotPlayMovieMap = hotPlayMovieArr.map(e => (e, "hotPlay")).toMap

    //获取豆瓣高评分的电影节目
    val highScoreMovie = validMovieInfo.filter(s"score>$doubanScoreThreshold").select("sid", "score")
    BizUtils.getDataFrameInfo(highScoreMovie, "highScoreMovie")

    val highScoreMovieArr = ArrayUtils.randomTake(highScoreMovie.map(r => r.getString(0)).collect(), numOfHighScore)
    for (i <- 0 until 10) println(highScoreMovieArr(i))
    val highScoreMovieMap = highScoreMovieArr.map(e => (e, "highScore")).toMap

    //将以上几种节目合并并去重，作为通用节目推荐池
    val generalRecMovieMap = latestUpdateMovieMap ++ thisYearMovieMap ++ hotPlayMovieMap ++ highScoreMovieMap
    println("合并及去重后的通用节目推荐数目为：" + generalRecMovieMap.size)

    val generalRecWithSource = sc.parallelize(generalRecMovieMap.toSeq).toDF("sid", "source")

    //筛选出活跃用户
    val activeUser = BizUtils.readActiveUser
    //为活跃用户生成通用推荐结果
    val generalMovieUidSid = activeUser.flatMap(r => generalRecMovieMap.toArray.map(e => (r.getLong(0), e._1)))
      .toDF("uid", "sid").repartition(1000)
    BizUtils.getDataFrameInfo(generalMovieUidSid, "generalMovieUidSid")


    //个性化推荐部分 (由als、看过的最后一部电影的相似影片、长视频聚类、搜索点击的相似影片及用户观影高评分构成)
    //获得als推荐数据
    val alsRecommendMovie = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("ALS/rec4moviehalfmonthago")),
      100, Constants.ARRAY_OPERATION_TAKE_AND_RANDOM).repartition(1000).join(validMovie, "sid")
      .as("a").join(activeUser.as("b"), expr("a.uid =b.user")).selectExpr("a.uid as uid", "a.sid as sid")
    //获得看过的最后一部电影的相似内容推荐
    val LastMoviesimilarRecommend = BizUtils.getUidSidDataFrame(DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/similarityRecommendHalfMonthAgo/Latest")),
      10, Constants.ARRAY_OPERATION_TAKE).join(validMovie, "sid").as("a")
      .join(activeUser.as("b"), expr("a.uid =b.user")).selectExpr("a.uid as uid", "a.sid as sid")
    //获得长视频聚类推荐
    val MovieClusterRecommend = BizUtils.getUidSidDataFrame(DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/HalfMonthAgoCluster/Latest")),
      120, Constants.ARRAY_OPERATION_TAKE_AND_RANDOM).repartition(1000).join(validMovie, "sid")
      .as("a").join(activeUser.as("b"), expr("a.uid =b.user")).selectExpr("a.uid as uid", "a.sid as sid")
    BizUtils.getDataFrameInfo(alsRecommendMovie,"alsRecommendMovie")
    BizUtils.getDataFrameInfo(LastMoviesimilarRecommend,"LastMoviesimilarRecommend")
    BizUtils.getDataFrameInfo(MovieClusterRecommend,"MovieClusterRecommend")

    //获取用户以15天前为时间基线的前一个月观看的电影信息
    val startDate = DateUtils.farthestDayWithDelimiter(75)
    val endDate = DateUtils.farthestDayWithDelimiter(46)
    val userWatchedMovie = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvMovieScore))
      .filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'").selectExpr("userid as uid", "sid_or_subject_code as sid", "score")
    val userWatchedMovieTrunc = BizUtils.rowNumber(userWatchedMovie, "uid", "score", numOflastMonthWatched,
      true).repartition(1000).join(validMovie, "sid").as("a")
      .join(activeUser.as("b"), expr("a.uid =b.user")).selectExpr("a.uid as uid", "a.sid as sid")
    BizUtils.getDataFrameInfo(userWatchedMovieTrunc, "userWatchedMovieTrunc")

    //一周内的搜索结果
    val searchRes = DataReader.read(new HivePath("select user_id, result_sid from ods_view.log_medusa_main4x_search_result_click " +
      s"where key_day >= '${DateUtils.farthestDayWithDelimiter(7)}'"))
      .map(r => (TransformUDF.calcLongUserId(r.getString(0)), r.getString(1))).distinct().toDF("uid", "sid").repartition(1000)
    BizUtils.getDataFrameInfo(searchRes, "searchRes")

    val searchSimilarMovie = searchRes.as("a").join(DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/similarMix/movie/Latest"))
      .as("b"), expr("a.sid=b.sid")).selectExpr("a.uid as uid", "b.item as sid", "b.similarity as similarity")

    val searchSimilarUidSid = BizUtils.rowNumber(searchSimilarMovie, "uid","similarity",
      numOfSearchSimilar+1, true).select("uid", "sid").as("a")
      .repartition(1000).join(activeUser.as("b"), expr("a.uid =b.user")).selectExpr("a.*")
    BizUtils.getDataFrameInfo(searchSimilarUidSid, "searchSimilarUidSid")

    println("调用推荐结果合并算法前的时间戳为："+DateUtils.getTimeStamp)
    //使用推荐结果合并算法
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm
    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "uid"
    param.recommendItemColumn = "sid"
    //因为各个数据源都没有过滤地域屏蔽，所以这里多取一些数据
    param.recommendNum = 200
    param.outputOriginScore = false
    param.mixMode = MixModeEnum.STACKING
    mixAlg.initInputData(Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> LastMoviesimilarRecommend,
      mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> userWatchedMovieTrunc,
      mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> MovieClusterRecommend, mixAlg.INPUT_DATA_KEY_PREFIX + "4" -> alsRecommendMovie))
    mixAlg.run()
    println("调用推荐结果合并算法后的时间戳为："+DateUtils.getTimeStamp)
    //DataFrame[uid,sid]
    //电影个性化推荐的结果
    val personalizedMovieUidSid = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult.select("uid", "sid")
    BizUtils.getDataFrameInfo(personalizedMovieUidSid,"personalizedMovieUidSid")

    val alsRecWithSource = alsRecommendMovie.withColumn("source", lit("als"))
    val lastSimilarWithSource = LastMoviesimilarRecommend.withColumn("source", lit("lastSimilar"))
    val userWatchedWithSource = userWatchedMovieTrunc.withColumn("source", lit("userWatched"))
    val clusterWithSource = MovieClusterRecommend.withColumn("source", lit("cluster"))
    val personalizedRecWithSource = alsRecWithSource.union(lastSimilarWithSource).union(userWatchedWithSource).union(clusterWithSource)
    BizUtils.getDataFrameInfo(personalizedRecWithSource, "personalizedRecWithSource")

    val unionDF = personalizedMovieUidSid.union(generalMovieUidSid).repartition(2000)
    BizUtils.getDataFrameInfo(unionDF, "unionDF")
    //过滤曝光和已看
    val frontPageExposedLongVideos = BizUtils.getContentImpression(22, DateUtils.farthestDayWithOutDelimiter(15))
    BizUtils.getDataFrameInfo(frontPageExposedLongVideos, "filteredLongVideos")
    val recallUidSid = BizUtils.uidSidFilter(unionDF, frontPageExposedLongVideos,"left","black")
      .persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(recallUidSid, "recallUidSid")

    //过滤地域屏蔽
    val recallDistinctWithRiskFilter = filterRiskFlag(recallUidSid).distinct()
    BizUtils.getDataFrameInfo(recallDistinctWithRiskFilter, "recallDistinctWithRiskFilter")

/*  val recallDistinctRes = recallUidSid.groupBy("uid").agg(collect_list("sid"))
      .map(r => (r.getLong(0), r.getSeq[String](1).toArray.distinct.take(numOfRecallRec))).toDF("uid", "items")*/

    val recallRes = BizUtils.rowNumber(recallDistinctWithRiskFilter, "uid", "uid",
      numOfRecallRec+1, false)

/*
    val recall4ActiveUser = recallRes.as("a").join(activeUser.as("b"), expr("a.uid =b.user"))
      .selectExpr("a.*")*/

    BizUtils.getDataFrameInfo(recallRes, "recallRes")
    val recallResFromGeneral = recallRes.join(generalRecWithSource, "sid").select("uid", "sid", "source")
    val recallResFromPersonalized = recallRes.join(personalizedRecWithSource, Seq("uid", "sid")).select("uid", "sid", "source")
    val recallResWithSource = recallResFromGeneral.union(recallResFromPersonalized).dropDuplicates("uid", "sid")
    BizUtils.getDataFrameInfo(recallResWithSource, "recallResWithSource")

    BizUtils.outputWrite(recallRes, "recall/halfMonthAgoMovie")
    BizUtils.outputWrite(recallResWithSource, "recall/halfMonthSourceTrack")

    }

  /**
    * 过滤地域屏蔽
    * 要求输入的DataFrame中包含uid，sid这两列
    * @param recommend  格式为：DataFrame[Row(uid,sid, 其他列....)],recommend可能包含不止uid,sid的列
    * @return DataFrame  recommend的子集
    */
  def filterRiskFlag(recommend: DataFrame): DataFrame = {
    //获取用户风险等级
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS
    val userRiskPath = new HivePath("select a.user_id, b.dangerous_level " +
      "from dw_dimensions.dim_medusa_terminal_user a left join dw_dimensions.dim_web_location b " +
      "on a.web_location_sk = b.web_location_sk " +
      "where a.dim_invalid_time is null and b.dangerous_level > 0")
    val userRisk = DataReader.read(userRiskPath).map(e => (TransformUDF.calcLongUserId(e.getString(0)), e.getInt(1))).toDF("uid", "userRisk")
    BizUtils.getDataFrameInfo(userRisk, "userRisk")

    //获取视频风险等级
    val movieRisk  = validMovieInfo.select("sid", "risk_flag").toDF("sid", "videoRisk")
    BizUtils.getDataFrameInfo(movieRisk, "movieRisk")
    val finalRecommend=recommend.join(userRisk, recommend("uid") === userRisk("uid"), "left")
      .join(movieRisk, recommend("sid") === movieRisk("sid"), "left")
      .where(expr("if(userRisk is null,0,userRisk)+if(videoRisk is null,0,videoRisk) <= 2"))
      .select(recommend("*"))

    finalRecommend
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
