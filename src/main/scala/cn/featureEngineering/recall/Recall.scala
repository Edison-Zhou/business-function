package cn.featureEngineering.recall

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.constant.Constants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions.{collect_list, expr}
import org.apache.spark.storage.StorageLevel

/**
  * 用户喜好节目召回API
  *
  * @author Edison_Zhou
  * @since 2019/6/25
  */
@deprecated
object Recall extends BaseClass {

  //从数据库中取的近期更新的节目数量
  val numOfLatestUpdate = 25
  //数据库中取的当年的电影数量
  val numOfThisYearMovie = 15
  //从hot算法中获取的节目数量(热度从高到低)
  val numOfHotVideo = 20
  //从数据中获取的豆瓣高评分节目数量
  val numOfHighScore = 40
  //分数阈值设定为8.5
  val doubanScoreThreshold = 8.5
  //从个性化推荐结果获取节目
  val numOfPersonalizedRec = 100
  //搜索点击表的节目相似节目的数量
  val numOfSearchSimilar = 40

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS

    //有效电影的指定字段信息
    val validMovieInfo = DataReader.read(new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid", "updateTime", "year", "score"), "contentType = 'movie' and videoType = 0" +
        " and type = 1 and status = 1")).persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(validMovieInfo, "validMovieInfo")

    //有效电影sid
    val validMovie = validMovieInfo.select("sid")

    //通用推荐部分
    // 获取数据库中近期更新的电影数据
    val latestUpdateMovie = validMovieInfo.filter(s"updateTime >= '${DateUtils.lastDayWithDelimiter}'")
        .select("sid", "updateTime")
    BizUtils.getDataFrameInfo(latestUpdateMovie, "latestUpdateMovie")

    val latestUpdateMovieArr = ArrayUtils.randomTake(latestUpdateMovie.map(r => r.getString(0)).collect(), numOfLatestUpdate)
    latestUpdateMovieArr.foreach(println)

    //获取今年上映的电影节目
    val thisYearMovie = validMovieInfo.filter(s"year >= ${DateUtils.todayWithDelimiter.substring(0,4)}")
        .select("sid", "year")
    BizUtils.getDataFrameInfo(thisYearMovie, "thisYearMovie")

    val thisYearMovieArr = ArrayUtils.randomTake(thisYearMovie.map(r => r.getString(0)).collect(), numOfThisYearMovie)
    thisYearMovieArr.foreach(println)

    //获取近期高频次被播放的热门影片
    val hotPlayMovie = DataReader.read(BizUtils.getHdfsPathForRead("hotVideo/movie")).select("sid", "popularity")
    BizUtils.getDataFrameInfo(hotPlayMovie, "hotPlayMovie")

    val hotPlayMovieArr = hotPlayMovie.map(r => r.getAs[String]("sid")).collect().take(numOfHotVideo)
    for (i <- 0 until 10) println(hotPlayMovieArr(i))

    //获取豆瓣高评分的电影节目
    val highScoreMovie = validMovieInfo.filter(s"score>=$doubanScoreThreshold").select("sid", "score")
    BizUtils.getDataFrameInfo(highScoreMovie, "highScoreMovie")

    val highScoreMovieArr = ArrayUtils.randomTake(highScoreMovie.map(r => r.getString(0)).collect(), numOfHighScore)
    for (i <- 0 until 10) println(highScoreMovieArr(i))

    //将以上几种节目合并并去重，作为通用节目推荐池
    val generalHighQualityMovie = (latestUpdateMovieArr ++ thisYearMovieArr ++ hotPlayMovieArr ++ highScoreMovieArr).distinct
    println("合并及去重后的通用节目推荐数目为："+generalHighQualityMovie.length)

    //筛选出活跃用户
    val activeUser = BizUtils.readActiveUser
    //为活跃用户生成通用推荐结果
    val generalMovie4Rec = activeUser.map(r => (r.getLong(0), generalHighQualityMovie))
      .toDF("uid", "sidArr")
    BizUtils.getDataFrameInfo(generalMovie4Rec, "generalMovie4Rec")


    //个性化推荐部分 (由als、最后一部电影的相似影片、长视频聚类构成)
    //获得als推荐数据
    val alsRecommendMovie = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("ALS/recommend")),
      -1, Constants.ARRAY_OPERATION_RANDOM).persist(StorageLevel.MEMORY_AND_DISK).join(validMovie, "sid")
    //获得看过的最后一部电影的相似内容推荐
    val LastMoviesimilarRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("similarityRecommend")),
      50, Constants.ARRAY_OPERATION_TAKE).persist(StorageLevel.MEMORY_AND_DISK).join(validMovie, "sid")
    //获得长视频聚类推荐
    val MovieClusterRecommend = BizUtils.getUidSidDataFrame(DataReader.read(BizUtils.getHdfsPathForRead("longVideoClusterRecommend")),
      120, Constants.ARRAY_OPERATION_TAKE_AND_RANDOM).persist(StorageLevel.DISK_ONLY).join(validMovie, "sid")

    //使用推荐结果合并算法
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm
    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "uid"
    param.recommendItemColumn = "sid"
    //因为各个数据源都没有过滤地域屏蔽，所以这里多取一些数据
    param.recommendNum = 200
    param.outputOriginScore = false
    param.mixMode = MixModeEnum.RATIO
    param.ratio = Array(2, 1, 1)
    mixAlg.initInputData(Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> MovieClusterRecommend,
      mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> LastMoviesimilarRecommend, mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> alsRecommendMovie))
    mixAlg.run()
    //DataFrame[uid,sid]
    val df = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult.persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(df,"df")

    //电影个性化推荐的结果
    val personalizedMovie = df.join(validMovie, "sid")
    val personalizedMovie4Rec = BizUtils.rowNumber(personalizedMovie, "uid", "uid",
      numOfPersonalizedRec+1, false).groupBy("uid").agg(collect_list("sid"))
      .toDF("uid", "sidArr")
    BizUtils.getDataFrameInfo(personalizedMovie4Rec,"personalizedMovie4Rec")

    //一周内的搜索结果
    val searchRes = DataReader.read(new HivePath("select user_id, result_sid from ods_view.log_medusa_main4x_search_result_click " +
      s"where key_day >= '${DateUtils.farthestDayWithDelimiter(7)}'"))
      .map(r => (TransformUDF.calcLongUserId(r.getString(0)), r.getString(1))).distinct().toDF("uid", "sid")
    BizUtils.getDataFrameInfo(searchRes, "searchRes")

    val searchSimilar = searchRes.as("a").join(DataReader.read(BizUtils.getHdfsPathForRead("similarMix/movie"))
      .as("b"), expr("a.sid=b.sid")).selectExpr("a.uid as uid", "b.item as sid", "b.similarity as similarity")

    val searchSimilar4Rec = BizUtils.rowNumber(searchSimilar, "uid","similarity",
      numOfSearchSimilar+1, true).groupBy("uid").agg(collect_list("sid"))
      .toDF("uid", "sidArr")
    BizUtils.getDataFrameInfo(searchSimilar4Rec, "searchSimilar4Rec")

    val unionDF = personalizedMovie4Rec.as("a").join(generalMovie4Rec.as("b"), expr("a.uid = b.uid"),
      "full").join(searchSimilar4Rec.as("c"), expr("a.uid = c.uid"), "full")
      .selectExpr("case when a.uid is not null then a.uid when b.uid is not null then b.uid else c.uid end as uid",
        "a.sidArr as sidArrPersonalized", "b.sidArr as sidArrGeneral", "c.sidArr as sidArrBySearch")

    BizUtils.getDataFrameInfo(unionDF, "unionDF")

    val recallDistinctRes = unionDF.rdd.map(r => {
      val uid = r.getLong(0)
      var sidArr = Array[String]()
      val sidArrPersonalized = r.getSeq[String](1)
      val sidArrGeneral = r.getSeq[String](2)
      val sidArrBySearch = r.getSeq[String](3)
      if (sidArrPersonalized==null) {
        if (sidArrGeneral==null) sidArr = sidArrBySearch.toArray
        else if (sidArrBySearch==null) sidArr = sidArrGeneral.toArray
        else sidArr = (sidArrGeneral.toSet ++ sidArrBySearch.toSet).toArray
      }
      else if (sidArrPersonalized!=null && sidArrGeneral==null) {
        if (sidArrBySearch == null) sidArr = sidArrPersonalized.toArray
        else sidArr = (sidArrPersonalized.toSet ++ sidArrBySearch.toSet).toArray
      }
      else if (sidArrPersonalized!=null && sidArrGeneral!=null) {
        if (sidArrBySearch == null) sidArr = (sidArrPersonalized.toSet ++ sidArrGeneral.toSet).toArray
        else sidArr = (sidArrGeneral.toSet ++ sidArrPersonalized.toSet ++ sidArrBySearch.toSet).toArray.take(200)
      }
      (uid, sidArr)
    }).toDF("uid", "items")

    BizUtils.getDataFrameInfo(recallDistinctRes, "recallDistinctRes")
    BizUtils.outputWrite(recallDistinctRes, "recall/movie")

  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
