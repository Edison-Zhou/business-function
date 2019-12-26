package cn.featureEngineering.recallOfflineIndicator

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, HivePath, MysqlPath}
import cn.moretv.doraemon.common.util.DateUtils
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author Edison_Zhou
  * @since 2019/7/9
  * 使用ALS模型进行电影推荐
  * 作为200个召回结果当不足个数时的补充
  */
object HalfMonthAgoALSRec4Movie extends BaseClass {

  /**
    * 构造根据得分将视频递减排序的类
    * */
  class OrderByScore extends Ordering[(String,Float)]{
    override def compare(x:(String,Float),y:(String,Float))={
      if(x._2 > y._2) -1 else if(x._2 == y._2) 0 else 1
    }
  }
  /**
    * 用ALS的结果进行推荐
    *
    * @param userFactorsRaw ALS训练的用户特征
    * @param itemFactorsRaw ALS训练的视频特征
    * @param topN 推荐视频数目
    * @param videoDataDF 有效视频数据
    * @param weighedVideos 需要加权的视频
    * @param userWatchedDF 用户看过的视频
    * @param userExposedDF 曝光给用户过的视频
    * @return DataFrame[(uid, sid，score)]
    */
  def ALSRecommend(userFactorsRaw:DataFrame,
                   itemFactorsRaw:DataFrame,
                   topN: Int,
                   videoDataDF: DataFrame,
                   weighedVideos: Map[String, Double],
                   userWatchedDF: DataFrame,
                   userExposedDF: DataFrame):DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    val sc: SparkContext = ss.sparkContext
    import ss.implicits._
    //过滤有效视频
    val itemFactors=itemFactorsRaw.join(videoDataDF,itemFactorsRaw("item")===videoDataDF("sid")).select(videoDataDF("sid"),itemFactorsRaw("features"))
    BizUtils.getDataFrameInfo(itemFactors,"itemFactors")

    //加权
    val itemFactorsRDD = itemFactors.rdd.
      map(x => (x.getString(0),x.getSeq[Double](1).toArray)).
      map(x => {
        val sid = x._1
        val weight = weighedVideos.getOrElse(sid, 1.0)

        (sid, x._2.map(y => (weight * y).toFloat))
      })

    //将item特征向量作为广播变量
    val bcItemVectors = sc.broadcast(itemFactorsRDD.collect())

    val userFactorsRdd= userFactorsRaw.rdd.map(x=>(x.getLong(0),x.getSeq[Double](1).toArray.map(e => e.toFloat)))

    val userWatchedRdd = userWatchedDF.rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    val userExposedRdd=userExposedDF.rdd
      .map(r => (r.getLong(0), r.getString(1)))
      .groupByKey()
      .map(e => (e._1, e._2.toArray))

    val recommends=userFactorsRdd.leftOuterJoin(userWatchedRdd)
      .leftOuterJoin(userExposedRdd).repartition(500)
      .mapPartitions(partition=>{
        lazy val order = new OrderByScore
        val result = partition.map(x=>{
          val uid = x._1

          val userWatched = x._2._1._2 match {
            case Some(y) => y.toSet
            case _ => Set.empty[String]
          }

          val exposures2user = x._2._2 match {
            case Some(y) => y.toSet
            case _ => Set.empty[String]
          }

          val available = bcItemVectors.value.filter(y => {
            val sid = y._1
            !userWatched.contains(sid) && !exposures2user.contains(sid)
          })

          val likeV = available.
            map(y=> (y._1,VectorFunctions.denseProduct(y._2,x._2._1._1)))
          (uid,TopN.apply(topN,likeV)(order).toArray)
        })

        result.map(e => (e._1, e._2.take(topN)))
      })

    recommends.flatMap(e => e._2.map(x => (e._1, x._1, x._2.toDouble)))
      .toDF("uid", "sid", "score")
  }

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
    val movieRiskPath: MysqlPath = new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid","risk_flag"), "contentType = 'movie' and videoType = 0 and type = 1 and status = 1 ")
    val movieRisk = DataReader.read(movieRiskPath).map(e=>(e.getString(0),e.getInt(1))).toDF("sid","videoRisk")
    BizUtils.getDataFrameInfo(movieRisk, "movieRisk")
    val finalRecommend=recommend.join(movieRisk, recommend("sid") === movieRisk("sid"), "left")
      .join(userRisk, recommend("uid") === userRisk("uid"), "left")
      .where(expr("if(userRisk is null,0,userRisk)+if(videoRisk is null,0,videoRisk) <= 2"))
      .select(recommend("*"))

    finalRecommend
  }

  override def execute(args: Array[String]): Unit = {
    try {
      //----读取HDFS数据-----
      //1.获取用户看过的电影节目
      val numDaysOfWatched = 60
      val startDate = DateUtils.farthestDayWithDelimiter(numDaysOfWatched)
      val endDate = DateUtils.todayWithDelimiter
      val userWatchedPath: HdfsPath = new HdfsPath(PathConstants.pathOfMoretvMovieHistory,
        s"select userid, sid_or_subject_code from tmp where latest_optime >= '$startDate' and latest_optime <= '$endDate'")
      val userWatched = DataReader.read(userWatchedPath).withColumnRenamed("userid","uid")
        .withColumnRenamed("sid_or_subject_code","sid")
      BizUtils.getDataFrameInfo(userWatched,"userWatched")

      //2.获取首页曝光给用户的长视频
      val frontPageExposedLongVideos = BizUtils.getContentImpression(22, DateUtils.farthestDayWithOutDelimiter(15))
      BizUtils.getDataFrameInfo(frontPageExposedLongVideos,"frontPageExposedLongVideos")

      //3.读取moretv电影有效节目
      val movieSidDF= DataReader.read(BizUtils.getMysqlPath("movie_valid_sid"))
      BizUtils.getDataFrameInfo(movieSidDF,"movieSidDF")

      //4.获取“编辑精选”标签的电影
      val weightVideos = BizUtils.sidFromEditor(PathConstants.weightOfSelectMovies)
      println("weightVideos.size:"+weightVideos.size)

      //load ALSmodel
      val userFactorsRaw = DataReader.read(new HdfsPath("/ai/tmp/model/test/medusa/ALS/halfmonthago/matrixU"))
      val itemFactorRaw = DataReader.read(new HdfsPath("/ai/tmp/model/test/medusa/ALS/halfmonthago/matrixV"))
      BizUtils.getDataFrameInfo(userFactorsRaw,"userFactorsRaw")
      BizUtils.getDataFrameInfo(itemFactorRaw,"itemFactorRaw")

      //首页今日推荐的基础数据
      val ALSMovieRes = ALSRecommend(userFactorsRaw, itemFactorRaw, 300,
        movieSidDF, weightVideos, userWatched, frontPageExposedLongVideos)
      val ALSMovieWithRiskFilter = filterRiskFlag(ALSMovieRes)
      BizUtils.outputWrite(ALSMovieWithRiskFilter,"ALS/rec4moviehalfmonthago")

    } catch {
      case e: Exception => throw e
    }
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}