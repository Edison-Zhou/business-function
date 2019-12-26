package cn.moretv.doraemon.biz.defaultRecommend

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HdfsPath, RedisPath}
import cn.moretv.doraemon.common.util.ArrayUtils
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/9/20.
  */
object DefaultRecommend extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    /**
      * 数据源
      */
    //数据1、编辑精选电影
    val selectMovies = DataReader.read(BizUtils.getMysqlPath("movie_editor_recommend"))
    BizUtils.getDataFrameInfo(selectMovies, "selectMovies")
    val editorSelectMovies = selectMovies.rdd.map(x => x.getString(0)).collect()

    //数据2、vip节目
    val vipSidArray = BizUtils.getVipSid(0).rdd.map(e => e.getString(0)).collect()

    //数据3、最热榜
    val hotMovie = BizUtils.getHotRankingList("movie", 15, 200)
    val hotTv = BizUtils.getHotRankingList("tv", 15, 200)
    val hotLongVideo = BizUtils.getHotRankingList("all", 15, 200)

    //数据4、聚类结果
    val dataWriter = new DataWriter2Kafka

    //数据5.TOP-N vip sid
    val hotVip = BizUtils.getHotRankingVipList("all", 15, 200)

    /**
      * 个性化推荐
      */
    /*4.0大首页默认推荐
    方案：编辑精选电影随机*/
    val portalDefault = ArrayUtils.takeThenRandom(editorSelectMovies, 50)
    val vipDefault = ArrayUtils.takeThenRandom(vipSidArray, 50)
    val hotSidArray = BizUtils.getHotSubjects.filter("copyright = 1").rdd.map(r => r.getString(0)).collect()
    val hotDefault = ArrayUtils.takeThenRandom(hotSidArray, 50)

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val portalDefaultDF = portalDefault.toList.map(e => ("default", e)).toDF("uid", "sid")
    val vipDefaultDF = vipDefault.toList.map(e => ("default", e)).toDF("uid", "sid")
    val hotDefaultDF = hotDefault.toList.map(e => ("default", e)).toDF("uid", "sid")

    BizUtils.getDataFrameInfo(portalDefaultDF, "portalDefaultDF")
    BizUtils.getDataFrameInfo(vipDefaultDF, "vipDefaultDF")
    BizUtils.getDataFrameInfo(hotDefaultDF, "hotDefaultDF")

    val frontPageUnionPack = BizUtils.frontPageUnionPack(portalDefaultDF, "editorSelect", vipDefaultDF, "randomVip", hotDefaultDF, "random")
    dataWriter.write(frontPageUnionPack, new CouchbasePath(ConfigUtil.get("couchbase.moretv.topic"), 1728000))

    /*VIP推荐*/
    BizUtils.defaultRecommend2Kafka4Couchbase("p:v:", vipDefault, "randomVip", ConfigUtil.get("couchbase.moretv.topic"), 60)

    /*首页今日推荐*/
    BizUtils.defaultRecommend2Kafka4Couchbase("p:p:", portalDefault, "editorSelect", ConfigUtil.get("couchbase.moretv.topic"), 60)

    /*兴趣推荐
    方案：长视频最热榜*/
    val interestDefault = ArrayUtils.takeThenRandom(hotLongVideo, 60)
    BizUtils.defaultRecommend2Kafka4Couchbase("p:i:", interestDefault, "hotRanking", ConfigUtil.get("couchbase.moretv.topic"), 60)

    /*猜你喜欢
    方案：最热榜*/
    val guessYouLikeMovieDefault = ArrayUtils.takeThenRandom(hotMovie, 100)
    val guessYouLikeTvDefault = ArrayUtils.takeThenRandom(hotTv, 100)

    //猜你喜欢电影
    BizUtils.defaultRecommend2Kafka4Couchbase("c:m:", guessYouLikeMovieDefault, "hotRanking", ConfigUtil.get("couchbase.moretv.topic"), 100)
    //猜你喜欢电视剧
    BizUtils.defaultRecommend2Kafka4Couchbase("c:t:", guessYouLikeTvDefault, "hotRanking", ConfigUtil.get("couchbase.moretv.topic"), 100)
    //猜你喜欢vip
    guessYouLikeVip(hotVip)
    /**
      * 类型相关
      */
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      /**
        * 最热榜数据
        */
      val hotRankingList = BizUtils.getHotRankingList(contentType, 30, 200)

      /**
        * 主题推荐
        */
      val clusterDF = DataReader.read(new HdfsPath("/ai/data/medusa/videoFeatures/" + contentType + "Clusters/Latest"))
      val videoCluster = clusterDF.rdd.map(r => (r.getInt(0), r.getSeq[String](1)))
        .flatMap(e => e._2.map(x => (x, e._1)))
      val validVideos = BizUtils.getValidLongVideoSid()
        .rdd.map(r => (r.getString(0), 1))
      val validVideoCluster = videoCluster.join(validVideos)
        .map(e => (e._2._1, e._1))
        .groupByKey()
        .map(e => (e._1, e._2.toArray))
        .persist(StorageLevel.MEMORY_AND_DISK)

      val clusterIndexes = validVideoCluster.map(e => e._1).collect()
      val cluster2videoMap = validVideoCluster.collectAsMap()

      var themeArray = Array.empty[String]
      ArrayUtils.randomTake(clusterIndexes, 40).foreach(index => {
        themeArray = themeArray ++: ArrayUtils.randomTake(cluster2videoMap.getOrElse(index, Array.empty[String]), 1)
      })


      /**
        * 退出推荐
        */
      val exitDefaultDF = List(("default" + contentType, ArrayUtils.takeThenRandom(hotRankingList, 100)
        .map(e => (e, 1.0)).toList)).toDF("sid", "id")

      val exitPackParam = new DataPackParam
      exitPackParam.zsetAlg = null
      exitPackParam.format = FormatTypeEnum.ZSET

      BizUtils.getDataFrameInfo(exitDefaultDF, "exitDefaultDF")
      val exitDefaultPackedDF = DataPack.pack(exitDefaultDF, exitPackParam)

      /**
        * 写入redis
        */
      val dataWriter = new DataWriter2Kafka

      val topic = ConfigUtil.get("default.redis.topic")
      val host = ConfigUtil.get("default.redis.host")
      val port = ConfigUtil.getInt("default.redis.port")
      val dbIndex = ConfigUtil.getInt("default.redis.dbindex")
      val ttl = ConfigUtil.getInt("default.redis.ttl")

      val exitFormatType = FormatTypeEnum.ZSET
      val exitRedisPath = RedisPath(topic, host, port, dbIndex, ttl, exitFormatType)

      dataWriter.write(exitDefaultPackedDF, exitRedisPath)
    })
  }

  //猜你喜欢vip
  def guessYouLikeVip(recommendArray: Array[String]): Unit = {
    BizUtils.defaultRecommend2Kafka4Couchbase("c:v:", recommendArray, "hotRanking", ConfigUtil.get("couchbase.moretv.topic"), 100)
  }

}
