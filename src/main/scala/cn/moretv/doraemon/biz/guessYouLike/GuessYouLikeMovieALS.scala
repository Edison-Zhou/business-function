package cn.moretv.doraemon.biz.guessYouLike

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.{GuessYouLikeParam, Parameters}
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil, ReorderUtils}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author Edison_Zhou
  * @since 2019/7/22
  *
  *        电影猜你喜欢业务A算法--ALS  测试阶段，当前适用于电视猫电影。
  */
object GuessYouLikeMovieALS extends BaseClass {
  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    try {
      //----解析传入参数----
      val guessYouLikeParam: GuessYouLikeParam = Parameters.getParams(args)
      //获取业务线,节目类型，活跃用户时间，是否ab测试，alg标识
      val contentType = "movie"
      val numOfDays = 90
      val kafkaTopic = ConfigUtil.get("couchbase.moretv.topic")
      val alg = "als"
      val userWatchedDays = 180

      //----数据部分----
      //1.从评分矩阵中获取近N天的活跃用户
      //悦厅电影也是从content为movie的频道中获得评分数据，进而从评分数据中获得日活用户。
      //yueting_movie，movie的评分contentType都为movie。
      var scoreContentType=contentType
      if(contentType.equalsIgnoreCase("yueting_movie")) scoreContentType="movie"

      val activeUserWithALS = BizUtils.readActiveUser().rdd
        .map(r => (r.getLong(0), BizUtils.getAlg(r.getLong(0)))).filter(_._2 == "als").map(r => (r._1, 1))
      println("猜你喜欢将走ALS算法的用户个数为：" + activeUserWithALS.collect().length)
//      val activeUserInfo = BizUtils.getActiveUserFromScoreMatrix(numOfDays, 0, scoreContentType).rdd.map(r => (r.getLong(0), 1))
      //println("activeUserInfo.count():"+activeUserInfo.count())

      //2.有效影片的数据
      //对于悦厅电影、悦厅电视剧的有效影片需要copyright_code='sohu'条件限制
      val validSidPath = BizUtils.getMysqlPath(s"${contentType}_valid_sid")
      val metaItemInfo = DataReader.read(validSidPath).rdd.map(e => (e.getString(0), 1))
      //println("metaItemInfo.count():"+metaItemInfo.count())


      //3.从评分数据中获取用户已观看的视频数据
//      val notRecommendData = BizUtils.getUserWatchedSidByContentType(scoreContentType, userWatchedDays)
      //println("notRecommendData.count():"+notRecommendData.count())

      //4.用户ALS,Sid ALS
      val userFactorsRaw = DataReader.read(new HdfsPath("/ai/tmp/model/pre/medusa/ALS/default/matrixU"))
      val itemFactorRaw = DataReader.read(new HdfsPath("/ai/tmp/model/pre/medusa/ALS/default/matrixV"))
      BizUtils.getDataFrameInfo(userFactorsRaw,"userFactorsRaw")
      BizUtils.getDataFrameInfo(itemFactorRaw,"itemFactorRaw")


      //5.获取可用的用户隐式矩阵
      val userFullUserData = userFactorsRaw.rdd.map(r => (r.getLong(0), r.getSeq[Double](1).toArray))
        .join(activeUserWithALS).map(r => (r._1, r._2._1))
      println("userFullUserData.count():"+userFullUserData.count())

      //6.获取可用的物品隐式矩阵，并广播变量
      val userFullItemData = itemFactorRaw.rdd.map(r => (r.getString(0), r.getSeq[Double](1).toArray))
        .join(metaItemInfo).map(r => (r._1, r._2._1))
      val bcItems = sc.broadcast(userFullItemData.collect)
      println("userFullItemData.count():"+userFullItemData.count())

      //----计算部分----
      //1.计算活跃用户的topN节目数据
      val recommendData = {
        val filterTopN = if (guessYouLikeParam.filterTopN > guessYouLikeParam.numRecommendations)
          guessYouLikeParam.filterTopN
        else
          guessYouLikeParam.numRecommendations

        userFullUserData.map(e => {
            lazy val order = new OrderByValue
              val uid = e._1
              val available = bcItems.value
              val likeV = available.
                map(y => (y._1, VectorFunctions.denseProduct(y._2, e._2))) //计算推荐视频的评分,item特征与user特征做点积
              (uid, TopN.apply(filterTopN, likeV)(order).toArray)
            })
          }

      println("recommendData.count():"+recommendData.count())

      //----重排序部分----
      val reOrderData = ReorderUtils.randomReorder(recommendData, guessYouLikeParam.numRecommendations)
      println("reOrderData.count():"+reOrderData.count())

      //----数据写入部分----

      val resultDate = reOrderData.flatMap(e => e._2.map(x => (e._1, x._1))).toDF("uid", "sid")
      BizUtils.getDataFrameInfo(resultDate,"resultDate")
      val businessPrefix = contentType match {
        case "movie" => "c:m:"
        case "tv" => "c:t:"
        case "yueting_movie" => "c:ytm:"
        case "yueting_tv" => "c:ytt:"
      }
      BizUtils.recommend2Kafka4Couchbase(businessPrefix, resultDate, alg, kafkaTopic)
    } catch {
      case e: Exception => throw e
    }
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
  class OrderByValue extends Ordering[(String, Double)] {
    override def compare(x: (String, Double), y: (String, Double)) = {
      if (x._2 > y._2) -1 else if (x._2 == y._2) 0 else 1
    }
  }

}
