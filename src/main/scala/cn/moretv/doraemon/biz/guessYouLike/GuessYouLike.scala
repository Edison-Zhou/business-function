package cn.moretv.doraemon.biz.guessYouLike

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.{GuessYouLikeParam, Parameters}
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil, ReorderUtils}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.MysqlPath
import cn.moretv.doraemon.common.util.TransformUtils
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author wang.baozhi
  * @since 2018/9/26 上午10:53
  *
  *        猜你喜欢业务 电影和电视剧共用此类。悦厅电影和电视剧共用此类
  */
object GuessYouLike extends BaseClass {
  override def execute(args: Array[String]): Unit = {
    try {
      //----解析传入参数----
      val guessYouLikeParam: GuessYouLikeParam = Parameters.getParams(args)
      //获取业务线,节目类型，活跃用户时间，是否ab测试，alg标识
      val contentType = guessYouLikeParam.contentType
      val numOfDays = guessYouLikeParam.numDaysPlay
      val kafkaTopic = ConfigUtil.get("couchbase.moretv.topic")
      val ifAbTest = guessYouLikeParam.ifAbTest
      val alg = guessYouLikeParam.alg
      val userWatchedDays = 300

      //----数据部分----
      //1.从评分矩阵中获取近N天的活跃用户
      //悦厅电影、悦厅电视剧也是从content为movie、tv的频道中获得评分数据，进而从评分数据中获得日活用户。
      //yueting_movie，movie的评分contentType都为movie。yueting_tv，tv的评分contentType都为tv。
      var scoreContentType=contentType
      if(contentType.equalsIgnoreCase("yueting_movie")){
        scoreContentType="movie"
      }else if(contentType.equalsIgnoreCase("yueting_tv")){
        scoreContentType="tv"
      }

      val activeUserInfo = BizUtils.getActiveUserFromScoreMatrix(numOfDays, 0, scoreContentType).rdd.map(r => (r.getLong(0), 1))
      //println("activeUserInfo.count():"+activeUserInfo.count())

      //2.有效影片的数据
      //对于悦厅电影、悦厅电视剧的有效影片需要copyright_code='sohu'条件限制
      val validSidPath = BizUtils.getMysqlPath(s"${contentType}_valid_sid")
      val metaItemInfo = DataReader.read(validSidPath).rdd.map(e => (e.getString(0), 1))
      //println("metaItemInfo.count():"+metaItemInfo.count())


      //3.从评分数据中获取用户已观看的视频数据
      val notRecommendData = BizUtils.getUserWatchedSidByContentType(scoreContentType, userWatchedDays)
      //println("notRecommendData.count():"+notRecommendData.count())

      //4.用户ALS,Sid ALS
      val alsModel = new AlsModel()
      alsModel.load()
      val userFactorsRaw = alsModel.matrixU
      val itemFactorRaw = alsModel.matrixV
      BizUtils.getDataFrameInfo(userFactorsRaw,"userFactorsRaw")
      BizUtils.getDataFrameInfo(itemFactorRaw,"itemFactorRaw")


      //5.进行ab测试,获取可用的用户隐式矩阵
      val userFullUserData = ifAbTest match {
        case false => userFactorsRaw.
          rdd.map(r => (r.getLong(0), r.getSeq[Double](1).toArray)).
          join(activeUserInfo).
          map(r => (r._1, r._2._1))

        case true => userFactorsRaw.
          rdd.map(r => (r.getLong(0), r.getSeq[Double](1).toArray)).
          join(activeUserInfo).
          map(r => (r._1, r._2._1, BizUtils.getAlg(r._1))).
          filter(_._3 == "other").
          map(r => (r._1, r._2))
      }

      //6.获取可用的物品隐式矩阵，并广播变量
      val userFullItemData = itemFactorRaw.rdd.map(r => (r.getString(0), r.getSeq[Double](1).toArray)).
        join(metaItemInfo).map(r => (r._1, r._2._1))
      val bcItems = sc.broadcast(userFullItemData.collect)
      //println("userFullItemData.count():"+userFullItemData.count())

      //----计算部分----
      //1.计算活跃用户的topN节目数据
      val recommendData = {
        val filterTopN = if (guessYouLikeParam.filterTopN > guessYouLikeParam.numRecommendations)
          guessYouLikeParam.filterTopN
        else
          guessYouLikeParam.numRecommendations

        userFullUserData.join(notRecommendData, 1000).
          mapPartitions(partition => {
            lazy val order = new OrderByValue
            val result = partition.map(x => {
              val uid = x._1
              val notRec = x._2._2
              val available = bcItems.value.filter(y => !notRec.contains(y._1))
              val likeV = available.
                map(y => (y._1, VectorFunctions.denseProduct(y._2, x._2._1))) //计算推荐视频的评分
              (uid, TopN.apply(filterTopN, likeV)(order).toArray)
            })
            result
          })
      }

      //println("recommendData.count():"+recommendData.count())

      //----重排序部分----
      val reOrderData = ReorderUtils.randomReorder(recommendData, guessYouLikeParam.numRecommendations)
      //println("reOrderData.count():"+reOrderData.count())

      //----数据写入部分----
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
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
