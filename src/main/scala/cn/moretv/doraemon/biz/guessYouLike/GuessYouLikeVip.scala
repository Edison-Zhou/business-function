package cn.moretv.doraemon.biz.guessYouLike

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.{GuessYouLikeParam, Parameters}
import cn.moretv.doraemon.biz.util.{ConfigUtil, BizUtils, ReorderUtils}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.MysqlPath
import cn.moretv.doraemon.common.util.TransformUtils
import cn.whaley.sdk.algorithm.{TopN, VectorFunctions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author wang.baozhi
  * @since 2018/12/5 下午15:36
  *
  *  vip频道分类中的猜你喜欢
  *
  */
object GuessYouLikeVip extends BaseClass {
  override def execute(args: Array[String]): Unit = {
    try {
      //----解析传入参数----
      val guessYouLikeParam: GuessYouLikeParam = Parameters.getParams(args)
      //获取业务线,节目类型，活跃用户时间, kafkaTopic
      val numOfDays = guessYouLikeParam.numDaysPlay
      val kafkaTopic = ConfigUtil.get("couchbase.moretv.topic")
      val alg = guessYouLikeParam.alg
      val userWatchedDays = 300

      //----数据部分----
      //1.从评分矩阵中获取近N天的活跃用户
      val activeUserInfo = BizUtils.getActiveUserFromScoreMatrixForAllContentType(numOfDays, 0).rdd.map(r => (r.getLong(0), 1))
      println("activeUserInfo.count():"+activeUserInfo.count())

      //2.获得有效的vip的影片
      val metaItemInfo =  BizUtils.getVipSidOnlyMTVIP(-1).rdd.map(e => (e.getString(0), 1))
      println("metaItemInfo.count():"+metaItemInfo.count())

      //3.从评分数据中获取用户已观看的视频数据
      val notRecommendData = BizUtils.getUserWatchedSidForAllContentType(userWatchedDays)
      println("notRecommendData.count():"+notRecommendData.count())

      //4.用户ALS,Sid ALS
      val alsModel = new AlsModel()
      alsModel.load()
      val userFactorsRaw = alsModel.matrixU
      val itemFactorRaw = alsModel.matrixV
      BizUtils.getDataFrameInfo(userFactorsRaw,"userFactorsRaw")
      BizUtils.getDataFrameInfo(itemFactorRaw,"itemFactorRaw")


      //5.获取可用的用户隐式矩阵
      val userFullUserData = userFactorsRaw.
        rdd.map(r => (r.getLong(0), r.getSeq[Double](1).toArray)).
        join(activeUserInfo).
        map(r => (r._1, r._2._1))

      //6.获取可用的物品隐式矩阵，并广播变量
      val userFullItemData = itemFactorRaw.rdd.map(r => (r.getString(0), r.getSeq[Double](1).toArray)).
        join(metaItemInfo).map(r => (r._1, r._2._1))
      val bcItems = sc.broadcast(userFullItemData.collect)
      println("userFullItemData.count():"+userFullItemData.count())

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

      println("recommendData.count():"+recommendData.count())

      //----重排序部分----
      val reOrderData = ReorderUtils.randomReorder(recommendData, guessYouLikeParam.numRecommendations)
      println("reOrderData.count():"+reOrderData.count())

      //----数据写入部分----
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
      val resultDate = reOrderData.flatMap(e => e._2.map(x => (e._1, x._1))).toDF("uid", "sid")
      BizUtils.getDataFrameInfo(resultDate,"resultDate")
      val businessPrefix="c:v:"
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
