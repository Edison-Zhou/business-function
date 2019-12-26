package cn.moretv.doraemon.biz.cluster

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/10/30.
  */
object UserFeaturesExtractByLongVideoCluster extends BaseClass{
  /**
    * 对三元数组进行正则化
    * @param data Array[uid, score1, watchRatio]
    * @param quantile 分位数（0到1之间）
    * @return Array[uid, regularScore1, watchRatio]
    */
  def regularTuple3Array(data: Array[(Long, Double, Double)], quantile: Double): Array[(Long, Double, Double)] = {
    val length = data.length
    if(length >= 2) { //数组长度小于2时，不需要正则化
    val quantileLength = math.min(math.max(2, length - (quantile * length).toInt + 1), length - 1)
      val quantileValue = data.sortBy(e => e._2).takeRight(quantileLength).head._2

      data.map(e => {
        val uid = e._1
        val score = e._2
        if(score < quantileValue) {
          (uid, score / quantileValue, e._3)
        }else  {
          (uid, 1.0, e._3)
        }
      })
    }else {
      data
    }
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    /**
      * 长视频聚类信息
      * longVideoClusterData: RDD[(cluster_index, Array[sid])]
      * cluster2longVideoMap: Map[cluster_index, Array[sid] ]
      * longVideo2clusterMap: Map[sid, Array[cluster_index] ]
      */
    val longVideoClusterData = BizUtils.getDataFrameNewest("/ai/data/medusa/videoFeatures/longVideoClusters")
      .rdd.map(r => (r.getInt(0), r.getSeq[String](1).toArray)).persist(StorageLevel.MEMORY_AND_DISK)

    val cluster2longVideoMap = longVideoClusterData.collectAsMap()

    val longVideo2clusterMap = longVideoClusterData.flatMap(x => x._2.map(y => (y, x._1))).
      groupByKey().map(e => (e._1, e._2.toArray)).collectAsMap()

    val longVideoInCluster = longVideo2clusterMap.toArray.map(e => e._1)

    val featureDimension = cluster2longVideoMap.size

    var longVideoSumNum = 0

    longVideo2clusterMap.foreach(e => longVideoSumNum += e._2.size)

    println("cluster2longVideoMap")
    println(cluster2longVideoMap.size)

    println("longVideo2clusterMap")
    println(longVideo2clusterMap.size)

    println("averageClustersPerlongVideo")
    println(longVideoSumNum.toDouble / featureDimension)

    /**
      * 用户观看历史&评分
      */
    val userWatchedLongVideosWithScore = BizUtils
      .readUserScore(PathConstants.pathOfMoretvLongVideoScore, 400)
      .rdd.map(r => (r.getLong(0), (r.getString(1), r.getDouble(2))))
      .groupByKey()
      .map(e => (e._1, e._2.toArray.filter(x => x._2 > 0.5)))
      .filter(e => e._2.length > 0)
      .map(e => (e._1, e._2.filter(x => longVideoInCluster.contains(x._1))))
      .filter(e => e._2.length > 0)

    /**
      * 用户行为特征提取 (uid, Array[clusterIndex, maxWatchScore, averageWatchRatio])
      */
    val userFeaturesDF = userWatchedLongVideosWithScore.map(e => {
      val uid = e._1

      val features = e._2.map(x => (longVideo2clusterMap.getOrElse(x._1, Array.emptyIntArray), x._2))
        .flatMap(x => x._1.map(y => (y, x._2)))
        .groupBy(x => x._1)
        .map(x => {
          val clusterIndex = x._1
          val clusterSize = cluster2longVideoMap.getOrElse(x._1, Array.emptyIntArray).size
          val averageWatchScore = x._2.map(y => y._2).max
          var averageWatchRatio = 0.0
          if(clusterSize > 0)
            averageWatchRatio = x._2.length.toDouble / clusterSize

          (clusterIndex, averageWatchScore, averageWatchRatio)
        }).toArray

      (uid, features)
    }).flatMap(e => e._2.map(x => (x._1, (e._1, x._2, x._3))))
      .groupByKey()
      .map(e => (e._1, regularTuple3Array(e._2.toArray, 0.99)))
      .map(e => (e._1, e._2.sortBy(_._2)))
      .flatMap(e => e._2.map(x => (x._1, e._1, x._2, x._3)))
      .toDF("uid", "clusterIndex", "maxWatchScore", "averageWatchRatio")

    /**
      * 结果写入HDFS
      */
    val dataWriter = new DataWriter2Hdfs
    dataWriter.write(userFeaturesDF, new HdfsPath("/ai/data/medusa/base/word2vec/user/longVideoClusterFeature"))
  }

}
