package cn.moretv.doraemon.biz.tabReorder

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.algorithm.kMeans.KMModel
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.params.KMeansParams
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil, UrlUtils, VectorUtils}
import cn.moretv.doraemon.common.enum.ProductLineEnum
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * * 计算站点树与kMeans各组的相似度,基于ALS的隐式矩阵
  * 输入：站点树接口url获取节目列表 和 kMeans的中心向量
  * 参数：
  *
  * videoType:      视频类型
  * kafkaTopic:     kafka写入的topic
  * alg:            如果进行ab测试时的，alg标识
  * offset:         如果进行ab测试时的，分组偏移量（默认0）
  * code:           为当前站点树编码，犀利动作的code为常量 1_movie_tag_dongzuo,
  * codeType：      当前站点树的类别，为常量（0，1，3表示站点树， 4表示编排）
  * videoTypePrefix:类型前缀
  *
  * 输出：分组号对应的节目重排序列表
  *
  * @author wang.baozhi 
  * @since 2018/9/30 上午10:34
  *
  */
object SohuTreeSiteReorder extends BaseClass {
  def execute(args: Array[String]): Unit = {
    //----读取配置参数----
    val params: KMeansParams = KMeansParams.getParams(args)
    val videoType: String = params.videoType
    val offset: Int = params.offset
    val alg: String = params.alg
    val code: String = params.code
    val codeType: Int = params.codeType
    val videoTypePrefix: String = params.videoTypePrefix

    println("videoType:" + videoType)
    println("offset:" + offset)
    println("videoTypePrefix:" + videoTypePrefix)
    println("alg:" + alg)
    println("codeType:" + codeType)
    println("code:" + code)

    //----数据部分----
    //1.获得用户ALS数据
    val alsModel = new AlsModel()
    alsModel.load()
    val itemFactorRaw = alsModel.matrixV

    //2.获得kMeans模型
    val kmModel = new KMModel()
    kmModel.load(videoType)
    val kMeansModel = kmModel.kMeansModel
    val centers = kMeansModel.clusterCenters

    //----计算部分----
    // 计算高中低 三种地区的用户数据
    for (desc <- Array(0, 1, 2)) {
      // 根据站点树的类型，从不同的url获取数据
      val url = codeType match {
        case 4 => s""
        case _ => s"http://vod.tvmore.com.cn/v/queryPrograms/detail?contentType=${videoType}&code=${code}&type=${codeType}&pageSize=20&desc=526${desc}884${desc}25&appVersion=4.0.2&pageIndex="
      }

      // 从url获得(节目列表， 节目信息列表)
      val videoList = UrlUtils.getDataFromURL(url)._1
      println("desc：" + desc)
      println("videoList.length：" + videoList.length)

      // 频道节目隐式矩阵的向量(sid, vector)
      val channelVector = itemFactorRaw.
        rdd.map(r => (r.getString(0), r.getSeq[Double](1))).
        filter(r => videoList.contains(r._1)).
        map(r => (r._1, VectorUtils.denseVector2Sparse(r._2))).
        collectAsMap().toMap

      // 读取用户组对应的标签向量
      val groupIdVector = centers.map(r => (kMeansModel.predict(r), r)).
        map(r => {
          val groupId = r._1 + offset
          val groupIdVector = r._2.toSparse
          val sequence = new ArrayBuffer[(String, Double)]()
          videoList.foreach(sid => {
            val sidVector = channelVector.getOrElse(sid, null)
            if (sidVector == null)
              sequence += ((sid, 0))
            else
              sequence += ((sid, VectorUtils.cosineSimilarity(sidVector, groupIdVector)))
          })
          val id = sequence.toArray.sortBy(-_._2).map(_._1)
          val key = videoType + "_" + code + "_" + codeType + "_" + desc + "_" + groupId
          (key, id)
        })
      println("groupIdVector.length:" + groupIdVector.length)

      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
      val result = ss.sparkContext.parallelize(groupIdVector).toDF("key", "id")
      BizUtils.getDataFrameInfo(result, "result")

      //----数据写入部分----
      val servicePrefix = "o:" + videoTypePrefix + "t:"
      println("ConfigUtil.get(\"couchbase.moretv.topic\"):"+ConfigUtil.get("couchbase.moretv.topic"))
      BizUtils.recommend2KafkaTabReorder(servicePrefix, result, ConfigUtil.get("couchbase.moretv.topic"),alg)
    }
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
