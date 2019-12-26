package cn.moretv.doraemon.biz.tabReorder

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.algorithm.kMeans.KMModel
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.params.KMeansParams
import cn.moretv.doraemon.biz.util.{ConfigUtil, BizUtils, VectorUtils}
import cn.moretv.doraemon.common.enum.ProductLineEnum
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json.JSONObject

/**
  * 基于kmeans模型的用户分组
  *
  * 类输入参数：
  * businessLine:   业务线（helios or medusa）
  * videoType:      视频类型
  * numOfDays:      用户play日志所需的天数
  * score:          用户观看评分选择
  * kafkaTopic:     kafka topic名
  *
  * 输出：各向量经过kmeans模型训练后的分组号groupId
  *
  * 数据（uid，groupId）传入kafka，同步到couchbase
  *
  * @author wang.baozhi
  * @since 1.0.0
  */
object User2GroupByALS extends BaseClass {

  // 进行ab测试时，获取用户的分组算法标识
  def getAlg(url: String, uid: Long) = {
    val json = new JSONObject(scala.io.Source.fromURL(url.replace("uid", uid.toString)).mkString)
    val alg = json.getJSONObject("abTest").getJSONObject("sitTree_queryPrograms").getString("alg")
    alg
  }

  def execute(args: Array[String]): Unit = {
    //----读取配置参数----
    val params: KMeansParams = KMeansParams.getParams(args)
    val videoType: String = params.videoType
    val numOfDays: Int = params.numOfDays
    val score: Double = params.score
    val abTest: Boolean = params.ifABTest
    val alg: String = params.alg
    val userGroupPrefix: String = params.userGroup
    val offSet: Int = params.offset

    println("params.toString:"+params.toString)
    println("videoType:"+videoType)
    println("numOfDays:"+numOfDays)
    println("score:"+score)
    println("abTest:"+abTest)
    println("alg:"+alg)
    println("userGroupPrefix:"+userGroupPrefix)
    println("offSet:"+offSet)


    //----数据部分----
    //1.从评分矩阵中获取近N天的活跃用户
    val activeUserInfo = BizUtils.getActiveUserFromScoreMatrix(numOfDays, score, videoType).rdd.map(r => (r.getLong(0), 1))
    //2.用户ALS数据
    val alsModel = new AlsModel()
    alsModel.load()
    val userFactorsRaw = alsModel.matrixU
    //3.获得kMeans模型
    val kmModel = new KMModel()
    kmModel.load(videoType)
    val kMeansModel = kmModel.kMeansModel
    //4.进行ab测试,获取可用的用户隐式矩阵
    val abGroup = abTest match {
      case false => userFactorsRaw.
        rdd.map(r => (r.getLong(0), r.getSeq[Double](1)))
      case true => userFactorsRaw.
        rdd.map(r => (r.getLong(0), r.getSeq[Double](1))).
        filter(r => getAlg(PathConstants.MEDUSA_ABTEST_URL, r._1) == alg)
    }
    //5.对用户进行预测分组，并生成json格式
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val userGroup = abGroup.join(activeUserInfo).
      map(r => (r._1, VectorUtils.denseVector2Sparse(r._2._1))).
      map(r => {
        val uid = r._1
        val groupId = kMeansModel.predict(r._2) + offSet
        (uid, groupId.toString)
      }).toDF("uid", "groupId")
    BizUtils.getDataFrameInfo(userGroup,"userGroup")

    //----数据写入部分----
    println("ConfigUtil.get(\"couchbase.moretv.topic\"):"+ConfigUtil.get("couchbase.moretv.topic"))
    BizUtils.recommend2KafkaTabReorder("o:" + userGroupPrefix+":", userGroup, ConfigUtil.get("couchbase.moretv.topic"))
  }
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
