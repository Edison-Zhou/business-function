package cn.moretv.doraemon.biz.peoplealsolike

import cn.moretv.doraemon.algorithm.matrix.fold.{MatrixFoldAlgorithm, MatrixFoldModel, MatrixFoldParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.RedisPath
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by guohao on 2018/10/8.
  * 3.2.3 后，该产品形态不存在
  */
object MovieReturnRecommend extends BaseClass{
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val topN:Integer = 30 //最终推荐的节目数
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS
    //获取电影高危视屏sid ，risk_flag == 1
    val riskSids = ReaderUtil.getMysqlDF().selectExpr("sid","risk_flag")
          .filter(row=>{
            val risk_flag = row.getAs[Integer]("risk_flag")
            risk_flag == 1
          })
      .map(f=>f.getAs[String]("sid"))
      .collect()

    //1.基于als的推荐
    //读取movie als recommend
    val alsRecommendResult = getReCommendResult(ss,"similarAls/movie",riskSids,topN).persist(StorageLevel.MEMORY_AND_DISK)
    //基于word2Vec
    val word2VecRecommendResult = getReCommendResult(ss,"similarWord2Vec/movie",riskSids,topN).persist(StorageLevel.MEMORY_AND_DISK)
    //基于tag
    val tagRecommendResult = getReCommendResult(ss,"similarTag/movie",riskSids,topN).persist(StorageLevel.MEMORY_AND_DISK)
    //聚合
    val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm
    val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
    param.recommendUserColumn = "sid"
    param.recommendItemColumn = "item"
    param.scoreColumn = "similarity"
    param.recommendNum = topN
    param.mixMode = MixModeEnum.STACKING
    param.outputOriginScore = false
    mixAlg.initInputData(
      Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> alsRecommendResult,
        mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> word2VecRecommendResult,
        mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> tagRecommendResult)
    )
    mixAlg.run()
    //输出到hdfs
    mixAlg.getOutputModel.output("peoplealsolike/movie")
    val mixResult = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult
    //数据格式转换
    val foldAlg = new MatrixFoldAlgorithm
    val foldParam = foldAlg.getParameters.asInstanceOf[MatrixFoldParameters]
    foldParam.idX = "sid"
    foldParam.idY = "item"
    foldParam.score = "similarity"

    foldAlg.initInputData(Map(foldAlg.INPUT_DATA_KEY -> mixResult))
    foldAlg.run()

    var foldResult = foldAlg.getOutputModel.asInstanceOf[MatrixFoldModel].matrixFold.toDF("sid", "items")

    //key转真实id，行数增多
    val sidRel = BizUtils.readVirtualSidRelation()
    foldResult = foldResult.as("a").join(sidRel.as("b"), expr("a.sid = b.virtual_sid"), "leftouter")
      .selectExpr("case when b.sid is not null then b.sid else a.sid end as sid", "items")

    //输出到redis
    val dataPackParam = new DataPackParam
    dataPackParam.format = FormatTypeEnum.ZSET
    dataPackParam.zsetAlg = "als"
    val outputDf = DataPack.pack(foldResult, dataPackParam)

    val dataWriter = new DataWriter2Kafka

    val topic = ConfigUtil.get("peoplealsolike.redis.topic")
    val host = ConfigUtil.get("peoplealsolike.redis.host")
    val port = ConfigUtil.getInt("peoplealsolike.redis.port")
    val dbIndex = ConfigUtil.getInt("peoplealsolike.redis.dbindex")
    val ttl = ConfigUtil.getInt("peoplealsolike.redis.ttl")
    val formatType = FormatTypeEnum.ZSET

    val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)
    dataWriter.write(outputDf, path)
  }


  /**
    * @param ss 全量的推荐结果
    * @param bizName 算法名
    * @param riskSids 高危节目sid
    * @return 过滤推荐结果
    */
  def getReCommendResult(ss:SparkSession,bizName:String,riskSids:Array[String],topN:Int): DataFrame ={
    val low_risk_Num:Int = 10
    val df = DataReader.read(BizUtils.getHdfsPathForRead(bizName))
    val fullRecommend = df.rdd.groupBy(row=>row.getString(0)).map(f=>{
      val sid = f._1
      val itemAndSimilarty = f._2.map(row=>{
        val item = row.getString(1)
        val similarity = row.getDouble(2)
        (item,similarity)
      })
      (sid,itemAndSimilarty)
    })

    val finalRecommend = fullRecommend.map(f=>{
      val firstTen =  f._2.take(topN-low_risk_Num).toArray
      val lastTenRiskSafe = f._2.toArray.diff(firstTen).filter(f=>{
        !riskSids.contains(f._1)
      }).take(low_risk_Num)
      val result = firstTen ++ lastTenRiskSafe
      (f._1,result)
    }).flatMap(x=>{
      x._2.map(y=>{
        (x._1,y._1,y._2)
      })
    })

    import ss.implicits._
    finalRecommend.toDF("sid","item","similarity")

  }



}
