package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.algorithm.matrix.fold.{MatrixFoldAlgorithm, MatrixFoldModel, MatrixFoldParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{EnvEnum, FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.RedisPath
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import cn.moretv.doraemon.reorder.mix.{MixModeEnum, RecommendMixAlgorithm, RecommendMixModel, RecommendMixParameter}
import org.apache.spark.sql.functions._

/**
  * 相似影片推荐结果融合
  * Created by lituo on 2018/7/20.
  */
object SimilarMix extends BaseClass {
  override def execute(args: Array[String]): Unit = {

    //读取
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val word2VecDf = DataReader.read(BizUtils.getHdfsPathForRead("similarWord2Vec/" + contentType))
      val tagDf = DataReader.read(BizUtils.getHdfsPathForRead("similarTag/" + contentType))
      val defaultDf = DataReader.read(BizUtils.getHdfsPathForRead("similarDefault/" + contentType))

      //算法
      val mixAlg: RecommendMixAlgorithm = new RecommendMixAlgorithm

      val param = mixAlg.getParameters.asInstanceOf[RecommendMixParameter]
      param.recommendUserColumn = "sid"
      param.recommendItemColumn = "item"
      param.scoreColumn = "similarity"
      param.recommendNum = 60
      param.mixMode = MixModeEnum.STACKING
      param.outputOriginScore = false

      mixAlg.initInputData(
        Map(mixAlg.INPUT_DATA_KEY_PREFIX + "1" -> word2VecDf,
          mixAlg.INPUT_DATA_KEY_PREFIX + "2" -> tagDf,
          mixAlg.INPUT_DATA_KEY_PREFIX + "3" -> defaultDf)
      )

      mixAlg.run()

      //输出到hdfs
      val mixResult = mixAlg.getOutputModel.asInstanceOf[RecommendMixModel].mixResult
      mixAlg.getOutputModel.output("similarMix/" + contentType)

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
      dataPackParam.zsetAlg = "mix"
      val outputDf = DataPack.pack(foldResult, dataPackParam)

      val dataWriter = new DataWriter2Kafka

      val topic = ConfigUtil.get("similar.redis.topic")
      val host = ConfigUtil.get("similar.redis.host")
      val port = ConfigUtil.getInt("similar.redis.port")
      val dbIndex = ConfigUtil.getInt("similar.redis.dbindex")
      val ttl = ConfigUtil.getInt("similar.redis.ttl")
      val formatType = FormatTypeEnum.ZSET

      val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)

      dataWriter.write(outputDf, path)

      //输出到内部redis，给兴趣推荐使用
      if (env == EnvEnum.PRO || env == EnvEnum.PRE) {
        val topic = ConfigUtil.get("similar.inner.redis.topic")
        val host = ConfigUtil.get("similar.inner.redis.host")
        val port = ConfigUtil.getInt("similar.inner.redis.port")
        val dbIndex = ConfigUtil.getInt("similar.inner.redis.dbindex")
        val ttl = ConfigUtil.getInt("similar.inner.redis.ttl")
        val formatType = FormatTypeEnum.ZSET
        val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)
        dataWriter.write(outputDf, path)
      }
    })

  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
