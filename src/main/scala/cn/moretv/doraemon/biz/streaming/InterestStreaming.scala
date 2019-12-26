package cn.moretv.doraemon.biz.streaming

import cn.moretv.doraemon.biz.util.BizUtils
import cn.whaley.sdk.utils.TransformUDF._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Protocol}
import cn.moretv.doraemon.data.reader.DataReader
import cn.moretv.doraemon.common.util.ArrayUtils
import cn.moretv.doraemon.biz.util.ConfigUtil
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/**
  * Created by cheng_huan on 2018/9/10.
  */
object InterestStreaming {

  def main(args: Array[String]): Unit = {
    // 初始化
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: XXX <sourcebrokers> <groupId> <sourcetopics> <duration>
        """.stripMargin)
      System.exit(1)
    }

    val types = Array("movie", "tv", "zongyi", "comic", "kids", "jilu")
    types.foreach(println)
    args.foreach(println)
    val Array(sourceBrokers, groupId, sourceTopics, duration) = args.takeRight(4)
    val topicsArray = sourceTopics.split(",").toSet

    val kafkaParams = getKafkaParams(sourceBrokers, groupId)
    val streamingContext = new StreamingContext(new SparkConf(), Seconds(duration.toInt))

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsArray, kafkaParams)
    )
    val ss = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val dataReader = new DataReader()
    val useeProgramDF = dataReader.read(BizUtils.getUsee4SearchMysqlPath("pre"))
      .select("sid","program_type").persist(StorageLevel.MEMORY_AND_DISK)
    BizUtils.getDataFrameInfo(useeProgramDF, "useeProgramDF")
    for (i <- 0 to 10) println(useeProgramDF.rdd.map(r => r.getString(0)).collect()(i))

    val useeMovieArr = useeProgramDF.filter("program_type = 'movie'").map(r => r.getString(0)).collect()
    val useeTVArr = useeProgramDF.filter("program_type = 'tv'").map(r => r.getString(0)).collect()
    val useeZongyiArr = useeProgramDF.filter("program_type = 'zongyi'").map(r => r.getString(0)).collect()
    val useeComicArr = useeProgramDF.filter("program_type = 'comic'").map(r => r.getString(0)).collect()
    val useeKidsArr = useeProgramDF.filter("program_type = 'kids'").map(r => r.getString(0)).collect()
    var useeJiluArr = useeProgramDF.filter("program_type = 'jilu'").map(r => r.getString(0)).collect()
    if (useeJiluArr.length == 0) useeJiluArr = useeProgramDF.map(r => r.getString(0)).collect()
    val typeProgramArrMap:Map[String, Array[String]] = Map("movie"->useeMovieArr, "tv"->useeTVArr,
      "zongyi"->useeZongyiArr, "comic"->useeComicArr, "kids"->useeKidsArr, "jilu"-> useeJiluArr)

    // 流式特征处理逻辑
    val streamData = stream.map(record => (record.key, new JSONObject(record.value))). // bi流式日志处理
      filter(
      e => {
        if (e._2.has("base_info")) {
          val baseInfo = e._2.getJSONObject("base_info")
          if (baseInfo.has("user_id")) {
            e._2.put("userId", baseInfo.get("user_id"))
          }
          if (baseInfo.has("account_id")) {
            e._2.put("accountId", baseInfo.get("account_id"))
          }
          if(e._2.has("video_sid")) {
            val videoSid = e._2.get("video_sid")
            e._2.put("videoSid", videoSid)
          }
          if(e._2.has("log_type")) {
            val logType = e._2.get("log_type")
            e._2.put("logType", logType)
          }
          if(e._2.has("content_type")) {
            val contentType = e._2.get("content_type")
            e._2.put("contentType", contentType)
            e._2.put("version", "4.x")
          }
        }else {
          e._2.put("version", "3.x")
        }
        e._2.has("logType") &&
          e._2.has("contentType") &&
          e._2.has("userId") &&
          e._2.has("accountId") &&
          e._2.has("videoSid")
      }
    ).
      filter(x => {
        // 进行类型过滤
        types.contains(x._2.get("contentType").toString)
      }).
      mapPartitions(par => {
        val host = "10.10.61.240"
        val port = 6344
        val db = 11

        val config: JedisPoolConfig = new JedisPoolConfig()
        val metadataPool: JedisPool = new JedisPool(config, host, port, 100 * Protocol.DEFAULT_TIMEOUT, null, db)
        val redis: Jedis = metadataPool.getResource
        val result = par.map(x => {
          // 提取特征
          val userId = x._2.get("userId").toString
          val accountId = x._2.get("accountId").toString
          val sid = x._2.get("videoSid").toString
          val contentType = x._2.get("contentType").toString

          val uid = transformUserId(userId, accountId)
          val moretvSimilarArr = redis.zrevrange(sid, 0, -1).asScala.toArray.reverse

          val useeProgramRecArr = typeProgramArrMap.getOrElse(contentType, new Array[String](0)).take(moretvSimilarArr.length/3)

          val result = ArrayUtils.arrayAlternate(moretvSimilarArr, useeProgramRecArr, 4, 1)

          val recommend = if (result.length > 1) {
            result.take(math.min(result.length - 1, 60))
          } else {
            Array.empty[String]
          }
          // val recommend = getSimilarVideosFromRedis(sid)
          val version = x._2.get("version")
          if(version.equals("4.x") && recommend.size > 10) {
            println(s"version=$version")
            println(s"sid = $sid")
            println(s"uid = $uid")
            println(s"recommend = ${recommend.mkString(",")}")
            println()
          }

          (uid, recommend)
        })
        metadataPool.destroy()
        result
      }).filter(e => e._2.length > 10)



    // 插入kafka
    streamData.foreachRDD(rdd => {
      val DF = rdd.flatMap(e => e._2.map(x => (e._1, x))).toDF("uid", "sid").distinct()
      //BizUtils.getDataFrameInfo(DF, "DF")
      BizUtils.recommend2Kafka4Couchbase("p:i:", DF, "streaming", "couchbase-medusa")
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  //连接kafka
  def getKafkaParams(sourceBrokers: String, groupId: String) = {
    Map[String, Object](
      "bootstrap.servers" -> sourceBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor"
    )
  }
}
