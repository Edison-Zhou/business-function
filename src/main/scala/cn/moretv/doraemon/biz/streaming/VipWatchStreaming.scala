package cn.moretv.doraemon.biz.streaming

import java.util
import java.util.concurrent.TimeUnit

import cn.moretv.doraemon.common.util.ArrayUtils
import cn.whaley.sdk.utils.TransformUDF._
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.{Bucket, CouchbaseCluster}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json.JSONObject
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig, Protocol}
import cn.moretv.doraemon.biz.util.BizUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by michael on 2019/2/14.
  *
  * 用于会员看看的实时推荐业务。
  * 文档地址：
  * http://doc.moretv.com.cn/pages/viewpage.action?pageId=26578940
  *
  * 根据实时播放日志（影片完整看完或用户主动退出但观影时长超过最小设定阀值）获得观看vip影片的sid，uid，accountId，
  * 然后根据sid获得此sid的相似影片，更新cc中的推荐结果。由于目前没有会员看看的曝光，获得vip相似影片后，随机打乱顺序。
  *
  * 过滤逻辑：过滤掉播放日志中的启播行为记录，过滤掉播放时长小于10分钟的记录，过滤掉404以下版本的多剧集记录。
  *
  *
  * 依赖 ：
  * 1.针对长视频vip影片，训练各个vip影片的相似vip影片，存储在redis db中。zset方式存储。
  * 目前不能直接从原相似影片中过滤vip类型影片生成，因为可能存在一个vip影片的相似影片中绝大部分都是非vip影片，导致推荐数目不足的情况。
  * 需要将基于tag、word2Vec、热门/编辑推荐 方式生成相似推荐影片的数据源全部使用vip影片代替，然后再次混合此三种推荐结果。
  *
  * 2.DataWriter改动，支持写入单string到Kafka中。【目前，因为涉及到cc的读，所以可以利用此cc的连接完成写操作。不通过kafka，直接操作cc。】
  *
  *
  * 其他：
  * 暂不考虑对couchbase的更新频率（根据couchbase中json结构体与当前时间对比，如果时间比较近，则忽略此次更新。）
  * 支持后续实时曝光模块插入
  *
  * 参考：
  * 个性化专题-实时推荐设计文档
  * http://doc.moretv.com.cn/pages/viewpage.action?pageId=24840159
  *
  * spark app-svr4. 脚本
  * /opt/ai/interestOptimization-201707/release/bin/InterestStreaming.sh
  *
  * Design Patterns for using foreachRDD
  *https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
  *
  * 404版本，增加parent_sid作为剧头sid
  * http://bigdata-app.whaley.cn/matrix/#/logShow/showInfo/693
  *
  * Managing Connections using the Java SDK with Couchbase Server
  * https://docs.couchbase.com/java-sdk/2.7/managing-connections.html
  *
  * TODO:
  * 1.使用ConnectionPool的方式
  * dstream.foreachRDD { rdd =>
  * rdd.foreachPartition { partitionOfRecords =>
  * // ConnectionPool is a static, lazily initialized pool of connections
  * val connection = ConnectionPool.getConnection()
  * partitionOfRecords.foreach(record => connection.send(record))
  * ConnectionPool.returnConnection(connection)  // return to the pool for future reuse
  * }
  * }
  *
  * 2.适用于实时流的baseclass，读取不同环境配置参数
  *
  *
  * 3.KafkaUtils 使用方式调研（目前kafka异常，导致cc连接数激增）
  * 部署方式：
  * bigdata-appsvr-130-4,spark用户
  * /opt/ai/streaming/release
  */
object VipWatchStreaming {
  //会员影片日志中字段值名称
  val vipFlag = "MTVIP"
  //观看最少时长（单位：秒）
  val durationLowerLimit=600

  def main(args: Array[String]): Unit = {

    //参数初始化
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: XXX <sourcebrokers> <groupId> <sourcetopics> <duration>
        """.stripMargin)
      System.exit(1)
    }

    //404后，有parent_sid后，放开多剧集频道
    val types = Array("movie", "tv", "zongyi", "comic", "kids", "jilu")
    //val types = Array("movie")
    types.foreach(println)
    args.foreach(println)
    println("-------version 17")
    val Array(sourceBrokers, groupId, sourceTopics, duration) = args.takeRight(4)
    val topicsArray = sourceTopics.split(",").toSet

    val kafkaParams = getKafkaParams(sourceBrokers, groupId)
    val streamingContext = new StreamingContext(new SparkConf(), Seconds(duration.toInt))
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsArray, kafkaParams)
    )

    val filteredUserID : Set[String] = BizUtils.getDataFrameNewest("/ai/tmp/output/test/medusa/gouzheng/userID").
      rdd.map(r => r.getString(0)).collect().toSet
    val bcItem = streamingContext.sparkContext.broadcast(filteredUserID)

    // 流式特征处理逻辑
    val dStream = stream.map(record => new JSONObject(record.value)).filter(jsonObj => {
      /**
        * 因为3x和4x的实时日志格式不一致，此次特殊处理
        * 参考 http://bigdata-app.whaley.cn/matrix/#/logShow/showInfo/693
        */

      if (jsonObj.has("base_info")) {
        val baseInfo = jsonObj.getJSONObject("base_info")
        if (baseInfo.has("user_id")) {
          jsonObj.put("userId", baseInfo.get("user_id"))
        }
        if (baseInfo.has("account_id")) {
          jsonObj.put("accountId", baseInfo.get("account_id"))
        }

        //404之前的版本，对于多剧集节目，日志里面的video_sid为剧集sid。404版本增加parent_sid记录多剧集的剧头sid
        if (jsonObj.has("video_sid") && jsonObj.has("content_type") && jsonObj.getString("content_type").equalsIgnoreCase("movie")) {
          val videoSid = jsonObj.get("video_sid")
          jsonObj.put("videoSid", videoSid)
        }
        if (jsonObj.has("parent_sid")) {
          val videoSid = jsonObj.get("parent_sid")
          jsonObj.put("videoSid", videoSid)
        }

        if (jsonObj.has("log_type")) {
          val logType = jsonObj.get("log_type")
          jsonObj.put("logType", logType)
        }
        if (jsonObj.has("content_type")) {
          val contentType = jsonObj.get("content_type")
          jsonObj.put("contentType", contentType)
          jsonObj.put("version", "4.x")
        }
        if (jsonObj.has("member_type")) {
          val member_type = jsonObj.getString("member_type")
          if (member_type.equalsIgnoreCase(vipFlag)) {
            jsonObj.put("isVip", 1)
          }
        }
      } else {
        jsonObj.put("version", "3.x")
        if (jsonObj.has("videoGoodsType")) {
          val videoGoodsType = jsonObj.getString("videoGoodsType")
          if (videoGoodsType.equalsIgnoreCase(vipFlag)) {
            jsonObj.put("isVip", 1)
          }
        }
      }

      var isOkForDuration = false
        if (jsonObj.has("event") && jsonObj.has("duration") && (!jsonObj.getString("event").equalsIgnoreCase("startplay"))) {
            var durationInt=0
            try {
              durationInt=Integer.parseInt(jsonObj.getString("duration"))
            }catch {
              case e:Exception=>println(e.toString)
            }
            if(durationInt > durationLowerLimit){
              isOkForDuration = true
            }
        }

      //过滤逻辑判断
      isOkForDuration &&
        jsonObj.has("isVip") &&
        jsonObj.has("logType") &&
        jsonObj.has("contentType") &&
        jsonObj.has("userId") &&
        jsonObj.has("accountId") &&
        jsonObj.has("videoSid") &&
        types.contains(jsonObj.get("contentType").toString)
    })

    //首页聚合接口业务前缀
    val businessPrefix = "p:a:"
    val moduleName = "vip"
    val recommendCount = 20
    val ttl = 172800

    //todo  如果用户有多个vip影片播放，以最后的vip影片为准.
    //Output Operations on DStreams
    //repartition(3) 用来防止打开过多cc connection,for now cc close is still a question
    dStream.repartition(3).foreachRDD { rdd =>
      rdd.foreachPartition {
        partitionOfRecords =>
          //捕获某个批次open bucket的异常
          var redis:Jedis=null
          var bucket:Bucket=null
          var couchbaseCluster:CouchbaseCluster=null
          try{
          //init connection
            redis = getRedis
           // bucket = getBucket
          couchbaseCluster=  getCouchbaseCluster
          bucket = couchbaseCluster.openBucket("medusa", 30, TimeUnit.SECONDS)
          partitionOfRecords.foreach(record => {
            // 提取特征
            val userId = record.get("userId").toString
            val accountId = record.get("accountId").toString
            val sid = record.get("videoSid").toString
            val uid = transformUserId(userId, accountId)

            if (!bcItem.value.contains(userId)) {
              //获得这个vip影片的相似影片
               val redisContent = redis.zrevrange(sid, 0, -1).toArray
              //val redisContent = redis.zrevrange("tvwycestkl23", 0, -1).toArray
              val recommend = if (redisContent.length > 1) {
                redisContent.takeRight(math.min(redisContent.length - 1, 60))
              } else {
                Array.empty[String]
              }

            /*  println("---raw record:" + record.toString())
              //log print
              val version = record.get("version")
              if (version.equals("3.x")) {
                println(s"version=$version")
                println(s"sid = $sid")
                println(s"uid = $uid")
                println(s"isVip = " + record.get("isVip"))
                // println(s"recommend = ${recommend.mkString(",")}")
                println()
              } else if (version.equals("4.x")) {
                println(s"version=$version")
                println(s"sid = $sid")
                println(s"uid = $uid")
                println(s"isVip = " + record.get("isVip"))
                // println(s"recommend = ${recommend.mkString(",")}")
                println()
              }*/

              //有可用于更新的数据
              //val couchbaseKey = "p:a:10000003"
              val couchbaseKey = businessPrefix + uid
              println("----couchbaseKey:"+couchbaseKey)
              val isExist = bucket.exists(couchbaseKey, 5, TimeUnit.SECONDS)
              if (recommend.length > 0 && isExist) {
                //获得cc中，用户的首页聚合接口的推荐结果
                val jsonDocument = bucket.get(couchbaseKey)
                val content = jsonDocument.content()
                println("----old content:"+content)

                //如果有vip模块json，则删除；没有的话，添加
                if (content.containsKey(moduleName)) {
                  content.removeKey(moduleName)
                }

                val jsonObj = JsonObject.create()
                jsonObj.put("alg", "similar")
                val jsonArray = JsonArray.create()
                val recommendResult = ArrayUtils.randomArray(recommend).take(Math.min(recommendCount, recommend.length))
                recommendResult.foreach(e => {
                  jsonArray.add(e)
                })
                jsonObj.put("id", jsonArray)
                content.put(moduleName, jsonObj)
                val newJsonDocument = JsonDocument.create(couchbaseKey, ttl, content)
                println("----new content:"+content)
                bucket.upsert(newJsonDocument)
              }
            }
          }
          )
          }catch {
            case e:Exception=>println(e.toString)
          }finally {
            //return to the pool for future reuse
            redis.close()
            couchbaseCluster.disconnect()
           // bucket.close()
          }
      }
    }

    //启动实时流
    streamingContext.start()
    streamingContext.awaitTermination()
  }


  //获得couchbase bucket资源
  def getBucket(): Bucket = {
 /*  val host= ConfigUtil.get("couchbase.host")
   val bucketName= ConfigUtil.get("couchbase.bucket")
   val username= ConfigUtil.get("couchbase.username")
   val pwd= ConfigUtil.get("couchbase.pwd")*/
  /*  val host= "couchbase://bigtest-cmpt-129-203"
    val bucketName= "usee"
    val username= "root"
    val pwd= "123456"*/

   /* val host= "couchbase://10.10.147.85"
    val bucketName= "medusa"
    val username= "liu.qiang"
    val pwd= "mlw321@moretv"
    println("--------couchbaseInfo:",host,bucketName,username,pwd)
    val cluster = CouchbaseCluster.create(host).authenticate(username,pwd)*/

    val nodes = util.Arrays.asList("10.10.147.85", "10.10.155.113","10.10.166.108","10.10.172.85")
    val bucketName= "medusa"
    val username= "liu.qiang"
    val pwd= "mlw321@moretv"
    println("--------couchbaseInfo:",nodes,bucketName,username,pwd)
    val cluster = CouchbaseCluster.create(nodes).authenticate(username,pwd)

    /*val cluster = CouchbaseCluster.create(DefaultCouchbaseEnvironment.builder().
      bootstrapCarrierEnabled(false).build(),util.Arrays.asList("10.10.147.85")).authenticate(username,pwd)
   */
    val bucket = cluster.openBucket(bucketName, 30, TimeUnit.SECONDS)
    bucket
  }

  //获得redis资源
  def getRedis(): Jedis = {
    /*val host = ConfigUtil.get("similarVip.redis.topic")
    val port = ConfigUtil.get("similarVip.redis.port").toInt
    val db = ConfigUtil.get("similarVip.redis.dbindex").toInt*/
    //val host = "bigtest-cmpt-129-204"
    val host = "10.19.60.71"
    val port = "6379".toInt
    val db = "5".toInt
    println("--------redisInfo:",host,port,db)
    val config: JedisPoolConfig = new JedisPoolConfig()
    val metadataPool: JedisPool = new JedisPool(config, host, port, 100 * Protocol.DEFAULT_TIMEOUT, null, db)
    val redis = metadataPool.getResource
    redis
  }

  def getCouchbaseCluster(): CouchbaseCluster = {
    val nodes = util.Arrays.asList("10.10.147.85", "10.10.155.113","10.10.166.108","10.10.172.85")
    val bucketName= "medusa"
    val username= "liu.qiang"
    val pwd= "mlw321@moretv"
    println("--------couchbaseInfo:",nodes,bucketName,username,pwd)
    val cluster = CouchbaseCluster.create(nodes).authenticate(username,pwd)
    cluster
  }

  //kafka相关参数配置
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
