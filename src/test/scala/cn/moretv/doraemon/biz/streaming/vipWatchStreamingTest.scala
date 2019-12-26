package cn.moretv.doraemon.biz.streaming

/**
  *
  * @author wang.baozhi 
  * @since 2019/2/18 下午5:19 
  */

import java.util.concurrent.TimeUnit

import cn.moretv.doraemon.common.util.ArrayUtils
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.{CouchbaseCluster}
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import org.junit.{Test}
import redis.clients.jedis.{Jedis, Protocol, JedisPool, JedisPoolConfig}

class vipWatchStreamingTest {
  @Test
  def testReadCouchbase(): Unit ={
   val cluster=CouchbaseCluster.create("bigtest-cmpt-129-204").authenticate("root","123456")
    val bucket=cluster.openBucket("medusa", 5, TimeUnit.SECONDS)
    val jsonContent=bucket.get("p:a:88")
    println(jsonContent.expiry())
    println(jsonContent.cas())
    val moduleName="vip"
    val content=jsonContent.content()
    if (content.containsKey(moduleName)){
/*
     val vipContent=content.getObject(moduleName)
*/
      content.removeKey(moduleName)
      val jsonObj=JsonObject.create()
      jsonObj.put("als","similar")
      val jsonArray=JsonArray.create()
      jsonArray.add("11Stvwy3fn8qrgh")
      jsonArray.add("22tvwy1c23rtmo")
      jsonObj.put("id",jsonArray)
      content.put(moduleName,jsonObj)
    }
    println(content.toString)
  }


  @Test
  def testInsertCouchbase(): Unit = {
    val cluster = CouchbaseCluster.create("bigtest-cmpt-129-204").authenticate("root", "123456")
    val bucket = cluster.openBucket("usee", 10, TimeUnit.SECONDS)

    //首页聚合接口业务前缀
    val businessPrefix = "p:a:"
    val moduleName = "vip"

    val host = "bigtest-cmpt-129-204"
    val port = 6379
    val db = 5
    val config: JedisPoolConfig = new JedisPoolConfig()
    val metadataPool: JedisPool = new JedisPool(config, host, port, 100 * Protocol.DEFAULT_TIMEOUT, null, db)
    val redis: Jedis = metadataPool.getResource



    val sid = "e5l71b7od4d4"
    val uid = 10000003

    //获得这个vip影片的相似影片
    val redisContent = redis.zrevrange(sid, 0, -1).toArray
    val recommend = if (redisContent.length > 1) {
      redisContent.takeRight(math.min(redisContent.length - 1, 60))
    } else {
      Array.empty[String]
    }

    //recommend.foreach(e => println(e))

    //有可用于更新的数据
    if (recommend.length > 0 && bucket.exists(businessPrefix + uid)) {
      //获得cc中，用户的首页聚合接口的推荐结果
      val jsonDocument = bucket.get(businessPrefix + uid)
      val content = jsonDocument.content()

      //如果有vip模块json，则删除；没有的话，添加
      if (content.containsKey(moduleName)) {
        content.removeKey(moduleName)
      }

      val jsonObj = JsonObject.create()
      jsonObj.put("als", "similar")
      val jsonArray = JsonArray.create()
      val recommendResult=ArrayUtils.randomArray(recommend).take(Math.min(20,recommend.length))
      recommendResult.foreach(e => {
        jsonArray.add(e)
      })
      jsonObj.put("id", jsonArray)
      content.put(moduleName, jsonObj)

      println(businessPrefix + uid)
      val newJsonDocument = JsonDocument.create(businessPrefix + uid, 172800, content)
      bucket.upsert(newJsonDocument)
    }
  }

    @Test
    def testReadRedis(): Unit = {
      //从redis获得vip影片的相似影片
      val host = "bigtest-cmpt-129-204"
      val port = 6379
      val db = 5

      val config: JedisPoolConfig = new JedisPoolConfig()
      val metadataPool: JedisPool = new JedisPool(config, host, port, 100 * Protocol.DEFAULT_TIMEOUT, null, db)
      val redis: Jedis = metadataPool.getResource

      val sid = "e5l71b7od4d4"
      val redisContent = redis.zrevrange(sid, 0, -1).toArray
      println(redisContent.length)
      val recommend = if (redisContent.length > 1) {
        redisContent.takeRight(math.min(redisContent.length - 1, 60))
      } else {
        Array.empty[String]
      }
      redisContent.foreach(e => println(e))
      println("---------")
      recommend.foreach(e => println(e))


    }

  }
