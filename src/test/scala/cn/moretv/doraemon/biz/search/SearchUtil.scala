package cn.moretv.doraemon.biz.search

import java.util.Date

import cn.moretv.doraemon.biz.util.{ConfigUtil, ElasticSearchUtil, ElasticSearchUtil2}
import cn.moretv.doraemon.common.enum.{EnvEnum, FormatTypeEnum}
import cn.moretv.doraemon.common.path.RedisPath
import cn.moretv.doraemon.data.writer.DataWriter2Kafka
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * Created by lituo on 2018/11/16.
  */
object SearchUtil{
  val redisWriter: DataWriter2Kafka = new DataWriter2Kafka

  /**
    * 将dataframe的内容批量插入ES和redis
    *
    * @param df        输入数据，包含字段 searchKey，contentType, content
    * @param indexName 索引名称。索引type固定为doc
    * @param biz
    * @param alg
    */
  def outputBatch(df: DataFrame,
                  indexName: String,
                  redisKeyPrefix: String,
                  biz: String,
                  alg: String)
                 (implicit env: EnvEnum.Value): Unit = {

    //输出到ES
    val BATCH_SIZE = 5000
    df.foreachPartition(iter => {
      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      var batch = 0
      var size = 0
      while (iter.hasNext) {
        batch = batch + 1
        println("Batch " + batch + "--" + new Date())
        val stringBuilder = new mutable.StringBuilder(1000000)
        while (size < BATCH_SIZE && iter.hasNext) {
          size = size + 1
          val row = iter.next()
          val mutableValueMap = mutable.HashMap[String, Any]()
          val searchKey = row.getAs[String]("searchKey")
          mutableValueMap.put("key", searchKey)
          mutableValueMap.put("alg", alg)
          mutableValueMap.put("biz", biz)
          val value = row.getAs[Seq[Row]]("value")
          value.foreach(r => mutableValueMap.put(r.getAs[String]("contentType"), r.getAs[String]("content")))

          val valueString = mapper.writeValueAsString(mutableValueMap.toMap)

          val indexMap = Map("index" -> Map("_type" -> "doc", "_index" -> indexName, "_id" -> searchKey))
          val indexString = mapper.writeValueAsString(indexMap)

          stringBuilder.append(indexString).append("\n").append(valueString).append("\n")

          if (searchKey.length <= 3) {
            redisWriter.writeOne(Map("key" -> (redisKeyPrefix + searchKey), "value" -> mutableValueMap.toMap), getRedisPath)
          }
        }
        println("Batch " + batch + " Insert --" + new Date())
        ElasticSearchUtil.bulkCreateIndex(stringBuilder.toString())
        println("Batch " + batch + " Complete --" + new Date())

        size = 0

      }
    })
  }

  /**
    * 更新ES索引中的一条数据，如果不存在则新建
    *
    * @param searchKey     搜索词，同时也是es的一条记录的id
    * @param indexName     索引名称。索引type固定为doc
    * @param biz           业务名，只在新建时有效
    * @param alg           算法名，只在新建时有效
    * @param addContent    需要添加的内容，map的key是contentType，value的格式是【sid_highlight_score】
    * @param deleteContent 需要删除的内容，map的key是contentType，value是sid
    * @return
    */
  def updateSearchKey(searchKey: String,
                      indexName: String,
                      redisKeyPrefix: String,
                      biz: String,
                      alg: String,
                      addContent: Map[String, Seq[String]],
                      deleteContent: Map[String, Seq[String]])(implicit env: EnvEnum.Value): Boolean = {

    val current = ElasticSearchUtil2.get(indexName, "doc", searchKey)
    if (current == null && addContent.nonEmpty) {
      val result = addContent.mapValues(v => v.mkString("|")) ++ Map("biz" -> biz, "alg" -> alg, "key" -> searchKey)
      ElasticSearchUtil2.index(indexName, "doc", searchKey, result)
      if (searchKey.length <= 3) {
        /*println("key:"+redisKeyPrefix + searchKey)
        println("value:" +result.toString())*/
        redisWriter.writeOne(Map("key" -> (redisKeyPrefix + searchKey), "value" -> result), getRedisPath)
        //redisWriter2.writeOne(Map("key" -> (redisKeyPrefix + searchKey), "value" -> result), getRedisPath4dev)
      }
      return true
    }

    val result = mutable.Map[String, Any](current.toSeq: _*)

    //throw new RuntimeException(result.toString()+"####################"+addContent.toString()+"####################"+deleteContent.toString())
        /*println("当前：")
        println(result)
        println(addContent)
        println(deleteContent)*/

    addContent.foreach(map => {
      val content = result.get(map._1)
      content match {
        case Some(c) => {
          result.put(map._1,
            (c.asInstanceOf[String].split("\\|").toSeq ++ map._2).distinct.map(s => s.split("_"))
              .filter(s => s.length == 3)
              .sortBy(s => s(2).toDouble * -1)
              .map(s => s.mkString("_"))
              .mkString("|"))
        }
        case None =>
      }
    })

    deleteContent.foreach(map => {
      val content = result.get(map._1)
      content match {
        case Some(c) => {
          result.put(map._1,
            c.toString.split("\\|").toSeq
              .filter(s => s != null && s.trim != "" && s.split("_").length == 3 && !map._2.contains(s.split("_")(0))) //过滤掉对应的sid
              .mkString("|"))
        }
        case None =>
      }
    })

        /*println("处理后：")
        println(result)*/

    ElasticSearchUtil2.update(indexName, "doc", searchKey, result.toMap)

    //输出到redis
    if (searchKey.length <= 3) {
      /*println("key:"+redisKeyPrefix + searchKey)
      println("value:" +result.toString())*/
      redisWriter.writeOne(Map("key" -> (redisKeyPrefix + searchKey), "value" -> result), getRedisPath)
      //redisWriter2.writeOne(Map("key" -> (redisKeyPrefix + searchKey), "value" -> result), getRedisPath4dev)
    }

    true
  }

  var redisPath: RedisPath = _

  def getRedisPath()(implicit env: EnvEnum.Value): RedisPath = {
    if (redisPath == null) {
      val topic = ConfigUtil.get("search.redis.topic")
      val host = ConfigUtil.get("search.redis.host")
      val port = ConfigUtil.getInt("search.redis.port")
      val dbIndex = ConfigUtil.getInt("search.redis.dbindex")
      val ttl = ConfigUtil.getInt("search.redis.ttl")
      val formatType = FormatTypeEnum.HASH
      redisPath = RedisPath(topic, host, port, dbIndex, ttl, formatType)
    }
    redisPath
  }

  def getRedisPath4dev()(implicit env: EnvEnum.Value): RedisPath = {
    if (redisPath == null) {
      val topic = ConfigUtil.get("search.redis.topic")
      val host = "172.16.16.93"
      val port = 6379
      val dbIndex = 12
      val ttl = ConfigUtil.getInt("search.redis.ttl")
      val formatType = FormatTypeEnum.HASH
      redisPath = RedisPath(topic, host, port, dbIndex, ttl, formatType)
    }
    redisPath
  }

}


