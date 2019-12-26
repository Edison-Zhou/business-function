package cn.moretv.doraemon.biz.search

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.ConfigUtil
import cn.moretv.doraemon.common.enum.ProductLineEnum
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger


/**
  * Created by lituo on 2018/11/19.
  */
object SearchStreaming extends BaseClass {

  override def execute(args: Array[String]): Unit = {

    //测试代码
    val ss = spark
    import ss.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigUtil.get("search.sync.kafka.servers"))
      .option("subscribe", ConfigUtil.get("search.sync.kafka.topic.virtualprogram") + ","
        + ConfigUtil.get("search.sync.kafka.topic.program") + ","
        + ConfigUtil.get("search.sync.kafka.topic.subject") + ","
        + ConfigUtil.get("search.sync.kafka.topic.person"))
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(topic AS STRING)")
      .as[(String, String, String)]

    val program = df.where(s"topic = '${ConfigUtil.get("search.sync.kafka.topic.program")}'")
      .select(get_json_object(col("value"), "$.sid").as("sid"),
        get_json_object(col("value"), "$.contentType").as("contentType"),
        get_json_object(col("value"), "$.title").as("title"),
        get_json_object(col("value"), "$.riskFlag").cast("int").as("riskFlag"),
        get_json_object(col("value"), "$.status").cast("int").as("status"),
        to_timestamp(get_json_object(col("value"), "$.updateTime")).as("updateTime"))
      .filter("sid is not null and contentType is not null and title is not null and riskFlag is not null and status is not null and updateTime is not null")

    val longVideoDf = program.where("contentType in ('movie', 'tv', 'zongyi', 'jilu', 'kids', 'comic')")

    val virtualProgram = df.where(s"topic = '${ConfigUtil.get("search.sync.kafka.topic.virtualprogram")}'")
      .select(get_json_object(col("value"), "$.virtualSid").as("virtualSid"),
        get_json_object(col("value"), "$.contents").as("contents"),
        to_timestamp(get_json_object(col("value"), "$.updateTime")).as("updateTime"))
      .map(r => (r.getAs[String]("virtualSid"),  JSON.parseArray(r.getAs[String]("contents")).toArray.
        map(x => (x.asInstanceOf[JSONObject].getString("sid"),
          x.asInstanceOf[JSONObject].getString("contentType"),
          x.asInstanceOf[JSONObject].getString("title"),
          x.asInstanceOf[JSONObject].getString("status").toInt,
          x.asInstanceOf[JSONObject].getString("copyrightCode"))).toSeq,
        r.getAs[java.sql.Timestamp]("updateTime"))
      )
        .flatMap(x => {
          val contents = x._2
          x._2.map(y => (y._1, contents, x._3))
        })
      .toDF("sid", "contents", "updateTime")

    val longVideoJoinDf = longVideoDf.map(r => (r.getAs[String]("sid"),
      r.getAs[String]("contentType"),
      r.getAs[String]("title"),
      r.getAs[Int]("riskFlag"),
      r.getAs[Int]("status"),
      r.getAs[java.sql.Timestamp]("updateTime"))).toDF("sid", "contentType", "title", "riskFlag", "status", "updateTime")
      .withWatermark("updateTime", "1 hours")
      .as("A").join(virtualProgram.withWatermark("updateTime", "1 hours").as("B"),
      expr("A.sid = B.sid" +
        " AND " +
        "B.updateTime >= A.updateTime " +
        " AND " +
        "B.updateTime <= A.updateTime + interval 1 hour"),
      joinType = "leftOuter").select("A.sid", "contentType", "title", "riskFlag", "status", "contents")
      .map(r => (r.getAs[String]("sid"),
        r.getAs[String]("contentType"),
        r.getAs[String]("title"),
        r.getAs[Int]("riskFlag"),
        r.getAs[Int]("status"),
        //sid, contentType, title, status, copyrightCode
        r.getAs[Seq[Row]]("contents") match {
          case null => Seq((r.getAs[String]("sid"),
            r.getAs[String]("contentType"),
            r.getAs[String]("title"),
            r.getAs[Int]("status"),
            "useless"))
          case _ => r.getAs[Seq[Row]]("contents").map(x => (x.getString(0), x.getString(1), x.getString(2), x.getInt(3), x.getString(4)))
        }))
        .flatMap(e => {//sid, contentType, title, riskFlag, status
          var isTencentAvalible = false
          e._6.foreach(x => if(x._5 == "tencent" && x._4 == 1) isTencentAvalible = true)
          if(isTencentAvalible) { //有腾讯源，腾讯源节目优先
            e._6.map(x => (x._1, x._2, x._3, x._4, if (x._5 == "tencent") 1 else -1))
          }else { //无腾讯源，本节目上线，其他节目下线
            e._6.map(x => (x._1, x._2, x._3, x._4, if (e._5 == 1) 1 else -1))
          }
        })
      .toDF("sid", "contentType", "title", "riskFlag", "status")
      .distinct()
      .filter("sid is not null and contentType is not null and title is not null and riskFlag is not null and status is not null")

    val shortVideoDf = program.where("contentType in ('hot', 'game', 'sports', 'mv')")

    val subjectVideoDf = df.where(s"topic = '${ConfigUtil.get("search.sync.kafka.topic.subject")}'")
      .select(get_json_object(col("value"), "$.code").as("code"),
        get_json_object(col("value"), "$.title").as("title"),
        get_json_object(col("value"), "$.status").cast("int").as("status"))
      .filter("code is not null and title is not null and status is not null")

    val starVideoDf = df.where(s"topic = '${ConfigUtil.get("search.sync.kafka.topic.person")}'")
      .select(get_json_object(col("value"), "$.sid").as("sid"),
        get_json_object(col("value"), "$.name").as("name"),
        get_json_object(col("value"), "$.status").cast("int").as("status"))
      .filter("sid is not null and name is not null and status is not null")

    //召回和计算匹配分
    val searchWordDfAdd = SearchAlg.recall(longVideoJoinDf.where("status = 1"),
      shortVideoDf.where("status = 1"),
      subjectVideoDf.where("status = 1"),
      starVideoDf.where("status = 1"))

    val searchWordDfDelete = SearchAlg.recall(longVideoJoinDf.where("status != 1"),
      shortVideoDf.where("status != 1"),
      subjectVideoDf.where("status != 1"),
      starVideoDf.where("status != 1"))
      .withColumn("highlight", lit("")) //使用highlight为空来表示是需要下线的

    //开始排序阶段
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))

    val videoScore = sqlContext.sql(s"select sid, video_score from dws_medusa_bi.medusa_video_score where day_p = '$date'")

    val beforeReorderDf = SearchAlg.computeScore(searchWordDfAdd.union(searchWordDfDelete), videoScore)
      .withColumn("timestamp", from_unixtime(unix_timestamp()).cast("timestamp"))
      .withWatermark("timestamp", "10 minutes")
      .groupBy(window(col("timestamp"), "3 minutes", "3 minutes"), col("searchKey"), col("contentType"))
      .agg(collect_list(concat_ws("_", col("sid"), col("highlight"), col("score"), col("riskFlag"))).as("content"))

    val orderDf = SearchAlg.reorder(beforeReorderDf)

    //输出结果到ES
    val biz = "pinyin_search"
    val alg = "rule0_streaming"

    orderDf.writeStream
      .foreach(new EsForeachWriter(alg, biz))
      .option("checkpointLocation", ConfigUtil.get("search.streaming.checkpoint"))
      .trigger(Trigger.ProcessingTime(3, TimeUnit.MINUTES))
      .queryName("search-streaming")
      .outputMode("update")
      .start()

    spark.streams.awaitAnyTermination()
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
