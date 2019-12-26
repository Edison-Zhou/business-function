package cn.moretv.doraemon.biz.detail

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.RedisPath
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by lituo on 2018/8/13.
  */
object DetailPageRecommend extends BaseClass {
  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    //获取影片评分数据
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(),-1))

    //获取搜索算法评分数据
    val video_score = BizUtils.getSearchVideoData(ss,date)
      .select(expr("sid"),expr("video_score"),expr("content_type"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    contentTypeList.foreach(contentType => {

      val vipDf = DataReader.read(BizUtils.getHdfsPathForRead("detailVip/" + contentType))
      vipDf.toDF("sid","item","score").createOrReplaceTempView(s"detailVip_${contentType}_tmp")

      //评分添加0-1随机数，打乱分数,过滤非vip节目
      val vipSql =
        s"""
           |  select a.sid,a.item from
           |  (
           |    select sid,item,score,
           |    row_number() over(partition by sid order by (score+rand()) desc) as rn
           |    from detailVip_${contentType}_tmp
           |   ) a
           |  inner join
           |  (
           |    select sid from `ods_view`.`db_snapshot_mysql_medusa_mtv_program`
           |    where key_day='latest' and key_hour='latest' and status = 1
           |    and type = 1 and supply_type = 'vip'
           |   ) b
           |   on a.item = b.sid and  a.rn <= 24
        """.stripMargin


      val vipFoldDf = sqlContext.sql(vipSql).groupBy(expr("sid")).agg(collect_list(expr("item")).as("id"))
        .selectExpr("cast(sid as string) as sid", "id")

      val hotDf = DataReader.read(BizUtils.getHdfsPathForRead("hot/" + contentType))
      hotDf.toDF("sid","item","score").createOrReplaceTempView(s"hot_${contentType}_tmp")

      //评分添加0-1随机数，打乱分数
      val hotSql =
        s"""
           |select a.sid,a.item from
           |(
           |  select sid,item,score,
           |    row_number() over(partition by sid order by (score+rand()) desc) as rn
           |  from hot_${contentType}_tmp
           |  ) a
           |where a.rn <= 24
        """.stripMargin

      val hotFoldDf = sqlContext.sql(hotSql).groupBy(expr("sid")).agg(collect_list(expr("item")).as("id"))
        .selectExpr("cast(sid as string) as sid", "id")


      val similarDf = DataReader.read(BizUtils.getHdfsPathForRead("similarMix/" + contentType))
      similarDf.toDF("sid","item","score").createOrReplaceTempView(s"similar_${contentType}_tmp")


      val similarSql =
        s"""
           |select a.sid,a.item from (
           |  select sid,item,score,
           |  row_number() over(partition by sid order by score desc) as rn
           |  from similar_${contentType}_tmp
           |) a
           | where a.rn <=24
        """.stripMargin
      val similarFoldDf = sqlContext.sql(similarSql).groupBy(expr("sid")).agg(collect_list("item").alias("id"))
        .selectExpr("cast(sid as string) as sid", "id")


      val themeDf = DataReader.read(BizUtils.getHdfsPathForRead("theme/" + contentType))
      themeDf.toDF("sid", "id").select(expr("sid"),explode(expr("id")).alias("item"))
        .as("a").join(video_score.as("b"),expr("a.item=b.sid"),"left")
        .selectExpr("a.sid","a.item","b.video_score")
        .createOrReplaceTempView(s"theme_${contentType}_tmp")
      val themeSql =
        s"""
          |select a.sid,a.item from
          | (
          |   select sid,item,video_score,
          |   row_number() over(partition by sid order by video_score desc) as rn
          |   from theme_${contentType}_tmp
          | ) a
          | where a.rn<=20
        """.stripMargin

      val themeFoldDf = sqlContext.sql(themeSql).groupBy(expr("sid")).agg(collect_list(expr("item")).as("id"))
        .selectExpr("cast(sid as string) as sid", "id")


      val subjectDf = DataReader.read(BizUtils.getHdfsPathForRead("subject/" + contentType))
      val subjectFoldDf = subjectDf.toDF("sid", "id")
        .selectExpr("cast(sid as string) as sid", "id")



      val dataPackParam = new DataPackParam
      dataPackParam.format = FormatTypeEnum.KV
      dataPackParam.extraValueMap = Map("alg" -> "mix")
      val similarPackDf = DataPack.pack(similarFoldDf, dataPackParam).toDF("key", "similar")

      val dataPackParam2 = new DataPackParam
      dataPackParam2.format = FormatTypeEnum.KV
      dataPackParam2.extraValueMap = Map("alg" -> "random", "title" -> "更多精彩")
      val themePackDf = DataPack.pack(themeFoldDf, dataPackParam2).toDF("key", "theme")

      val dataPackParam3 = new DataPackParam
      dataPackParam3.format = FormatTypeEnum.KV
      dataPackParam3.extraValueMap = Map("alg" -> "relevantSubject", "title" -> "专题推荐")
      val subjectPackDf = DataPack.pack(subjectFoldDf, dataPackParam3).toDF("key", "subject")

      val dataPackParam4 = new DataPackParam
      dataPackParam4.format = FormatTypeEnum.KV
      dataPackParam4.extraValueMap = Map("alg" -> "mixHot", "title" -> "热门推荐")
      val hotPackDf = DataPack.pack(hotFoldDf, dataPackParam4).toDF("key", "detailHot")


      val dataPackParam5 = new DataPackParam
      dataPackParam5.format = FormatTypeEnum.KV
      dataPackParam5.extraValueMap = Map("date" -> new SimpleDateFormat("yyyyMMdd").format(new Date()))


      val dataPackParam6 = new DataPackParam
      dataPackParam6.format = FormatTypeEnum.KV
      dataPackParam6.extraValueMap = Map("alg" -> "mixVip", "title" -> "会员推荐")
      val detailVipPackDf = DataPack.pack(vipFoldDf, dataPackParam6).toDF("key", "detailVip")

      var joinDf = similarPackDf.as("a").join(themePackDf.as("b"), expr("a.key = b.key"), "full")
        .join(subjectPackDf.as("c"), expr("a.key = c.key or b.key = c.key"), "full")
        .join(hotPackDf.as("d"), expr("a.key = d.key or b.key = d.key or c.key=d.key "), "full")
        .join(detailVipPackDf.as("e"), expr("a.key = e.key or b.key = e.key or c.key=e.key or  d.key=e.key "), "full")
        .selectExpr("case when a.key is not null then a.key " +
          "when b.key is not null then b.key " +
          "when c.key is not null then c.key " +
          "when d.key is not null then d.key else e.key end as key",
          "a.similar", "b.theme", "c.subject","d.detailHot","e.detailVip")

      //key转真实id，行数增多
      val sidRel = BizUtils.readVirtualSidRelation()
      joinDf = joinDf.as("a").join(sidRel.as("b"), expr("a.key = b.virtual_sid"), "leftouter")
        .selectExpr("case when b.sid is not null then b.sid else a.key end as key",
          "similar", "theme", "subject","detailHot","detailVip")

      val result = DataPack.pack(joinDf, dataPackParam5)

      val dataWriter = new DataWriter2Kafka

      val topic = ConfigUtil.get("detail.redis.topic")
      val host = ConfigUtil.get("detail.redis.host")
      val port = ConfigUtil.getInt("detail.redis.port")
      val dbIndex = ConfigUtil.getInt("detail.redis.dbindex")
      val ttl = ConfigUtil.getInt("detail.redis.ttl")
      val formatType = FormatTypeEnum.KV
      val path = RedisPath(topic, host, port, dbIndex, ttl, formatType)

      dataWriter.write(result, path)
    })

  }
}
