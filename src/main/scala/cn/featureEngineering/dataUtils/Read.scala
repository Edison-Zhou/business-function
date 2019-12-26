package cn.featureEngineering.dataUtils

import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.FileFormatEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2018/11/7.
  */
object Read {
  /**
    * 媒资库的MySQL路径
    * @param contentType 视频类型
    * @return
    */
  def validVideoPath(contentType: String): MysqlPath = {
    new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley", "id",
      Array("sid", "year", "cast", "director", "actor", "duration", "tags", "area",
        "score", "source", "language", "supply_type", "risk_flag", "copyright", "honor"),
      contentType match {
        case "movie" => "sid is not null and title is not null " +
          s" and contentType = '$contentType' and status = 1 and type = 1"
        case _ => "sid is not null and title is not null " +
          s" and contentType = '$contentType' and status = 1 and type = 1 and videoType = 1"
      })
  }

  /**
    * 视频的媒资数据
    * @param contentType 视频类型
    * @return
    */
  def videoMeiZiData(contentType: String): DataFrame = {
    val videoPath = Read.validVideoPath("movie")
    val videoData = DataReader.read(videoPath).filter("sid is not null").selectExpr("sid",
      "case when year is null then 0 else year end as year",
      "case when cast is null then 'missing' else cast end as cast",
      "case when director is null then 'missing' else director end as director",
      "case when duration is null then 0 else duration end as duration",
      "case when tags is null then 'missing' else tags end as tags",
      "case when area is null then 'missing' else area end as area",
      "case when score is null then 0 when score >= 10 then 0 else score end as score",
      "case when source is null then 'missing' else source end as source",
      "case when language is null then 'missing' else language end as language",
      "case when supply_type is null then 'missing' else supply_type end as supply_type",
      "case when risk_flag is null then 0 else risk_flag end as risk_flag",
      "case when copyright is null then 0 else copyright end as copyright",
      "case when honor is null then 'missing' else honor end as honor")
      .selectExpr("sid",
        "cast (year as string)",
        "cast (cast as string)",
        "cast (director as string)",
        "cast (duration as string)",
        "cast (tags as string)",
        "cast (area as string)",
        "cast (score as string)",
        "cast (source as string)",
        "cast (language as string)",
        "cast (supply_type as string)",
        "cast (risk_flag as string)",
        "cast (copyright as string)",
        "cast (honor as string)")

    videoData
  }


  /**
    * 获取视频标签
    * @param contentType 视频类型
    * @return DF("tag", "sid", "rating")
    */
  def getVideoTag(contentType: String): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val tag_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", "id",
      Array("id", "tag_name", "tag_type"),
      "is_douban = 1 and `status` = 1 and is_media_source = 1 and id is not null and tag_name is not null and tag_type is not null")

    val tag_program_config_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_config", "bislave", "slave4bi@whaley", "id",
      Array("content_type", "tag_type"),
      s"status = 1 and content_type = '$contentType' and content_type is not null and tag_type is not null")

    val tag_program_mapping_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
      Array("sid", "tag_id"),
      "`status` = 1 and sid is not null and tag_id is not null")

    val tagDF = DataReader.read(tag_Path).toDF("id", "tag_name", "tag_type")
    val tagProgramConfigDF = DataReader.read(tag_program_config_Path).toDF("content_type", "tag_type")
    val tagProgramMapppingDF = DataReader.read(tag_program_mapping_Path).toDF("sid", "tag_id")

    tagDF.as("a").join(tagProgramConfigDF.as("b"), expr("a.tag_type = b.tag_type"), "inner")
      .join(tagProgramMapppingDF.as("c"), expr("a.id = c.tag_id"))
      .selectExpr("a.tag_name as tag", "c.sid as sid").withColumn("rating", lit(1.0))
      .map(r => (r.getAs[String]("tag").replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9]+", ""),
         r.getAs[String]("sid"), r.getAs[Double]("rating"))).distinct()
      .toDF("tag", "sid", "rating").filter("tag != ''")
  }


  /**
    * 获取视频标签
    * @param contentType 视频类型
    * @return DF("tag_id", "tag_name", "sid", "rating")
    */
  def getVideoTag2(contentType: String): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val tag_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", "id",
      Array("id", "tag_name", "tag_type"),
      "`status` = 1 and is_media_source = 1 and id is not null and tag_name is not null and tag_type is not null")

    val tag_program_config_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_config", "bislave", "slave4bi@whaley", "id",
      Array("content_type", "tag_type"),
      s"status = 1 and content_type = '$contentType' and content_type is not null and tag_type is not null")

    val tag_program_mapping_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
      Array("sid", "tag_id"),
      "`status` = 1 and sid is not null and tag_id is not null")

    val tagDF = DataReader.read(tag_Path).toDF("id", "tag_name", "tag_type")
    val tagProgramConfigDF = DataReader.read(tag_program_config_Path).toDF("content_type", "tag_type")
    val tagProgramMapppingDF = DataReader.read(tag_program_mapping_Path).toDF("sid", "tag_id")

    tagDF.as("a").join(tagProgramConfigDF.as("b"), expr("a.tag_type = b.tag_type"), "inner")
      .join(tagProgramMapppingDF.as("c"), expr("a.id = c.tag_id"))
      .selectExpr("c.tag_id as tag_id", "a.tag_name as tag_name", "c.sid as sid").withColumn("rating", lit(1.0))
      .map(r => (r.getAs[Int]("tag_id"), r.getAs[String]("tag_name"), r.getAs[String]("sid"), r.getAs[Double]("rating"))).distinct()
      .toDF("tag_id", "tag_name", "sid", "rating")
  }
  /**
    * 获取视频标签
    * @param contentType 视频类型
    * @return DF("tag_id", "tag_name", "sid", "rating")
    */
  def getVideoTag3(contentType: String): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val tag_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", "id",
      Array("id", "tag_name", "tag_type"),
      "`status` = 1 and is_media_source = 1 and id is not null and tag_name is not null and tag_type is not null")

    val tag_program_config_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_config", "bislave", "slave4bi@whaley", "id",
      Array("content_type", "tag_type"),
      s"status = 1 and content_type = '$contentType' and content_type is not null and tag_type is not null")

    val tag_program_mapping_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
      Array("sid", "tag_id"),
      "`status` = 1 and sid is not null and tag_id is not null and tag_source != 3")

    val tagDF = DataReader.read(tag_Path).toDF("id", "tag_name", "tag_type")
    val tagProgramConfigDF = DataReader.read(tag_program_config_Path).toDF("content_type", "tag_type")
    val tagProgramMapppingDF = DataReader.read(tag_program_mapping_Path).toDF("sid", "tag_id")

    tagDF.as("a").join(tagProgramConfigDF.as("b"), expr("a.tag_type = b.tag_type"), "inner")
      .join(tagProgramMapppingDF.as("c"), expr("a.id = c.tag_id"))
      .selectExpr("c.tag_id as tag_id", "a.tag_name as tag_name", "c.sid as sid").withColumn("rating", lit(0.9))
      .map(r => (r.getAs[Int]("tag_id"), r.getAs[String]("tag_name"), r.getAs[String]("sid"), r.getAs[Double]("rating"))).distinct()
      .toDF("tag_id", "tag_name", "sid", "rating")
  }

  /**
    * 获取视频标签,并根据标签对影片的重要性赋予一定权重
    * @param contentType 视频类型
    * @return DF("tag_id", "tag_name", "sid", "rating")
    */
  def getVideoTag4(contentType: String): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val tag_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", "id",
      Array("id", "tag_name", "tag_type"),
      "`status` = 1 and is_media_source = 1 and id is not null and tag_name is not null and tag_type is not null")

    val tag_program_config_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_config", "bislave", "slave4bi@whaley", "id",
      Array("content_type", "tag_type"),
      s"status = 1 and content_type = '$contentType' and content_type is not null and tag_type is not null")

    val tag_program_mapping_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", "id",
      Array("sid", "tag_id", "tag_level_value"),
      "`status` = 1 and sid is not null and tag_id is not null and tag_source != 3")

    val tagDF = DataReader.read(tag_Path).toDF("id", "tag_name", "tag_type")
    val tagProgramConfigDF = DataReader.read(tag_program_config_Path).toDF("content_type", "tag_type")
    val tagProgramMapppingDF = DataReader.read(tag_program_mapping_Path).toDF("sid", "tag_id", "tag_level_value")

    tagDF.as("a").join(tagProgramConfigDF.as("b"), expr("a.tag_type = b.tag_type"), "inner")
      .join(tagProgramMapppingDF.as("c"), expr("a.id = c.tag_id"))
      .selectExpr("c.tag_id as tag_id", "a.tag_name as tag_name", "c.sid as sid", "c.tag_level_value as tag_level_value")
      .map(r => (r.getAs[Int]("tag_id"), r.getAs[String]("tag_name"), r.getAs[String]("sid"), r.getAs[Int]("tag_level_value")))
      .map(r => {
        val tagWeightValue = r._4 match {
          case 0 => 0.125 //未分类
          case 1 => 1.0 //核心
          case 2 => 0.5 //重要
          case 3 => 0.375 //主要
          case 4 => 0.25 //一般
        }
        (r._1, r._2, r._3, tagWeightValue)
      })
      .distinct().toDF("tag_id", "tag_name", "sid", "rating")
  }

  /**
    * 获取节目的Word2vec feature
    * @param contentType
    * @return
    */
  def getVideoWord2vecFeature(contentType: String): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val word2VecPath: HdfsPath = new HdfsPath(s"/ai/data/medusa/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
    DataReader.read(word2VecPath)
      .rdd.map(line => line.getString(0).split(","))
      .map(e => (e(0), e.takeRight(128)))
      .map(e => (e._1, e._2.map(x => x.toDouble)))
      .toDF("sid", "vector")
  }

  /**
    * 获取有效电影标签的信息
    * @return DF("id", "tag_name")
    */
  def getTagIndex: DataFrame = {
    val tag_Path = new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", "id",
      Array("id", "tag_name"),
      "`status` = 1 and id is not null and tag_name is not null and tag_type = 2")

    DataReader.read(tag_Path).toDF("tag_id", "tag_name")
  }

  /**
    * 获取视频的聚类
    * @param contentType
    * @return
    */
  def getVideoCluster(contentType: String): DataFrame = {
    DataReader.read(new HdfsPath(s"/ai/data/medusa/videoFeatures/${contentType}Clusters/Latest"))
      .toDF("index", "sidArray")
  }

}
