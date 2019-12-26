package cn.moretv.doraemon.biz.cluster

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.moretv.doraemon.common.util.DateUtils
import cn.whaley.sdk.dataOps.HDFSOps
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/4/1.
  */
object DataExtract4DL extends BaseClass{


  /**
    *
    * @param contentType
    * @param scoreThreshold
    */
  def extractUserWatchedVideos(contentType: String,
                               scoreThreshold: Double,
                               targetPath: String,
                               numDays: Int
                              ): Unit ={
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val dataSet = getScoreData(contentType, scoreThreshold, numDays)
      .groupBy("uid")
      .agg(collect_list("sid").as("sids"))
      .map(r => {
          val sids = r.getAs[Seq[String]]("sids")
          (sids, sids.size)
        }).toDF("sids", "count").filter("count > 1")
      .map(r => r.getAs[Seq[String]]("sids").mkString(","))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("dataSet")
    println(dataSet.count())

    val videos = dataSet.flatMap(r => r.split(",").distinct).distinct()

    println("Videos")
    println(videos.count())

    HDFSOps.deleteHDFSFile(targetPath + s"/$contentType")
    dataSet.rdd.repartition(1).saveAsTextFile(targetPath + s"/$contentType")
  }

  /**
    * 获取用户观看历史评分
    * @param contentType 类型
    * @param scoreThreshold 评分阈值
    * @return DF("uid", "sid")
    */
  def getScoreData(contentType:String,
                   scoreThreshold:Double,
                   numDays:Int):DataFrame = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val path = s"/ai/etl/ai_base_behavior/product_line=moretv/partition_tag=V1/content_type=$contentType/*"
    val date = DateUtils.farthestDayWithDelimiter(numDays)

    DataReader.read(new HdfsPath(path))
      .selectExpr("userid as uid", "sid_or_subject_code as sid", "latest_optime", "score")
      .filter(s"latest_optime < '$date' and score > $scoreThreshold")
      .drop("score").drop("latest_optime")
  }

  /**
    *
    * @return DF("sid", "tag")
    */
  def getMovieTag:DataFrame = {
    val tag_program_mapping = DataReader.read(new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program_mapping", "bislave", "slave4bi@whaley", Array("sid", "tag_id"),
      "status = 1"))

    val tag_program = DataReader.read(new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag_program", "bislave", "slave4bi@whaley", Array("sid"),
      "content_type = 'movie'"))

    val tag = DataReader.read(new MysqlPath("bigdata-appsvr-130-6", 3306,
      "europa", "tag", "bislave", "slave4bi@whaley", Array("id", "tag_name"),
      "tag_type_id in (301, 303)"))

    tag_program_mapping.as("a").join(tag_program.as("b"), expr("a.sid = b.sid"), "inner")
        .join(tag.as("c"), expr("a.tag_id = c.id"), "inner")
      .selectExpr("a.sid as sid", "c.tag_name as tag")
  }

  /**
    * 获取用户观看过的标签
    * @param contentType
    * @param scoreThreshold
    * @param targetPath
    */
  def extractUserWatchedTags(contentType: String,
                             scoreThreshold: Double,
                             targetPath: String,
                             numDays: Int
                              ): Unit ={
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    val scoreDF = getScoreData(contentType, scoreThreshold, numDays)
    println(s"scoreDF = ${scoreDF.count()}")

    val tagDF = getMovieTag
    println(s"tagDF = ${tagDF.count()}")

    val dataSet = scoreDF.as("a").join(tagDF.as("b"), expr("a.sid = b.sid"), "inner")
      .drop(expr("a.sid")).drop(expr("b.sid"))
      .groupBy("uid")
      .agg(collect_list("tag").as("tags"))
      .map(r => r.getAs[Seq[String]]("tags").mkString(","))
      .persist(StorageLevel.DISK_ONLY)
    println(s"user count = ${dataSet.count()}")

    val tags = dataSet.flatMap(r => r.split(",").distinct).distinct()

    println(s"tags = ${tags.count()}")

    HDFSOps.deleteHDFSFile(targetPath + s"/$contentType")
    dataSet.rdd.repartition(1).saveAsTextFile(targetPath + s"/$contentType")
  }

  override def execute(args: Array[String]): Unit = {
    Array("movie", "tv", "zongyi", "kids", "comic", "jilu").foreach(contentType => {
      extractUserWatchedVideos(contentType, 0.5, "/ai/tmp/DataExtract4DL/watchedLongVideos", 40)
      //extractUserWatchedTags(contentType, 0.5, "/ai/tmp/DataExtract4DL/watchedMovieTags")
    })
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

}
