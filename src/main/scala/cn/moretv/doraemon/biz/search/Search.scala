package cn.moretv.doraemon.biz.search


import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by lituo on 2018/10/11.
  */
object Search extends BaseClass {

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    //有效影片的数据

    val validSidPath = BizUtils.getMoretvTestMysqlPath("long_valid_sid")
    val longVideoDf = deDuplicateByVirtualSid(DataReader.read(validSidPath).toDF("sid", "contentType", "title", "riskFlag"))
      .select("sid", "contentType", "title", "riskFlag")

    val shortSidPath = BizUtils.getMoretvTestMysqlPath("short_valid_sid")
    val shortVideoDf = DataReader.read(shortSidPath).toDF("sid","contentType", "title", "riskFlag","updateTime")
      .filter(s"!(contentType in ('sports', 'hot') and updateTime < '${new SimpleDateFormat("yyyy-MM-dd")
        .format(DateUtils.addDays(new Date(), -180))}')").select("sid", "contentType", "title", "riskFlag")

    val subjectSidPath = BizUtils.getMoretvTestMysqlPath("valid_subject")
    val subjectVideoDf = DataReader.read(subjectSidPath).toDF("code", "title", "copyright")
      .map(r => {
        val code = r.getAs[String]("code")
        val title = r.getAs[String]("title")
        val riskFlag = r.getAs[Int]("copyright") match {
          case 1 => 0
          case _ => 1
        }
        (code, title, riskFlag)
      }).toDF("code", "title", "riskFlag")

    val starSidPath = BizUtils.getMoretvTestMysqlPath("valid_person")
    val starVideoDf = DataReader.read(starSidPath).toDF("sid", "name")

    val programPersonMappingDf = DataReader.read(BizUtils.getMoretvTestMysqlPath("program_person_mapping")).as("a")
      .join(starVideoDf.as("b"), expr("a.person_sid = b.sid")).selectExpr("a.content_sid as sid", "name")
      .join(longVideoDf, "sid").select("sid", "contentType", "title", "name", "riskFlag")
      .dropDuplicates("sid", "name")
    //此处多一步和长视频Df的join是为了保证视频节目皆为有效的,去重是因为发现数据库中有影人和节目信息的重复数据

    val useeSidPath = BizUtils.getUsee4SearchMysqlPath("pre")
    val useeVideoDf = DataReader.read(useeSidPath).withColumn("riskFlag", lit(2))
      .toDF("sid", "contentType", "title", "riskFlag")

    BizUtils.getDataFrameInfo(longVideoDf, "longVideoDF")
    BizUtils.getDataFrameInfo(shortVideoDf, "shortVideoDF")
    BizUtils.getDataFrameInfo(subjectVideoDf, "subjectVideoDF")
    BizUtils.getDataFrameInfo(starVideoDf, "starVideoDF")
    BizUtils.getDataFrameInfo(programPersonMappingDf, "programPersonMappingDf")
    BizUtils.getDataFrameInfo(useeVideoDf, "useeVideoDF")

    val searchWordDf = SearchAlg.recall(longVideoDf.union(useeVideoDf).repartition(20), shortVideoDf, subjectVideoDf, starVideoDf)
    //DF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore")
    val associativeWordMatchScoreDf = SearchAlg.recall4AssociativeWord(longVideoDf, subjectVideoDf, starVideoDf, programPersonMappingDf)
    //DF("sid", "titile", "contentType", "riskFlag", "associativeWord", "searchKey")
    BizUtils.getDataFrameInfo(associativeWordMatchScoreDf, "associativeWordMatchScoreDf")
    println("哪吒之魔童降世、斗罗大陆、在远方 和测试短视频的相关联想词结果如下：")
    associativeWordMatchScoreDf.filter("sid = 'tvwyd3rttu7n' or sid = 'tvwy45sutu9v' or sid = 'tvwycdx07ne5' or sid = 'tvwy2ctvdec3'")
      .show(500, false)

    val useeVideoScore = useeVideoDf.select("sid").withColumn("fakeScore", (rand() * 0.5 + 0.4) * 2.5)

    val searchWordDf2 = searchWordDf.as("a").join(useeVideoScore.as("b"), expr("a.sid = b.sid"), "left").drop(expr("b.sid"))
      .selectExpr("sid", "contentType", "riskFlag", "searchKey", "highlight", "case when fakeScore is null then matchScore else fakeScore end as matchScore")
      .repartition(50)

    //开始排序阶段
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))
    val keyDay = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -7))

    val videoScore = sqlContext.sql(s"select sid, video_score, douban_score from dws_medusa_bi.medusa_video_score where day_p = '$date'")
    val popularityScore = sqlContext.sql("select result_sid as sid, count(1) as num from ods_view.log_medusa_main4x_search_result_click " +
      s" WHERE key_day >= '$keyDay' group by result_sid")

    //置顶用的白名单
    val toppingDF = List(("tvwycec38sv0", "hot", 0, "dsm", "电视猫", 0.95),
      ("tvwycec3qtp8", "hot", 0, "dsm", "电视猫", 0.95),
      ("tvwy5i9vrsxy", "hot", 0, "dsm", "电视猫", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "hxc", "贺新春", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "qxhxc", "群星贺新春", 1.0),
      ("tvwy5i9vrsxy", "hot", 0, "qx", "群星", 1.0),
      ("tvwya1cdtve4", "tv", 0, "dsj", "电视剧", 1.0),
      ("ctvv0op6k9xgh", "personV2", 0, "xz", "肖战", 1.0),
      ("ctvv0opbdo8f5", "personV2", 0, "yy", "杨洋", 1.0))
      .toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "score")

    val searchKeyVideoInfoScoreDf = SearchAlg.computeScore2(searchWordDf2, videoScore, popularityScore).persist()

    val sidVideoScoreDf = searchKeyVideoInfoScoreDf.groupBy("sid").agg(max("score").as("videoScore"))
    BizUtils.getDataFrameInfo(sidVideoScoreDf.filter(
      "sid = 'tvwyd3rttu7n' or sid = 'tvwy45sutu9v' or sid = 'tvwycdx07ne5' or sid = 'tvwy2ctvdec3'"), "pinyinSearchScoreDf")

    val sidAssociativeWordVideoScoreDF = associativeWordMatchScoreDf.repartition(100).join(sidVideoScoreDf, "sid")
      .selectExpr("sid", "title", "contentType", "riskFlag", "associativeWord", "searchKey", "videoScore")
    BizUtils.getDataFrameInfo(sidAssociativeWordVideoScoreDF, "sidAssociativeWordVideoScoreDF")
    println("哪吒之魔童降世、斗罗大陆、在远方 的相关联想词及searchKey对应联想词分数结果如下：")
    sidAssociativeWordVideoScoreDF.filter("sid = 'tvwyd3rttu7n' or sid = 'tvwy45sutu9v' or sid = 'tvwycdx07ne5'")
      .show(500, false)

    val associativeWordScoreDF = sidAssociativeWordVideoScoreDF.groupBy( "associativeWord")
      .agg(max("videoScore").as("associativeWordScore"))
    BizUtils.getDataFrameInfo(associativeWordScoreDF, "associativeWordScoreDF")

    /*
    DF("sid", "title", "contentType", "riskFlag", "associativeWord", "searchKey", "videoScore")
    .join(DF("associativeWord", "associativeWordScore"))
      */
    val searchKeyAssociativeWordDF = sidAssociativeWordVideoScoreDF.as("a").repartition(50)
      .join(associativeWordScoreDF.as("b"), "associativeWord")
      .selectExpr("searchKey", "a.associativeWord", "associativeWordScore", "contentType", "sid", "title", "videoScore", "riskFlag")
      .map(r => {
        var sid = r.getString(4)
        if (r.getString(3) == "subject") sid = Base64Util.base64Encode(sid)
        (r.getString(0), r.getString(1), (r.getDouble(2)*1000).toInt.toDouble/1000, r.getString(3), sid, r.getString(5),
          (r.getDouble(6)*1000).toInt.toDouble/1000, r.getInt(7))
      }).toDF("searchKey", "associativeWord", "associativeWordScore", "contentType", "sid", "title", "score", "riskFlag")
        .sort($"searchKey", $"associativeWordScore".desc, $"score".desc).persist()
    println(s"searchKeyAssociativeWordDF.count():"+searchKeyAssociativeWordDF.count())
    println(s"searchKeyAssociativeWordDF.printSchema:")
    searchKeyAssociativeWordDF.printSchema()
    println("以周星驰为联想词的节目信息如下：")
    searchKeyAssociativeWordDF.filter("associativeWord = '周星驰'").show(200,false)
    println("哪吒之魔童降世的相关联想词及searchKey等信息如下：")
    searchKeyAssociativeWordDF.filter("sid = 'tvwyd3rttu7n'").show(100)
    println("以zxc为SK的Dataframe信息如下：")
    searchKeyAssociativeWordDF.filter("searchKey = 'zxc'").show(200)

    val SKAWLowRiskDF = searchKeyAssociativeWordDF.select("searchKey", "associativeWord", "associativeWordScore").distinct()
    val SKAWMiddleRiskDF = searchKeyAssociativeWordDF.filter("riskFlag <= 1").select("searchKey", "associativeWord", "associativeWordScore").distinct()
    val SKAWHighRiskDF = searchKeyAssociativeWordDF.filter("riskFlag = 0").select("searchKey", "associativeWord", "associativeWordScore").distinct()

    val SK2AWReorderLowRisk = changeFormat4SKAWDF(SKAWLowRiskDF)
    val SK2AWReorderMiddleRisk = changeFormat4SKAWDF(SKAWMiddleRiskDF)
    val SK2AWReorderHighRisk = changeFormat4SKAWDF(SKAWHighRiskDF)

    BizUtils.getDataFrameInfo(SK2AWReorderLowRisk, "SK2AWReorderLowRisk")
    BizUtils.getDataFrameInfo(SK2AWReorderMiddleRisk, "SK2AWReorderMiddleRisk")
    BizUtils.getDataFrameInfo(SK2AWReorderHighRisk, "SK2AWReorderHighRisk")
    BizUtils.outputWrite(SK2AWReorderLowRisk, "SK2AWReorder")


    val AW2SidLowRiskDF = searchKeyAssociativeWordDF.select("associativeWord", "sid", "contentType", "score", "title").distinct()
    val AW2SidMiddleRiskDF = searchKeyAssociativeWordDF.filter("riskFlag <= 1")
      .select("associativeWord", "sid", "contentType", "score", "title").distinct()
    val AW2SidHighRiskDF = searchKeyAssociativeWordDF.filter("riskFlag = 0")
      .select("associativeWord", "sid", "contentType", "score", "title").distinct()

    val AW2SidReorderLowRisk = changeFormat4AWSidDF(AW2SidLowRiskDF)
    val AW2SidReorderMiddleRisk = changeFormat4AWSidDF(AW2SidMiddleRiskDF)
    val AW2SidReorderHighRisk = changeFormat4AWSidDF(AW2SidHighRiskDF)

    BizUtils.getDataFrameInfo(AW2SidReorderLowRisk, "AW2SidReorderLowRisk")
    BizUtils.getDataFrameInfo(AW2SidReorderMiddleRisk, "AW2SidReorderMiddleRisk")
    BizUtils.getDataFrameInfo(AW2SidReorderHighRisk, "AW2SidReorderHighRisk")
    BizUtils.outputWrite(AW2SidReorderLowRisk, "AW2SidReorder")

    val beforeReorderDf = searchKeyVideoInfoScoreDf.union(toppingDF).map(r => {
      var sid = r.getString(0)
      if (r.getString(1) == "subject") sid = Base64Util.base64Encode(sid)
      (sid, r.getString(1), r.getInt(2), r.getString(3), r.getString(4), r.getDouble(5))
    }).toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "score")
      .groupBy("searchKey", "contentType")
      .agg(collect_list(concat_ws("_", col("sid"), col("highlight"), col("score"), col("riskFlag"))).as("content"))
      .repartition(50)



    val afterReorderDf = SearchAlg.reorder(beforeReorderDf)

    //BizUtils.getDataFrameInfo(afterReorderDf, "afterReorderDf")

    afterReorderDf.persist()

    BizUtils.outputWrite(afterReorderDf, "pinyinSearch")


    //输出结果到ES
    val biz = "pinyin_search"
    val alg = "rule0"
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 0), "moretv_search_low_risk", "IndexKeyWordKey:MINDEXLOW_", biz, alg)
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 1), "moretv_search_middle_risk", "IndexKeyWordKey:MINDEXHIGH_", biz, alg)
    SearchUtil.outputBatch(changeFormat(afterReorderDf, 2), "moretv_search_high_risk", "IndexKeyWordKey:MINDEXSUPERHIGH_", biz, alg)
    afterReorderDf.unpersist()


    val biz4SK2AW = "lianxiangci_search"
    val alg4SK2AW = "rule0"
    SearchUtil.outputBatch4SK2AW(SK2AWReorderLowRisk,"moretv_automated_search_low_risk", "IndexAutoMatedKeyWordKey:MINDEXLOW_", biz4SK2AW, alg4SK2AW)
    SearchUtil.outputBatch4SK2AW(SK2AWReorderMiddleRisk,"moretv_automated_search_middle_risk", "IndexAutoMatedKeyWordKey:MINDEXMIDDLE_", biz4SK2AW, alg4SK2AW)
    SearchUtil.outputBatch4SK2AW(SK2AWReorderHighRisk,"moretv_automated_search_high_risk", "IndexAutoMatedKeyWordKey:MINDEXHIGH_", biz4SK2AW, alg4SK2AW)

    val biz4AW2Sid = "zhongwen_search"
    val alg4AW2Sid = "rule0"
    SearchUtil.outputBatch4AW2Sid(AW2SidReorderLowRisk, "moretv_zhongwen_search_low_risk","IndexZhongwenKeyWordKey:MINDEXLOW_", biz4AW2Sid, alg4AW2Sid)
    SearchUtil.outputBatch4AW2Sid(AW2SidReorderMiddleRisk, "moretv_zhongwen_search_middle_risk","IndexZhongwenKeyWordKey:MINDEXMIDDLE_", biz4AW2Sid, alg4AW2Sid)
    SearchUtil.outputBatch4AW2Sid(AW2SidReorderHighRisk, "moretv_zhongwen_search_high_risk","IndexZhongwenKeyWordKey:MINDEXHIGH_", biz4AW2Sid, alg4AW2Sid)
  }

  def changeFormat4SKAWDF(SKAWDF:DataFrame):DataFrame = {
    val ss = spark
    import ss.implicits._
    SKAWDF.groupBy("searchKey")
      .agg(collect_list(concat_ws("_", col("associativeWord"), col("associativeWordScore"))).as("content"))
      .map(r => (r.getAs[String]("searchKey"), r.getAs[Seq[String]]("content").map(s => s.split("_"))
        .filter(s => s.length==2)
        .sortBy(s => -1 * s(1).toDouble).take(60)
        .map(s => s(0) + "_" + s(1)).mkString("|")))
      .toDF("searchKey", "value")
  }

  def changeFormat(df: DataFrame, risk: Int): DataFrame = {
    val ss = spark
    import ss.implicits._
    df.where("content is not null")
      .map(r => (r.getAs[String]("searchKey"), r.getAs[String]("contentType"),
        r.getAs[Seq[String]]("content")
          .map(s => s.split("_"))
          .filter(s => s.length == 4)
          .filter(s => (s(3).toInt + risk) <= 2)
          .sortBy(s => -1 * s(2).toDouble).take(25)
          .map(s => s(0) + "_" + s(1) + "_" + s(2))
          .mkString("|"))
      ).toDF("searchKey", "contentType", "content")
      .groupBy("searchKey").agg(collect_list(struct("contentType", "content")).as("value"))
      .repartition(20)
  }

  def changeFormat4AWSidDF(AWSidDF: DataFrame): DataFrame = {
    val ss = spark
    import ss.implicits._
    AWSidDF.groupBy("associativeWord", "contentType")
      .agg(collect_list(concat_ws("_", col("sid"), col("title"), col("score"))).as("content"))
      .where("content is not null")
      .map(r => (r.getAs[String]("associativeWord"), r.getAs[String]("contentType"),
        r.getAs[Seq[String]]("content")
          .map(s => s.split("_"))
          .filter(s => s.length == 3)
          .sortBy(s => -1 * s(2).toDouble).take(60)
          .map(s => s(0) + "_" + s(1) + "_" + s(2)).mkString("|"))
      ).toDF("associativeWord", "contentType", "content")
      .groupBy("associativeWord").agg(collect_list(struct("contentType", "content")).as("value"))
      .repartition(20)
  }

  //意为searchKeyAssociativeWordBeforeReorder
  /*
  def changeFormat4AssociativeWordDF(searchKeyAssociativeWordDF:DataFrame) :DataFrame = {
    val ss = spark
    import ss.implicits._
    searchKeyAssociativeWordDF.groupBy("searchKey", "associativeWord", "associativeWordScore", "contentType")
      .agg(collect_list(concat_ws("%", col("sid"), col("score"))).as("content"))
      .map(r => (r.getString(0), r.getString(1), r.getDouble(2), r.getString(3), r.getSeq[String](4).mkString("#")))
      .toDF("searchKey", "associativeWord", "associativeWordScore", "contentType", "content")
      .groupBy("searchKey", "contentType")
      .agg(collect_list(concat_ws("_", col("associativeWord"), col("associativeWordScore"), col("content"))).as("content"))
      .repartition(100).where("content is not null")
      .map(r => (r.getAs[String]("searchKey"), r.getAs[String]("contentType"),
        r.getAs[Seq[String]]("content").map(s => s.split("_"))
          .filter(s => s.length == 3)
          .sortBy(s => -1 * s(1).toDouble).take(60)
          .map(s => s(0) + "_" + s(1) + "_" + s(2)).mkString("|"))
      ).toDF("searchKey", "contentType", "content")
      .groupBy("searchKey").agg(collect_list(struct("contentType", "content")).as("value"))
      .repartition(20)
  }
  */


  /**
    * 用虚拟sid去重，结果保留真实sid
    * @param input DF("sid", "contentType", "title", "riskFlag")
    * @return
    */
  def deDuplicateByVirtualSid(input: DataFrame): DataFrame = {
    val sidRel = BizUtils.readVirtualSidRelationTencentFirst()
    val sidColumnName = "sid"
    input.as("a").join(sidRel.as("b"), expr(s"a.$sidColumnName = b.sid"), "leftouter").drop(expr("b.sid"))
      .withColumn("virtual_sid", expr(s"case when b.virtual_sid is not null then b.virtual_sid else a.$sidColumnName end"))
      .dropDuplicates("virtual_sid")
      .drop(expr("b.virtual_sid"))
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}

