package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HivePath, MysqlPath, HdfsPath}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * 针对勾正给出的用户画像数据进行个性化推荐
  * Created by Edison_Zhou on 2019/5/8.
  */
object GouZhengRec extends BaseClass {
  def execute(args: Array[String]): Unit = {

    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    TransformUDF.registerUDFSS

    val beforeTypeMatchedData = DataReader.read(new HivePath("select * from test.gouzheng_play_top5_new"))
      .map(r => (r.getString(0).replaceAll(":", ""), r.getString(1).substring(1, r.getString(1).length-1)))
      .map(r => (r._1, r._2.split(", "))).flatMap(r => r._2.map(e => (r._1, e.substring(1, e.length-1))))
      .map(r => (r._1, r._2.substring(0, r._2.indexOf("|")), r._2.substring(r._2.indexOf("|")+1)))

    val macTop5ProgramNewDF = typeMatching(beforeTypeMatchedData)
      .toDF("mac", "contentType", "title")
    BizUtils.getDataFrameInfo(macTop5ProgramNewDF, "macTop5ProgramNewDF")

    val md5macAndTop5PlayData = DataReader.read(new HivePath("select * from test.gouzheng_play_top5_extra"))
      .map(r => (r.getString(0), r.getString(1).replaceAll(" '", "").replaceAll("'", "")))
      .map(r => (r._1, r._2.split(","))).flatMap(r => r._2.map(e =>{
      // 经反复验证，部分数据无"|"，导致直接对字符串切片操作报出下标越界的异常，在此添加一个判断逻辑
      if (e.contains("|"))
        (r._1, e.substring(0, e.indexOf("|")), e.substring(e.indexOf("|")+1))
      else
        (r._1, "null", e)
    }))

    val macTop5ProgramExtraDF = typeMatching(md5macAndTop5PlayData)
      .toDF("mac_md5", "contentType", "title")
    BizUtils.getDataFrameInfo(macTop5ProgramExtraDF, "macTop5ProgramExtraDF")

    /*
    val macPersonAgeDF = DataReader.read(new HivePath("select mac, age from test.gouzheng_person_base_info"))
      .map(r => (r.getString(0).replaceAll(":", ""), r.getString(1)))
      .map(e => (e._1, e._2.substring(0, e._2.indexOf("岁"))))
      .map(e => (e._1, e._2.substring(e._2.length-2, e._2.length))).toDF("mac", "personAge")
    BizUtils.getDataFrameInfo(macPersonAgeDF, "macPersonAgeDF")
     */

    val macUseridMapDf = DataReader.read(new HivePath("select user_id, mac from test.gouzheng_mac_userid_map"))
      .where("length(mac) = 12")

    val md5macUseridMapDf = DataReader.read(new HivePath("select user_id, mac_md5 from test.gouzheng_md5mac_userid_map"))
      .where("length(mac_md5) = 32")

//    val useridProgramDF = macTop5ProgramNewDF.join(macUseridMapDf, "mac")
//    BizUtils.getDataFrameInfo(useridProgramDF, "useridProgramDF")

    val validVideoInfo = DataReader.read(new MysqlPath("bigdata-appsvr-130-4",3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid", "title", "contentType"), "status = 1"))

    val matchedInfoDF = macTop5ProgramNewDF.join(validVideoInfo, Seq("title", "contentType")).join(macUseridMapDf, "mac")
    val matchedExtraInfoDF = macTop5ProgramExtraDF.join(validVideoInfo, Seq("title", "contentType"))
      .join(md5macUseridMapDf, "mac_md5")

    val userItemDF = matchedInfoDF.select("user_id", "sid")
    val userItemExtraDF = matchedExtraInfoDF.select("user_id", "sid")
    val fullUserItemDF = userItemDF.union(userItemExtraDF).toDF("uid", "sid")
    BizUtils.getDataFrameInfo(fullUserItemDF, "fullUserItemDF")

    val userIdByGouZhengDF = fullUserItemDF.select("uid").distinct().toDF("userId")
    BizUtils.getDataFrameInfo(userIdByGouZhengDF, "userIdByGouZhengDF")
    BizUtils.outputWrite(userIdByGouZhengDF, "gouzheng/userID")

    var fullProgramSimilarRes = DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/similarMixVip/movie/Latest"))
    val otherContentTypeList = List("tv", "kids", "comic", "zongyi", "jilu")
    otherContentTypeList.foreach(contentType =>
      fullProgramSimilarRes = fullProgramSimilarRes.union(
        DataReader.read(new HdfsPath("/ai/tmp/output/pre/medusa/similarMixVip/" + contentType + "/Latest")))
    )
    BizUtils.getDataFrameInfo(fullProgramSimilarRes, "fullProgramSimilarRes")
    val similarProgramArrDF = fullProgramSimilarRes.rdd.map(r => (r.getString(0), r.getString(1))).groupByKey()
      .map(r => (r._1, r._2.toArray)).toDF("sid", "itemArr")
    BizUtils.getDataFrameInfo(similarProgramArrDF, "similarProgramArrDF")

    val recommend4GouzhengUser = fullUserItemDF.join(similarProgramArrDF, "sid")
      .select("uid", "itemArr").rdd.map(r => (r.getString(0), r.getSeq[String](1)))
      // dataframe中的列元素如果出现Array，获取时需使用getSeq方法而不能用getAs[Array]，否则在后续使用toArray时会报错
      .flatMap(r => r._2.map(e => (r._1, e))).groupByKey()
      .map(r => (TransformUDF.calcLongUserId(r._1), r._2.toArray)).toDF("uid", "recommendItems")
    BizUtils.getDataFrameInfo(recommend4GouzhengUser, "recommend4GouzhengUser")

    BizUtils.outputWrite(recommend4GouzhengUser, "homePage/gouzheng")

  }

  protected def typeMatching(gouzhengDF: Dataset[(String, String, String)]) = {
    //在execute函数体外解决隐式调用的两种方法：一、转换为rdd；二、import sc.implicits._
    gouzhengDF.rdd.map(r => (r._1, r._2.replaceAll("电视剧","tv"), r._3))
      .map(r => (r._1, r._2.replaceAll("电影","movie"), r._3))
      .map(r => (r._1, r._2.replaceAll("综艺","zongyi"), r._3))
      .map(r => (r._1, r._2.replaceAll("科教","jilu"), r._3))
      .map(r => (r._1, r._2.replaceAll("教育","jilu"), r._3))
      .map(r => (r._1, r._2.replaceAll("旅游","jilu"), r._3))
      .map(r => (r._1, r._2.replaceAll("纪录片","jilu"), r._3))
      .map(r => (r._1, r._2.replaceAll("少儿","kids"), r._3))
      .map(r => (r._1, r._2.replaceAll("动漫","comic"), r._3))
      .map(r => (r._1, r._2.replaceAll("体育","sports"), r._3))
      .map(r => (r._1, r._2.replaceAll("娱乐","yule"), r._3))
      .map(r => (r._1, r._2.replaceAll("游戏","game"), r._3))
  }


  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
