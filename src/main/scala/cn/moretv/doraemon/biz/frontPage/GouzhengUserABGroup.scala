package cn.moretv.doraemon.biz.frontPage

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.util.DateUtils
import org.apache.spark.sql.functions.collect_list
import cn.moretv.doraemon.common.path.MysqlPath
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions._


/**
  * Created by Edison_Zhou on 2019/5/28
  */
object GouzhengUserABGroup extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._
    TransformUDF.registerUDFSS

    val GZUserIDDF = DataReader.read(BizUtils.getHdfsPathForRead("gouzheng/userID"))
      .map(r => (r.getString(0), TransformUDF.calcLongUserId(r.getString(0)))).toDF("userID", "uid")
    BizUtils.getDataFrameInfo(GZUserIDDF, "GZUserIDDF")

    val homePageVipDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/vip"))
    val homePageGouzhengDF = DataReader.read(BizUtils.getHdfsPathForRead("homePage/gouzheng")).toDF("uid", "id")

    val gouzhengSortedUser = homePageGouzhengDF.select("uid").map(r => r.getLong(0)).collect().sorted
    val gouzhengUserNum = gouzhengSortedUser.length
    val gouzhengUserWithGZAlg = gouzhengSortedUser.take(gouzhengUserNum/2)
    val gouzhengUserWithALS = gouzhengSortedUser.take(gouzhengUserNum-gouzhengUserNum/2)

    val gouzhengWithGZAlgDF = sc.parallelize(gouzhengUserWithGZAlg).toDF("gouzhengUserWithGZAlg").
      selectExpr("cast(gouzhengUserWithGZAlg as string) as uid")
    val gouzhengWithALSDF = sc.parallelize(gouzhengUserWithALS).toDF("gouzhengUserWithALS").
      selectExpr("cast(gouzhengUserWithALS as string) as uid")

    val vipFoldDF = homePageVipDF.groupBy("uid").agg(collect_list("sid")).toDF("uid", "id").
      selectExpr("cast(uid as string) as uid", "id")
    BizUtils.getDataFrameInfo(vipFoldDF, "vipFoldDF")

    val ALSNewUserDF = vipFoldDF.join(gouzhengWithALSDF, "uid").select("uid")
      .join(GZUserIDDF, "uid").select("userID").withColumn("date", expr(DateUtils.todayWithOutDelimiter))
    BizUtils.getDataFrameInfo(ALSNewUserDF, "ALSNewUserDF")
    BizUtils.outputWrite(ALSNewUserDF, s"gouzheng/ALSNewuser/${DateUtils.todayWithOutDelimiter}")

    val gouzhengFoldDF = homePageGouzhengDF.selectExpr("cast(uid as string) as uid", "id")
    val GZAlgUserDF = gouzhengFoldDF.join(gouzhengWithGZAlgDF, "uid").select("uid")
      .join(GZUserIDDF, "uid").select("userID").withColumn("date", expr(DateUtils.todayWithOutDelimiter))
    BizUtils.getDataFrameInfo(GZAlgUserDF, "GZAlgUserDF")
    BizUtils.outputWrite(GZAlgUserDF, s"gouzheng/GZAlgUser/${DateUtils.todayWithOutDelimiter}")

  }
}
