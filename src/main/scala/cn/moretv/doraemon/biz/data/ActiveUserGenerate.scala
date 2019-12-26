package cn.moretv.doraemon.biz.data

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.{BizUtils, SendMailUtils}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.{DateUtils, HdfsUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs

/**
  * @author
  * Edison_Zhou on 2019/7/16
  *
  * @note
  * 1.可以看做是机房扩容后的对于活跃用户计算更为准确的新方案
  * 2.目前会有重复计算的问题，可作为日后的优化点
  *
  *
  * 依赖评分数据计算90天内用户的日、周、月和季度操作
  * 用于筛选活跃用户
  *
  */
object ActiveUserGenerate extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa
  val lastDay = DateUtils.lastDayWithOutDelimiter
  val weekFartherestDay = DateUtils.farthestDayWithOutDelimiter(7)
  val monthFartherestDay = DateUtils.farthestDayWithOutDelimiter(30)
  val seasonFartherestDay = DateUtils.farthestDayWithOutDelimiter(90)

  /**
    * 用于计算用户每天的操作(默认取用户昨天的操作)
    *
    * @param startDate        开始日期
    * @param endDate          结束日期
    * @param pathOfScoredData 视频评分的路径
    * @return RDD[(uid, operateNum)]
    */
  def dailyActive( startDate: String = DateUtils.lastDayWithDelimiter,
                   endDate: String = DateUtils.todayWithDelimiter,
                   pathOfScoredData: String): RDD[(Long, Int)] = {
    // println(s"Calculating the activeUser on $startDate")
    println(s"latest_optime >= '$startDate' and latest_optime < '$endDate'")
    val userData = DataReader.read(new HdfsPath((pathOfScoredData)))
      .filter(s"latest_optime >= '$startDate' and latest_optime < '$endDate'")
      .rdd.map(r => (r.getLong(0), 1))
    userData.reduceByKey(_ + _)
  }

  override def execute(args: Array[String]): Unit = {
    try {
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._
      /**
        * 用户近90天的每日操作
        */
      for (i <- 0 to 90) {
        val startDate = DateUtils.farthestDayWithDelimiter(i + 1)
        val endDate = DateUtils.farthestDayWithDelimiter(i)
        val userDailyOperation = dailyActive(startDate, endDate, PathConstants.pathOfMoretvLongVideoScore)

        val path = PathConstants.pathOfMoretvUserDailyOperation + DateUtils.farthestDayWithOutDelimiter(i + 1)
        HdfsUtil.deleteHDFSFileOrPath(path)
        userDailyOperation.toDF("uid", "operateNum").coalesce(2).write.parquet(path)
        println(s"startDate=$startDate,endDate=$endDate,path=$path")
      }

      val lastDayOperation = if (HdfsUtil.pathIsExist(PathConstants.pathOfMoretvUserDailyOperation + lastDay)) {
        ss.read.parquet(PathConstants.pathOfMoretvUserDailyOperation + lastDay).rdd.
          map(r => (r.getLong(0), r.getInt(1), 1))
      } else {
        sc.parallelize(Array.empty[(Long, Int, Int)])
      }
      val userOperationDaily = lastDayOperation.map(e => (e._1, e._2)).toDF("uid", "lastDayOpNums")
      BizUtils.getDataFrameInfo(userOperationDaily, "userOperationDaily")

      //统计用户一周之内的操作情况
      var operationWithinAWeek = lastDayOperation
      for (dateRange <- 2 to 7) {
        val path = PathConstants.pathOfMoretvUserDailyOperation + DateUtils.farthestDayWithOutDelimiter(dateRange)
        val userOpNumsNDays = if (HdfsUtil.pathIsExist(path)) {
          DataReader.read(new HdfsPath(path)).rdd.map(e => (e.getLong(0), e.getInt(1), 1))
        } else {
          sc.parallelize(Array.empty[(Long, Int, Int)])
        }
        operationWithinAWeek = operationWithinAWeek.union(userOpNumsNDays)
      }
      val userOperationWeekly = operationWithinAWeek.toDF("uid", "opNum", "opDay").groupBy("uid")
        .sum("opNum", "opDay").toDF("uid", "weekOpNums", "weekOpDays")
      BizUtils.getDataFrameInfo(userOperationWeekly, "userOperationWeekly")

      //统计用户一月之内的操作情况
      var operationWithinAMonth = operationWithinAWeek
      for (dateRange <- 8 to 30) {
        val path = PathConstants.pathOfMoretvUserDailyOperation + DateUtils.farthestDayWithOutDelimiter(dateRange)
        val userOpNumsNDays = if (HdfsUtil.pathIsExist(path)) {
          DataReader.read(new HdfsPath(path)).rdd.map(e => (e.getLong(0), e.getInt(1), 1))
        } else {
          sc.parallelize(Array.empty[(Long, Int, Int)])
        }
        operationWithinAMonth = operationWithinAMonth.union(userOpNumsNDays)
      }
      val userOperationMonthly = operationWithinAMonth.toDF("uid", "opNum", "opDay").groupBy("uid")
        .sum("opNum", "opDay").toDF("uid", "monthOpNums", "monthOpDays")
      BizUtils.getDataFrameInfo(userOperationMonthly, "userOperationMonthly")

      //统计用户一季度之内的操作情况
      var operationWithinASeason = operationWithinAMonth
      for (dateRange <- 31 to 90) {
        val path = PathConstants.pathOfMoretvUserDailyOperation + DateUtils.farthestDayWithOutDelimiter(dateRange)
        val userOpNumsNDays = if (HdfsUtil.pathIsExist(path)) {
          DataReader.read(new HdfsPath(path)).rdd.map(e => (e.getLong(0), e.getInt(1), 1))
        } else {
          sc.parallelize(Array.empty[(Long, Int, Int)])
        }
        operationWithinASeason = operationWithinASeason.union(userOpNumsNDays)
      }
      val userOperationSeasonly = operationWithinASeason.toDF("uid", "opNum", "opDay").groupBy("uid")
        .sum("opNum", "opDay").toDF("uid", "seasonOpNums", "seasonOpDays")
      BizUtils.getDataFrameInfo(userOperationSeasonly, "userOperationMonthly")

      //合并活跃用户一季的操作情况，并对缺失值做0填充
      val activeUserOpRes = userOperationSeasonly.as("a").join(userOperationMonthly.as("b"), expr("a.uid = b.uid")
      , "full").join(userOperationWeekly.as("c"), expr("a.uid=c.uid"), "full")
        .join(userOperationDaily.as("d"), expr("a.uid=d.uid"), "full").selectExpr("a.uid as uid",
        "case when d.lastDayOpNums is null then 0 else d.lastDayOpNums end as lastDayOpNums",
        "case when c.weekOpNums is null then 0 else c.weekOpNums end as weekOpNums",
        "case when b.monthOpNums is null then 0 else b.monthOpNums end as monthOpNums",
        "a.seasonOpNums as seasonOpNums",
        "case when c.weekOpDays is null then 0 else c.weekOpDays end as weekOpDays",
        "case when b.monthOpDays is null then 0 else b.monthOpDays end as monthOpDays",
        "a.seasonOpDays as seasonOpDays"
      ).selectExpr("uid", "lastDayOpNums",
        "cast (weekOpNums as integer) as weekOpNums",
        "cast (monthOpNums as integer) as monthOpNums",
        "cast (seasonOpNums as integer) as seasonOpNums",
        "cast (weekOpDays as integer) as weekOpDays",
        "cast (monthOpDays as integer) as monthOpDays",
        "cast (seasonOpDays as integer) as seasonOpDays"
      )

      println("activeUserOpRes.count():" + activeUserOpRes.count())
      println("activeUserOpRes.printSchema:")
      activeUserOpRes.printSchema()
      activeUserOpRes.show(100, false)

      new DataWriter2Hdfs().write(activeUserOpRes, new HdfsPath(PathConstants.pathOfMoretvActiveUser))

    } catch {
      case e: Exception => {
        val to=Array[String]("app-bigdata_group@moretv.com.cn")
        val subject="ActiveUserGenerate Error"
        val content=e.getMessage
        SendMailUtils.sendMailToPerson(subject,content,to)
        throw e
      }
    }
  }
}

