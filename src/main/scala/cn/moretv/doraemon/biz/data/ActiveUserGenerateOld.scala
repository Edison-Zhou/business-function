package cn.moretv.doraemon.biz.data

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.biz.util.SendMailUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.{HdfsUtil, DateUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author cheng,huan on 2017/2/8，有部分计算为负数。待修改。
  *         * 以上两点影响范围：低
  *         update by wang.baozhi on 2019/1/14 下午2:42,使用doraemon的相应方法，还可以进一步优化。
  *
  *         迁移自cn.whaley.ai.recommend.ALS.frontpage.moretv.ActiveUserCalculate，hamlet project
  *
  *         依赖评分数据计算用户的日、周、月和季度操作
  *         每天增量更新
  *         用于筛选活跃用户
  *
  *         注意：代码待改进：
  * 1.在重跑的时候，/ai/data/dw/moretv/userBehavior/activeUser会变成累计计算。
  * 2.18年五月份停止后，补数据后
  *
  */
object ActiveUserGenerateOld extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  /**
    * 用于计算用户每天的操作(默认取用户昨天的操作)
    *
    * @param startDate        开始日期
    * @param endDate          结束日期
    * @param pathOfScoredData 视频评分的路径
    * @return RDD[(uid, operateNum)]
    */
  def dailyActive(
                  startDate: String = DateUtils.lastDayWithDelimiter,
                  endDate: String = DateUtils.todayWithDelimiter,
                  pathOfScoredData: String): RDD[(Long, Int)] = {
    // println(s"Calculating the activeUser on $startDate")
    println(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'")
    val userData = DataReader.read(new HdfsPath((pathOfScoredData)))
      .filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'")
      .rdd.map(r => (r.getLong(0), 1))
    userData.reduceByKey(_ + _)
  }

  /**
    * 用于更新活跃用户
    *
    * @param ss SparkSession
    * @param sc SparkContextSparkContext
    * @return 更新后的活跃用户属性
    */
  def dailyUpdate(ss: SparkSession, sc: SparkContext, i: Int): RDD[(Long, Int, Int, Int, Int, Int, Int, Int)] = {
    /**
      * 路径变量
      */
    val pathOfActiveUser = PathConstants.pathOfMoretvActiveUser
    val pathOfUserDailyOperate = PathConstants.pathOfMoretvUserDailyOperation

    /**
      * 日期变量
      */
    val lastDay = DateUtils.farthestDayWithOutDelimiter(91 - i)
    val weekFarthestDay = DateUtils.farthestDayWithOutDelimiter(98 - i)
    val monthFarthestDay = DateUtils.farthestDayWithOutDelimiter(121 - i)
    val seasonFarthestDay = DateUtils.farthestDayWithOutDelimiter(181 - i)

    println(s"Updating the activeUser on $lastDay")

    val pastActive = if (HdfsUtil.pathIsExist(pathOfActiveUser + "/Latest")) {
      ss.read.
        parquet(pathOfActiveUser + "/Latest").rdd.
        map(r => (r.getLong(0), (0, r.getInt(2), r.getInt(3), r.getInt(4), r.getInt(5), r.getInt(6), r.getInt(7))))
    } else {
      sc.parallelize(Array.empty[(Long, (Int, Int, Int, Int, Int, Int, Int))])
    }

    val lastDayOperation = if (HdfsUtil.pathIsExist(pathOfUserDailyOperate + lastDay)) {
      ss.read.
        parquet(pathOfUserDailyOperate + lastDay).rdd.
        map(r => (r.getLong(0), r.getInt(1))).map(e => (e._1, (e._2, e._2, e._2, e._2, 1, 1, 1)))
    } else {
      sc.parallelize(Array.empty[(Long, (Int, Int, Int, Int, Int, Int, Int))])
    }

    val weekFarthestOperation = if (HdfsUtil.pathIsExist(pathOfUserDailyOperate + weekFarthestDay)) {
      ss.read.
        parquet(pathOfUserDailyOperate + weekFarthestDay).rdd.
        map(r => (r.getLong(0), r.getInt(1))).map(e => (e._1, (0, -e._2, 0, 0, -1, 0, 0)))
    } else {
      sc.parallelize(Array.empty[(Long, (Int, Int, Int, Int, Int, Int, Int))])
    }

    val monthFarthestOperation = if (HdfsUtil.pathIsExist(pathOfUserDailyOperate + monthFarthestDay)) {
      ss.read.
        parquet(pathOfUserDailyOperate + monthFarthestDay).rdd.
        map(r => (r.getLong(0), r.getInt(1))).map(e => (e._1, (0, 0, -e._2, 0, 0, -1, 0)))
    } else {
      sc.parallelize(Array.empty[(Long, (Int, Int, Int, Int, Int, Int, Int))])
    }

    val seasonFarthestOperation = if (HdfsUtil.pathIsExist(pathOfUserDailyOperate + seasonFarthestDay)) {
      ss.read.
        parquet(pathOfUserDailyOperate + seasonFarthestDay).rdd.
        map(r => (r.getLong(0), r.getInt(1))).map(e => (e._1, (0, 0, 0, -e._2, 0, 0, -1)))
    } else {
      sc.parallelize(Array.empty[(Long, (Int, Int, Int, Int, Int, Int, Int))])
    }

    val unionActive = pastActive.
      union(lastDayOperation).
      union(weekFarthestOperation).
      union(monthFarthestOperation).
      union(seasonFarthestOperation).groupByKey().map(e => {
      val uid = e._1
      var lastDayOpNums = e._2.map(r => r._1).sum
      var weekOpNums = e._2.map(r => r._2).sum
      var monthOpNums = e._2.map(r => r._3).sum
      var seasonOpNums = e._2.map(r => r._4).sum
      var weekOpDays = e._2.map(r => r._5).sum
      var monthOpDays = e._2.map(r => r._6).sum
      var seasonOpDays = e._2.map(r => r._7).sum

      //负数值保护
      lastDayOpNums=if (lastDayOpNums>0) lastDayOpNums else 0
      weekOpNums=if (weekOpNums>0) weekOpNums else 0
      monthOpNums=if (monthOpNums>0) monthOpNums else 0
      seasonOpNums=if (seasonOpNums>0) seasonOpNums else 0
      weekOpDays=if (weekOpDays>0) weekOpDays else 0
      monthOpDays=if (monthOpDays>0) monthOpDays else 0
      seasonOpDays=if (seasonOpDays>0) seasonOpDays else 0

      (uid, lastDayOpNums, weekOpNums, monthOpNums, seasonOpNums, weekOpDays, monthOpDays, seasonOpDays)
    }).filter(e => e._5 != 0)

    unionActive
  }

 /** 一次性重跑90天的数据，需要将/ai/data/dw/moretv/userBehavior/activeUser目录清空*/
  def reCalculate(ss: SparkSession, sc: SparkContext): Unit = {
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._

    /**
      * 用户每日操作
      */
    for (i <- 0 until 90) {
      val startDate = DateUtils.farthestDayWithDelimiter(i + 2)
      val endDate = DateUtils.farthestDayWithDelimiter(i + 1)
      val userDailyOperation = dailyActive(startDate, endDate, PathConstants.pathOfMoretvLongVideoScore)

      val path = PathConstants.pathOfMoretvUserDailyOperation + DateUtils.farthestDayWithOutDelimiter(i + 1)
      HdfsUtil.deleteHDFSFileOrPath(path)
      userDailyOperation.toDF("uid", "operateNum").coalesce(2).write.parquet(path)
      println(s"startDate=$startDate,endDate=$endDate,path=$path")
    }

    /**
      * 增量计算部分
      */
    for (i <- 0 until 90) {
      val updatedActiveUser = dailyUpdate(ss, sc, i)
      val data = updatedActiveUser.toDF("uid", "lastDayOpNums", "weekOpNums", "monthOpNums", "seasonOpNums",
        "weekOpDays", "monthOpDays", "seasonOpDays")
      HdfsUtil.dataFrameUpdate2HDFS(data, PathConstants.pathOfMoretvActiveUser)
    }
  }

  override def execute(args: Array[String]): Unit = {
    try {
      val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
      import ss.implicits._

      /**
        * 用户昨日操作
        */
      val startDate = DateUtils.lastDayWithDelimiter
      val endDate = DateUtils.todayWithDelimiter
      val userDailyOperation = dailyActive(startDate, endDate, PathConstants.pathOfMoretvLongVideoScore)

      val path = PathConstants.pathOfMoretvUserDailyOperation + startDate.replace("-", "")
      HdfsUtil.deleteHDFSFileOrPath(path)
      userDailyOperation.toDF("uid", "operateNum").coalesce(2).write.parquet(path)
      println(s"startDate=$startDate,endDate=$endDate,path=$path")

      /**
        * 增量计算部分
        */
//      val updatedActiveUser = dailyUpdate(ss, sc, 90)
      reCalculate(ss, sc)
//      val data = updatedActiveUser.toDF("uid", "lastDayOpNums", "weekOpNums", "monthOpNums", "seasonOpNums",
//        "weekOpDays", "monthOpDays", "seasonOpDays")
//      data.printSchema()
//      data.show()
//      HdfsUtil.dataFrameUpdate2HDFS(data, PathConstants.pathOfMoretvActiveUser)
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

