package cn.moretv.doraemon.biz.detail

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.algorithm.reorder2Replace.{Reorder2ReplaceAlgorithm, Reorder2ReplaceParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by guohao on 2019/01/05.
  */
object DetailHotRecommend extends BaseClass {
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa


  override def execute(args: Array[String]): Unit = {
    val ss = spark
    var date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(),-1))
    //获取评分节目
    var scoreSid = BizUtils.getSearchVideoData(ss,date)

    if(scoreSid.count() == 0){
      date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(),-2))
      scoreSid = BizUtils.getSearchVideoData(ss,date)
    }
    scoreSid.persist(StorageLevel.MEMORY_AND_DISK)

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(f = contentType => {
      //获取流行节目top120
      val scoreSidTopN = scoreSid.select(expr("sid"),expr("video_score"),expr("content_type")).where(s"content_type ='$contentType' ").select(expr("sid"),expr("video_score"))
      //转换为虚拟id
      val virSidResult = BizUtils.transferToVirtualSid(scoreSidTopN, "sid").orderBy(expr("video_score").desc).limit(150)
      var map:Map[String,Double] = Map()
      virSidResult.collect().foreach(row=>{
        val sid = row.getString(0)
        val score = row.getDouble(1)
        map += (sid->score)
      })

      //获取相似结果数据，similarMix 已经转换为虚拟sid
      val similarMix = DataReader.read(BizUtils.getHdfsPathForRead("similarMix/" + contentType))
          .select(expr("sid"),expr("item"),(expr("similarity")+10).as("score"))
      val algorithm:Reorder2ReplaceAlgorithm = new Reorder2ReplaceAlgorithm()
      val parameters = algorithm.getParameters.asInstanceOf[Reorder2ReplaceParameters]
      parameters.replaceSid = map
      algorithm.initInputData(
        Map(algorithm.INPUT_SOURCE -> similarMix)
      )
      algorithm.run()
      //输出到HDFS
      algorithm.getOutputModel.output("hot/" + contentType)

    })

  }


}
