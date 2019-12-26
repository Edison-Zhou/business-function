package cn.moretv.doraemon.biz.search

import java.text.SimpleDateFormat
import java.util.Date

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.data.writer.{KafkaConnector, KafkaSourceConnector}
import org.apache.commons.lang3.time.DateUtils

/**
  * Created by cheng_huan on 2019/4/15.
  */
object SearchScore2CMS extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))

    val videoScore = sqlContext.sql(s"select sid, video_score from dws_medusa_bi.medusa_video_score where day_p = '$date'")

    val kafkaConnector: KafkaConnector = new KafkaSourceConnector

    kafkaConnector.write2Kafka(videoScore.toJSON, "search-score-to-cms")
  }

}
