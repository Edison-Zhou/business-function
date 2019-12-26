package cn.moretv.doraemon.biz.defaultRecommend

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HdfsPath, Path, RedisPath}
import cn.moretv.doraemon.common.util.{ArrayUtils, DateUtils}
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by Edison_Zhou on 2019/3/22.
  * 因公司希望提升新用户留存所提出的新业务需求。算法逻辑为基于近三天中，每天的新增用户的观看记录，
  * 统计出针对新用户的热门播放影片作为推荐，与大盘原本的默认推荐（基于全量用户半月内播放影片的总次数筛出热门影片推荐）有一定差异。
  * 经一段时间的AB测试后，发现指标并无明显变化，后续可能废弃
  */
@deprecated
object DefaultRecommend4NewUser extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {


    //数据、最热榜
    val hotLongVideo = BizUtils.getHotRankingList4NewUser("all", 3, 100)


    /*兴趣推荐
    方案：长视频最热榜*/
    val interestDefault = hotLongVideo.take(60)
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    val param = new DataPackParam
    param.format = FormatTypeEnum.KV
    param.keyPrefix = "p:i:"
    param.extraValueMap = Map("date" -> DateUtils.getTimeStamp, "alg" -> "hot4NewUser")
    val path: Path = CouchbasePath(ConfigUtil.get("couchbase.moretv.topic"))
    val dataWriter: DataWriter = new DataWriter2Kafka
    val list = List(("default2", interestDefault))
    val dataSource = DataPack.pack(list.toDF("default2", "id"), param)
    dataSource.collect().foreach(println)
    dataWriter.write(dataSource, path)

  }

}
