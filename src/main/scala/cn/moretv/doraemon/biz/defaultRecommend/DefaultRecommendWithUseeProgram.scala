package cn.moretv.doraemon.biz.defaultRecommend

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.{BizUtils, ConfigUtil}
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FormatTypeEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{CouchbasePath, HdfsPath, RedisPath}
import cn.moretv.doraemon.common.util.ArrayUtils
import cn.moretv.doraemon.data.writer.{DataPack, DataPackParam, DataWriter2Kafka}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


/**
  * Created by Edison_Zhou on 2019/8/26.
  */
object DefaultRecommendWithUseeProgram extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    /**
      * 数据源: 媒资现网数据库
      * 返回节目数量：60（电影15电视剧15综艺10少儿10动漫10）
      */
    val ss: SparkSession = SparkSession.builder().config(new SparkConf()).enableHiveSupport().getOrCreate()
    import ss.implicits._
    //初始化待推荐的优视猫节目array
    var useeProgramRecArr = Array.empty[String]
    //初始化类型及数量的List
    val typeNumList = List(("movie", 15), ("tv", 15), ("zongyi", 10), ("kids", 10), ("comic", 10))
    typeNumList.foreach({ e =>
      val program_type = e._1
      val num = e._2
      val programByTypeArr = DataReader.read(BizUtils.getUsee4SearchMysqlPath("pre"))
        .filter(s"program_type = '$program_type'").map(r => r.getAs[String]("sid")).collect()
      val recByType = ArrayUtils.randomTake(programByTypeArr, num)
      useeProgramRecArr = useeProgramRecArr ++: recByType
    })

    val dataWriter = new DataWriter2Kafka

    val useeProgramDefaultDF = useeProgramRecArr.toList.map(e => ("default", e)).toDF("uid", "sid")
    BizUtils.getDataFrameInfo(useeProgramDefaultDF, "useeProgramDefaultDF")


    BizUtils.defaultRecommend2Kafka4Couchbase("p:u:", useeProgramRecArr, "random",
      ConfigUtil.get("couchbase.moretv.topic"), 60)

  }

}
