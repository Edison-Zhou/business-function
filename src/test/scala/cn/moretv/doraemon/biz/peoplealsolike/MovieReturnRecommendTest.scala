package cn.moretv.doraemon.biz.peoplealsolike

import cn.moretv.doraemon.algorithm.als.AlsModel
import cn.moretv.doraemon.algorithm.similar.vector.{SimilarVectorAlgorithm, SimilarVectorModel, SimilarVectorParameters}
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{EnvEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{DateRange, HdfsPath}
import cn.moretv.doraemon.common.util.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.junit.{Before, Test}

/**
  * Created by guohao on 2018/9/30.
  */
class MovieReturnRecommendTest {
  //服务器运行
  val config = new SparkConf()

  //本地测试需要指定，服务器中已经定义
  config.set("spark.sql.crossJoin.enabled", "true")
  config.setMaster("local[2]")
  /**
    * define some parameters
    */
  implicit var spark: SparkSession = null
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null

  implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

  implicit val env: EnvEnum.Value = EnvEnum.DEV

  @Before
  def init(): Unit = {
    spark = SparkSession.builder()
      .config(config)
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  @Test
  def test(): Unit ={

    val alsModel = new AlsModel()
    //需要计算movie的model
    //alsModel.load("movie")
    alsModel.load()
    val itemFactorRaw = alsModel.matrixV
    itemFactorRaw.show(2)
    itemFactorRaw.printSchema()
  }


  //计算相似度
  def matrixFactorizationForItem(itemFactorRaw:DataFrame): Unit ={
    //相似度计算
    val similarAlg: SimilarVectorAlgorithm = new SimilarVectorAlgorithm()
    val similarPara: SimilarVectorParameters = similarAlg.getParameters.asInstanceOf[SimilarVectorParameters]
    similarPara.isSparse = false
    similarPara.topN = 60
    val similarDataMap = Map(similarAlg.INPUT_DATA_KEY -> itemFactorRaw)
    similarAlg.initInputData(similarDataMap)
    similarAlg.run()
    similarAlg.getOutputModel.output("similarAls/movie")
//    similarAlg.getOutputModel.asInstanceOf[SimilarVectorModel].matrixData
  }
  @Test
  def test3(): Unit ={
    val numDaysOfData = new DateRange("yyyyMMdd",2)
    val hotRankingDF = DataReader.read(new HdfsPath(numDaysOfData, PathConstants.pathOfMoretvLongVideoScoreByDay))
    hotRankingDF.show()

  }

  @Test
  def test4(): Unit ={
    val arr = Array(("aa",2.222,5.2222),("bb",3.222,8.2222),("cc",4.222,3.2222),("dd",4.222,11.2222))
    arr.foreach(println(_))
    println("--------")
    val arr2 = arr.sortBy(f => -f._3).take(2)
    arr2.foreach(println(_))
  }


}
