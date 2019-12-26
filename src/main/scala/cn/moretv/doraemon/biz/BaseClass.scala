package cn.moretv.doraemon.biz

import cn.moretv.doraemon.biz.params.BaseParams
import cn.moretv.doraemon.common.enum.{EnvEnum, ProductLineEnum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by lituo on 2018/6/13.
  */
trait BaseClass {


   //.setMaster("local") 是本地运行
  //val config = new SparkConf().setMaster("local")

  //服务器运行
  val config = new SparkConf()

  //本地测试需要指定，服务器中已经定义
  config.set("spark.sql.crossJoin.enabled", "true")

  /**
    * define some parameters
    */
  implicit var spark: SparkSession = null
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null

  implicit val productLine: ProductLineEnum.Value

  implicit var env: EnvEnum.Value = _
  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]) {

    val param = BaseParams.getParams(args)

    val strEnv = param.env
    if(strEnv == null || strEnv.isEmpty) {
      throw new IllegalArgumentException("未设置环境参数")
    }
    env = EnvEnum.withName(strEnv.toUpperCase)
    println("当前环境为：" + strEnv.toUpperCase)

//    val strProductLine = param.productLine
//    if(strProductLine != null && !strProductLine.isEmpty) {
//      productLine = ProductLineEnum.withName(strProductLine.toLowerCase)
//      println("当前产品线为：" + strProductLine.toLowerCase)
//    }
    if("TEST".equals(strEnv.toUpperCase.trim)){
      config.set("spark.yarn.queue", "test")
    }

    System.out.println("init start ....")
    init()
    System.out.println("init success ....")
    execute(args)


  }

  /**
    * 全局变量初始化
    */
  def init(): Unit = {
    spark = SparkSession.builder()
      .config(config)
      .enableHiveSupport()
      .getOrCreate()

    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  def execute(args: Array[String]): Unit
}
