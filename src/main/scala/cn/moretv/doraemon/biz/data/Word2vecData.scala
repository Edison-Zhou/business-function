package cn.moretv.doraemon.biz.data

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.constant.PathConstants
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.common.util.DateUtils
import org.apache.spark.ml.feature.Word2Vec

/**
  *
  * @author wang.baozhi 
  * @since 2019/4/23 下午3:57 
  */
object Word2vecData extends BaseClass{
  override def execute(args: Array[String]): Unit = {
    // 用于获取评分数据
    import org.apache.spark.sql.functions._

    //val userScore = BizUtils.readUserScore(PathConstants.pathOfMoretvLongVideoScore,20)

    val endDate = DateUtils.todayWithDelimiter
    val startDate = DateUtils.farthestDayWithDelimiter(20)
    val aa1 = DataReader.read(new HdfsPath(PathConstants.pathOfMoretvLongVideoScore))
    val userScore = aa1.filter(s"latest_optime >= '$startDate' and latest_optime <= '$endDate'")

    userScore.printSchema()
    userScore.show(false)
    val df1=userScore.selectExpr("userid as uid","sid_or_subject_code as sid","latest_optime as orderNum")
    val df2=df1.orderBy("uid","orderNum").groupBy("uid").agg(collect_list("sid")).toDF("uid","sids")
    df2.printSchema()
    println("------------df2")
    df2.show(false)

    val sourceDf=df2.select("sids")
    sourceDf.printSchema()
    println("------------sourceDf")
    sourceDf.show(false)

    val word2Vec = new Word2Vec()
      .setInputCol("sids")
      .setOutputCol("result")
      .setVectorSize(128)
      .setMinCount(0)
    val model = word2Vec.fit(sourceDf)
    //各个词的向量
    val vecs = model.getVectors
    vecs.printSchema()
    println("------------vecs")
    vecs.show(false)

    println("------------展示推荐结果")
    vecs.take(10).foreach(e => {
      println("============"+e.getString(0))
      val recommendResult = model.findSynonyms(e.getString(0),10)
      recommendResult.show(false)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
