package cn.moretv.doraemon.biz.params

import scopt.OptionParser

/**
  *
  * @author wang.baozhi
  * @since 2018/9/26 上午11:14
  * 参数说明：
  *       numDaysPlay：                 活跃用户天数
  *       numRecommendations：          推荐节目数（默认72个）
  *       filterTopN：                  迭代过程中得到节目数
  *       ifAbtest：                    是否进行ab测试（默认false）
  *       businessLine：                业务线（moretv和helios）
  *       contentType：                 节目类型（默认movie）
  *       kafkaTopic：                  写入的kafka topic
  *       alg：                         算法标识
  *
  *
  */

protected[biz] object Parameters {

  lazy val parser = new OptionParser[GuessYouLikeParam]("biz") {
    override def errorOnUnknownArgument = false
    head("biz")
    opt[Int]("numDaysPlay").
      action((x, c) => c.copy(numDaysPlay = x)).
      text("number of days of log for training")
    opt[Int]("numRecommendations").
      action((x, c) => c.copy(numRecommendations = x)).
      text("number of result for recommend")
    opt[Int]("filterTopN").
      action((x, c) => c.copy(filterTopN = x)).
      text("top n random result")
    opt[Boolean]("ifAbtest").
      action((x, c) => c.copy(ifAbTest = x)).
      text("ifAbtest")
    opt[String]("businessLine").
      action((x, c) => c.copy(businessLine = x)).
      text("businessLine")
    opt[String]("contentType").
      action((x, c) => c.copy(contentType = x)).
      text("contentType")
    opt[String]("kafkaTopic").
      action((x, c) => c.copy(kafkaTopic = x)).
      text("kafkaTopic")
    opt[String]("alg").
      action((x, c) => c.copy(alg = x)).
      text("alg")
  }

  def getParams(args: Array[String]): GuessYouLikeParam = {
    var params: GuessYouLikeParam = null
    parser.parse(args, GuessYouLikeParam()) match {
      case Some(param) => {
        params = param
        params
      }
      case None => {
        println("ERROR: wrong input parameters")
        System.exit(1)
        params
      }
    }
  }
}

protected[biz] case class GuessYouLikeParam(
                                             numDaysPlay:Int = 90,
                                             numRecommendations:Int = 72,
                                             filterTopN:Int = 50,
                                             ifAbTest:Boolean = true,
                                             businessLine: String = "moretv",
                                             contentType: String = "movie",
                                             kafkaTopic: String = "couchbase-moretv",
                                             alg: String = ""
                                           )
