package cn.moretv.doraemon.biz.params

import scopt.OptionParser

/**
  *
  * @author wang.baozhi 
  * @since 2018/9/27 下午3:48 
  */
object KMeansParams {

  lazy val parser = new OptionParser[KMeansParams]("kMeans"){
    override def errorOnUnknownArgument = false
    opt[String]("businessLine").
      action((x, c) => c.copy(businessLine = x))
    opt[String]("videoType").
      action((x, c) => c.copy(videoType = x))
    opt[Int]("kNum").
      action((x, c) => c.copy(kNum = x))
    opt[Int]("numIter").
      action((x, c) => c.copy(numIter = x))
    opt[Int]("numOfDays").
      action((x, c) => c.copy(numOfDays = x))
    opt[Double]("score").
      action((x, c) => c.copy(score = x))
    opt[String]("kafkaTopic").
      action((x, c) => c.copy(kafkaTopic = x))
    opt[Boolean]("ifABTest").
      action((x, c) => c.copy(ifABTest = x))
    opt[String]("alg").
      action((x, c) => c.copy(alg = x))
    opt[Int]("offset").
      action((x, c) => c.copy(offset = x))
    opt[String]("userGroup").
      action((x, c) => c.copy(userGroup = x))
    opt[String]("code").
      action((x, c) => c.copy(code = x))
    opt[Int]("codeType").
      action((x, c) => c.copy(codeType = x))
    opt[String]("videoTypePrefix").
      action((x, c) => c.copy(videoTypePrefix = x))
  }

  def getParams(args: Array[String]): KMeansParams = {
    var params: KMeansParams = null
    parser.parse(args, KMeansParams()) match {
      case Some(param) =>
        params = param
        params
      case None => {
        println("ERROR: wrong input parameters")
        System.exit(1)
        params
      }
    }
  }

}
/**
  * 此类用于提供重排序模型的参数输入
  *
  * 类输入参数：
  *           businessLine:   业务线（helios or medusa）
  *           videoType:      视频类型
  *           kNum:           kMeans聚类中心点
  *           numIter:        kMeans模型迭代次数
  *           numOfDays:      用户play日志所需的天数
  *           score:          用户观看评分选择
  *           kafkaTopic:     kafka写入的topic
  *           ifABTest:       确认是否进行AB测试（默认：false）
  *           alg:            如果进行ab测试时的，alg标识
  *           offset:         如果进行ab测试时的，分组偏移量（默认0）
  *           userGroup:      用户分组前缀
  *           code:           为当前站点树编码，犀利动作的code为常量 1_movie_tag_dongzuo,
  *           codeType：      当前站点树的类别，为常量（0，1，3表示站点树， 4表示编排）
  *           videoTypePrefix:类型前缀
  *
*/
case class KMeansParams(
                         businessLine:String = "moretv",
                         videoType:String = "movie",
                         kNum:Int = 1000,
                         numIter:Int = 100,
                         numOfDays:Int = 30,
                         score:Double = 0.4,
                         kafkaTopic:String = "",
                         ifABTest:Boolean = false,
                         alg:String = "",
                         offset:Int = 0,
                         userGroup:String = "",
                         code:String = "",
                         codeType:Int = 0,
                         videoTypePrefix: String = ""
                       )
