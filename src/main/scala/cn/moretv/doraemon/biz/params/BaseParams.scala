package cn.moretv.doraemon.biz.params

import scopt.OptionParser


case class BaseParams(
                    env: String = null,
                    productLine: String = null,
                    supParam1: Double = 0.0,
                    supParam2: Double = 0.0,
                    part: Int = 1
                     )

object BaseParams{
  lazy val parser: OptionParser[BaseParams] = new OptionParser[BaseParams]("ParamsParse"){
    override def errorOnUnknownArgument = false
    opt[String]("env").
      action((x, c) => c.copy(env = x))
    opt[String]("productLine").
      action((x, c) => c.copy(productLine = x))
    opt[Double]("supParam1").
      action((x, c) => c.copy(supParam1 = x))
    opt[Double]("supParam2").
      action((x, c) => c.copy(supParam2 = x))
    opt[Int]("part").
      action((x, c) => c.copy(part = x))
  }

  def getParams(args: Array[String]): BaseParams = {
    var params: BaseParams = null
    parser.parse(args, BaseParams()) match {
      case Some(param) =>
        params = param
        params
      case None => {
        println("ERROR: BaseParams wrong input parameters")
        System.exit(1)
        params
      }
    }
  }
}