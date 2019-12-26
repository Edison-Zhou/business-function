package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.functions.expr

/**
  * 相似影片默认推荐
  * Updated by lituo on 2018/7/16
  */
object SimilarDefault extends BaseClass {

  def execute(args: Array[String]): Unit = {

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {

      val hotDf = DataReader.read(
        new HdfsPath(
          "/ai/dw/moretv/rankings/hot/Latest",
          s"select sid, '$contentType' as cluster from tmp where content_type = '$contentType'"
        ))

      val recommendDFOld = if(contentType != "movie")
        hotDf
      else
        hotDf.union(DataReader.read(BizUtils.getMysqlPath("movie_editor_recommend"))
          .selectExpr("sid", "cluster"))

      //转虚拟id
      val recommendDF = BizUtils.transferToVirtualSid(recommendDFOld, "sid")

      //有效影片的数据
      val validSidDF = BizUtils.getAvailableVideo(contentType)
        .selectExpr("sid", s"'$contentType' as cluster")

      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.preferTableUserColumn = "sid"
      similarClusterPara.clusterDetailTableContentColumn = "sid"
      similarClusterPara.outputUserColumn = "sid"
      similarClusterPara.topN = 70

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_PREFER -> validSidDF,
        similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> recommendDF)
      similarAlg.initInputData(dataMap)

      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarDefault/" + contentType)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
