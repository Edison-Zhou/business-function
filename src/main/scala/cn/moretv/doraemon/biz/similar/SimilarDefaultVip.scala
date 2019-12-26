package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import cn.whaley.sdk.utils.TransformUDF

/***
  * created by michael on 2019/2/14
  *
  * vip相似影片默认推荐
  */
object SimilarDefaultVip extends BaseClass {

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

      //有效vip影片的数据
      val validSidDF = BizUtils.getAvailableVipVideo(contentType)
        .selectExpr("sid", s"'$contentType' as cluster")
      BizUtils.getDataFrameInfo(validSidDF,"validSidDF")

      //获得只有vip类型的推荐影片
      val recommendVipDF=recommendDF.join(validSidDF,"sid").select(recommendDF("*"))
      BizUtils.getDataFrameInfo(recommendVipDF,"recommendVipDF")

      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.preferTableUserColumn = "sid"
      similarClusterPara.clusterDetailTableContentColumn = "sid"
      similarClusterPara.outputUserColumn = "sid"
      similarClusterPara.topN = 70

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_PREFER -> validSidDF,
        similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> recommendVipDF)
      similarAlg.initInputData(dataMap)

      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarDefaultVip/" + contentType)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa
}
