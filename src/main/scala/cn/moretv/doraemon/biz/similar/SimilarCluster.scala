package cn.moretv.doraemon.biz.similar

import cn.moretv.doraemon.algorithm.cluster.{ClusterKmeansAlgorithm, ClusterKmeansModel, ClusterKmeansParameters}
import cn.moretv.doraemon.algorithm.similar.cluster.{SimilarClusterAlgorithm, SimilarClusterParameters}
import cn.moretv.doraemon.algorithm.validationCheck.{ValidationCheckAlgorithm, ValidationCheckModel, ValidationCheckParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.ml.linalg.Vectors

/**
  * 基于聚类的影片相似度
  * Updated by lituo on 2018/7/23
  */
object SimilarCluster extends BaseClass {


  def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    TransformUDF.registerUDFSS

    val contentTypeList = List("movie", "tv", "zongyi", "comic", "kids", "jilu")

    contentTypeList.foreach(contentType => {
      //读入节目对应的Word2Vec数据
      val word2VecPath: HdfsPath = new HdfsPath(s"/ai/dw/moretv/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
      val word2VecSidDF = DataReader.read(word2VecPath)
        .rdd.map(line => line.getString(0).split(",")).
        map(e => (e(0).toInt, e.takeRight(128))).
        map(e => (e._1, Vectors.dense(e._2.map(x => x.toDouble))))
        .toDF("sid", "vector")

      //有效影片的数据
      val validSidPath= BizUtils.getMysqlPath(s"${contentType}_valid_sid")
      val validSidDF = DataReader.read(validSidPath)
        .selectExpr("transformSid(sid) as sid")

      //数据的有效性检查
      val validAlg: ValidationCheckAlgorithm = new ValidationCheckAlgorithm()
      val validPara = validAlg.getParameters.asInstanceOf[ValidationCheckParameters]
      validPara.userOrItem = "item"
      val validDataMap = Map(validAlg.INPUT_DATA_KEY -> word2VecSidDF, validAlg.INPUT_CHECKLIST_KEY -> validSidDF)
      validAlg.initInputData(validDataMap)
      validAlg.run()
      val validTagSidDF = validAlg.getOutputModel.asInstanceOf[ValidationCheckModel].checkedData

      //调用聚类方法
      val clusterAlg: ClusterKmeansAlgorithm = new ClusterKmeansAlgorithm()
      val clusterKmeansPara = clusterAlg.getParameters.asInstanceOf[ClusterKmeansParameters]
      val vectorDataMap = Map(clusterAlg.INPUT_DATA_KEY_VECTOR -> validTagSidDF)
      clusterAlg.initInputData(vectorDataMap)
      clusterAlg.run()
      val clusterDF = clusterAlg.getOutputModel.asInstanceOf[ClusterKmeansModel]

      //读入cluster的数据
      val recommendDF = clusterDF.matrixData

      //调用
      val similarAlg: SimilarClusterAlgorithm = new SimilarClusterAlgorithm()
      val similarClusterPara = similarAlg.getParameters.asInstanceOf[SimilarClusterParameters]
      similarClusterPara.topN = 70
      similarClusterPara.preferTableUserColumn = "sid"
      similarClusterPara.outputUserColumn = "sid"

      val dataMap = Map(similarAlg.INPUT_DATA_KEY_CLUSTER_DETAIL -> recommendDF,
        similarAlg.INPUT_DATA_KEY_PREFER -> recommendDF)

      similarAlg.initInputData(dataMap)
      similarAlg.run()
      //结果输出到HDFS
      similarAlg.getOutputModel.output("similarCluster/" + contentType)
    })
  }

  override implicit val productLine: ProductLineEnum.Value = ProductLineEnum.medusa

}
