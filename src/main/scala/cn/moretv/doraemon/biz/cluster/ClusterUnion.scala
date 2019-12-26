package cn.moretv.doraemon.biz.cluster

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/7/26.
  */
object ClusterUnion extends BaseClass{

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    val clusterCenter = getClusterCenter("movie", 128)
    val kMeans = new KMeans().setK(200).setMaxIter(100)
    val model = kMeans.fit(clusterCenter.select("features"))

    val clusterData = model.transform(clusterCenter).selectExpr("cluster", "prediction")

    BizUtils.outputWrite(clusterData, "clusterUnion")
  }

  /**
    * 计算聚类中心
    * @param contentType
    * @param featureSize
    * @return
    */
  def getClusterCenter(contentType: String, featureSize: Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._
    val word2VecPath: HdfsPath = new HdfsPath(s"/ai/data/medusa/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
    val word2VecSidDF = DataReader.read(word2VecPath)
      .rdd.map(line => line.getString(0).split(",")).
      map(e => (e(0), Vectors.dense(e.takeRight(featureSize).map(x => x.toDouble))))
      .toDF("sid", "vector")

    val clusterPath = new HdfsPath("/ai/data/medusa/videoFeatures" + s"/${contentType}Clusters/Latest")
    val videoCluster = DataReader.read(clusterPath)
      .flatMap(r => r.getAs[Seq[String]]("items").map(sid => {
        (r.getAs[Int]("cluster"), sid)
      })).toDF("cluster", "sid")

    videoCluster.as("a").join(word2VecSidDF.as("b"), expr("a.sid = b.sid"), "inner")
      .selectExpr("a.cluster as cluster", "b.vector as vector")
      .groupBy("cluster").agg(collect_list("vector").as("vecList"))
      .map(r => {
        val cluster = r.getAs[Int]("cluster")
        val vecList = r.getAs[Seq[DenseVector]]("vecList")
        val center = new Array[Double](featureSize)
        val size = vecList.size

        for(vector <- vecList) {
          for(i <- center.indices) {
            center(i) += vector(i)
          }
        }
        for(i <- center.indices) {
          center(i) /= size
        }
        (cluster, center)
      }).toDF("cluster", "features")

  }
}
