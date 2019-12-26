package cn.moretv.doraemon.biz.cluster

import cn.moretv.doraemon.algorithm.cluster.{ClusterKmeansAlgorithm, ClusterKmeansModel, ClusterKmeansParameters}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.{FileFormatEnum, ProductLineEnum}
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by cheng_huan on 2018/10/30.
  */
object LongVideoCluster extends BaseClass{
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss = spark
    import ss.implicits._

    val contentTypeList = List(("movie",1024), ("tv",1024), ("zongyi",1024), ("comic",512), ("kids",512), ("jilu",256))
    //val contentTypeList = List(("movieTag",1024))

    contentTypeList.foreach(e => {
      val contentType = e._1
      val clusterUnionNum = e._2
      //读入节目对应的Word2Vec数据
      val word2VecPath: HdfsPath = new HdfsPath(s"/ai/data/medusa/base/word2vec/item/Latest/${contentType}Features.txt", FileFormatEnum.TEXT)
      val word2VecSidDF = DataReader.read(word2VecPath)
        .rdd.map(line => line.getString(0).split(",")).
        map(e => (e(0), Vectors.dense(e.takeRight(128).map(x => x.toDouble))))
        .toDF("sid", "vector")

      val alg = new ClusterKmeansAlgorithm()
      val param = alg.getParameters.asInstanceOf[ClusterKmeansParameters]
      param.clusterUnionNum = clusterUnionNum
      param.filterMinSimilarity = 0.9
      param.filterClusterMinSize = 2
      val dataMap = Map(alg.INPUT_DATA_KEY_VECTOR -> word2VecSidDF)

      alg.initInputData(dataMap)
      alg.run()

      val clusterDF = alg.getOutputModel.asInstanceOf[ClusterKmeansModel].matrixData

      val dataWriter = new DataWriter2Hdfs
      dataWriter.write(clusterDF, new HdfsPath("/ai/data/medusa/videoFeatures" + s"/${contentType}Clusters"))
    })

    /**
      * 将所有长视频聚类合并在一起
      */
    var unionCluster = Array.empty[(Int, Array[String])]
    for (contentType <- Array("movie","tv", "zongyi", "jilu", "comic", "kids")) {
      val path = "/ai/data/medusa/videoFeatures" + s"/${contentType}Clusters"

      val videoClusterData = BizUtils.getDataFrameNewest(path).rdd.map(r => (r.getInt(0), r.getSeq[String](1).toArray))
      unionCluster = unionCluster ++ videoClusterData.collect()
    }

    val LongVideoClusterDF = sc.makeRDD(unionCluster.map(e => {
      val index = unionCluster.indexOf(e)
      (index, e._2)
    })).toDF("clusterIndex", "videoSid")

    val dataWriter = new DataWriter2Hdfs
    dataWriter.write(LongVideoClusterDF, new HdfsPath("/ai/data/medusa/videoFeatures/longVideoClusters"))
  }

}
