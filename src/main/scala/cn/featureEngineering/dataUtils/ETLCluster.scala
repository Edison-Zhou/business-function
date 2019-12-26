package cn.featureEngineering.dataUtils

import breeze.optimize.linear.PowerMethod.BDV
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cheng_huan on 2019/4/23.
  */
object ETLCluster {

  def vectorCosineSimilarity(V1:Array[Double], V2:Array[Double]):Double = {
    val BV1 = new BDV(V1)
    val BV2 = new BDV(V2)
    val value1 = BV1.dot(BV2)
    val value2 = math.sqrt(BV1.dot(BV1))
    val value3 = math.sqrt(BV2.dot(BV2))
    if(value2 == 0 || value3 == 0)
      0
    else
      value1/(value2 * value3)
  }

  /**
    * 计算sid与所在cluster之间的相似度
    * @param videoCluster DF("index", "sidArray")
    * @param videoWord2vecFeature DF("sid", "features")
    * @return DF("sid", "index", "similarity")
    */
  def videoClusterProperty(videoCluster:DataFrame, videoWord2vecFeature:DataFrame) = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val video2clusterDF = videoCluster.flatMap(r => r.getAs[Seq[String]]("sidArray")
      .map(e => (r.getAs[Int]("index"), e))).toDF("index", "sid")

    val clusterDF = video2clusterDF.as("a")
      .join(videoWord2vecFeature.as("b"), expr("a.sid = b.sid"), "inner")
      .drop(expr("a.sid")).rdd
      .map(r => (r.getAs[Int]("index"), r.getAs[Seq[Double]]("feature")))
      .groupByKey()
      .map(e => {
        val index = e._1
        val featureArray = e._2.toArray
        val size = featureArray.size
        val length = featureArray.head.length
        val sumArray = new Array[Double](length)
        for (i <- 0 until size) {
          for(j <- 0 until length) {
            sumArray(j) = sumArray(j) + featureArray(i)(j)
          }
        }
        for(k <- 0 until length) sumArray(k) = sumArray(k) / size

        (index, sumArray)
      }).toDF("index", "center")

    video2clusterDF.as("a").join(clusterDF.as("b"), expr("a.index = b.index"), "left")
      .join(videoWord2vecFeature.as("c"), expr("a.sid = c.sid"), "inner").repartition(2000)
      .selectExpr("a.sid as sid", "a.index as index", "b.center as center", "c.feature as feature")
      .map(r => {
        val center = r.getAs[Seq[Double]]("center").toArray
        val feature = r.getAs[Seq[Double]]("feature").toArray
        val similarity = vectorCosineSimilarity(center, feature)
        (r.getAs[String]("sid"), r.getAs[Int]("index"), similarity)
      }).toDF("sid", "index", "similarity")
  }

  /**
    * 计算聚类之间的相似度
    * @param videoCluster 视频聚类
    * @param videoWord2vecFeature 视频特征
    * @param threshold 相似度阈值
    * @return DF("cluster1", "cluster2", "similarity")
    */
  def videoClusterBetweenSimilarity(videoCluster:DataFrame,
                                    videoWord2vecFeature:DataFrame,
                                    threshold:Double) = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val video2clusterDF = videoCluster.flatMap(r => r.getAs[Seq[String]]("sidArray")
      .map(e => (r.getAs[Int]("index"), e))).toDF("index", "sid")

    val clusterDF = video2clusterDF.as("a")
      .join(videoWord2vecFeature.as("b"), expr("a.sid = b.sid"), "inner")
      .drop(expr("a.sid")).rdd
      .map(r => (r.getAs[Int]("index"), r.getAs[Seq[Double]]("feature")))
      .groupByKey()
      .map(e => {
        val index = e._1
        val featureArray = e._2.toArray
        val size = featureArray.size
        val length = featureArray.head.length
        val sumArray = new Array[Double](length)
        for (i <- 0 until size) {
          for(j <- 0 until length) {
            sumArray(j) = sumArray(j) + featureArray(i)(j)
          }
        }
        for(k <- 0 until length) sumArray(k) = sumArray(k) / size

        (index, sumArray)
      }).toDF("index", "center")

    clusterDF.as("a").join(clusterDF.as("b")).repartition(50)
      .selectExpr("a.index as index1", "b.index as index2", "a.center as center1", "b.center as center2")
      .map(r => {
        val cluster1 = r.getAs[Int]("index1")
        val cluster2 = r.getAs[Int]("index2")
        val center1 = r.getAs[Seq[Double]]("center1").toArray
        val center2 = r.getAs[Seq[Double]]("center2").toArray
        val similarity = vectorCosineSimilarity(center1, center2)

        (cluster1, cluster2, similarity)
      }).toDF("cluster1", "cluster2", "similarity").filter(s"similarity >= $threshold")

  }

  /**
    *
    * @param videoClusterProperty DF("sid", "index", "similarity")
    * @param clusterSize 1024
    * @return DF("sid", "features")
    */
  def videoFeature(videoClusterProperty: DataFrame, clusterSize: Int):DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    videoClusterProperty.groupBy("sid").agg(collect_list(concat_ws("%", col("index"), col("similarity"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("sid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(clusterSize, sequences).toSparse

        (sid, features)
      }).toDF("sid", "features")
  }

  /**
    *
    * @param videoClusterProperty DF("sid", "index", "similarity")
    * @param videoClusterBetweenSimilarity DF("cluster1", "cluster2", "similarity")
    * @param clusterSize 1024
    * @return
    */
  def videoFeature2(videoClusterProperty: DataFrame,
                    videoClusterBetweenSimilarity: DataFrame,
                    clusterSize: Int):DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    videoClusterProperty.as("a").join(videoClusterBetweenSimilarity.as("b"), expr("a.index = b.cluster1"), "inner")
      .repartition(500).selectExpr("a.sid as sid", "b.cluster2 as index", "a.similarity * b.similarity as similarity")
      .groupBy("sid", "index").agg(max("similarity").as("similarity"))
      .groupBy("sid").agg(collect_list(concat_ws("%", col("index"), col("similarity"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("sid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(clusterSize, sequences).toSparse

        (sid, features)
      }).toDF("sid", "features")
  }

  /**
    *
    * @param score DF("uid", "sid", "optime", "score")
    * @param videoClusterProperty DF("sid", "index", "similarity")
    * @param weightDecay 0.95
    * @param clusterSize 1024
    * @return DF("uid", "features")
    */
  def userFeature(score: DataFrame, videoClusterProperty: DataFrame, weightDecay: Double, clusterSize: Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    score.withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("optime").desc)))
      .map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[String]("optime"),
        r.getAs[Double]("score"), math.max(math.pow(weightDecay, r.getAs[Int]("rank")), 0.5)))
      .toDF("uid", "sid", "optime", "score", "weight")
      .as("a").join(videoClusterProperty.as("b"), expr("a.sid = b.sid"), "inner")
      .map(r => (r.getAs[String]("uid"), r.getAs[Int]("index"),
          r.getAs[Double]("weight") * r.getAs[Double]("score") * r.getAs[Double]("similarity")))
      .toDF("uid", "index", "prefer")
      .groupBy("uid", "index").agg(sum("prefer").as("prefer"))
        .groupBy("uid").agg(collect_list(concat_ws("%", col("index"), col("prefer"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("uid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(clusterSize, sequences).toSparse

        (sid, features)
      }).toDF("uid", "features")
  }

  /**
    *
    * @param score DF("uid", "sid", "optime", "score")
    * @param videoClusterProperty DF("sid", "index", "similarity")
    * @param videoClusterBetweenSimilarity DF("cluster1", "cluster2", "similarity")
    * @param weightDecay 0.95
    * @param clusterSize 1024
    * @return DF("uid", "features")
    */
  def userFeature2(score: DataFrame,
                   videoClusterProperty: DataFrame,
                   videoClusterBetweenSimilarity: DataFrame,
                   weightDecay: Double,
                   clusterSize: Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val userClusterProperty = score.repartition(2000).withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("optime").desc)))
      .map(r => (r.getAs[Long]("uid"), r.getAs[String]("sid"), r.getAs[String]("optime"),
        r.getAs[Double]("score"), math.max(math.pow(weightDecay, r.getAs[Int]("rank")), 0.5)))
      .toDF("uid", "sid", "optime", "score", "weight")
      .as("a").join(videoClusterProperty.as("b"), expr("a.sid = b.sid"), "inner").repartition(2000)
      .map(r => (r.getAs[Long]("uid"), r.getAs[Int]("index"),
        r.getAs[Double]("weight") * r.getAs[Double]("score") * r.getAs[Double]("similarity")))
      .toDF("uid", "index", "prefer")
      .groupBy("uid", "index").agg(sum("prefer").as("prefer"))

    val userFeature = userClusterProperty.as("a").join(videoClusterBetweenSimilarity.as("b"), expr("a.index = b.cluster1"), "inner")
      .repartition(2000).selectExpr("a.uid as uid", "b.cluster2 as index", "a.prefer * b.similarity as prefer")
      .groupBy("uid", "index").agg(max("prefer").as("prefer"))
      .groupBy("uid").agg(collect_list(concat_ws("%", col("index"), col("prefer"))).as("features"))
      .map(r => {
        val sid = r.getAs[Long]("uid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(clusterSize, sequences).toSparse

        (sid, features)
      }).toDF("uid", "features")

    userFeature
  }

}
