package cn.featureEngineering.dataUtils

import cn.moretv.doraemon.biz.util.BizUtils
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.expressions.Window

/**
  * Created by cheng_huan on 2019/2/19.
  */
object ETLSparse {

  /**
    * 将视频标签转化为特征
    *
    * @param DF DF("tag_id", "sid", "rating")
    * @return DF("sid", "features")
    */
  def tag2VideoFeatures(DF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val tags = DF.select("tag_id").distinct().map(r => r.getAs[Int]("tag_id")).collect().sorted
    println(s"tags.length = ${tags.length}")

    val size = tags.max + 1

    DF.groupBy("sid").agg(collect_list(concat_ws("%", col("tag_id"), col("rating"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("sid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(size, sequences).toSparse

        (sid, features)
      }).toDF("sid", "features")
  }

  /**
    * 标签重新编码
    * @param DF DF("tag_id", "tag_name")
    * @return DF("tag_id", "tag_name", "index")
    */
  def tagReIndex(DF: DataFrame): DataFrame = {
    DF.select("tag_id", "tag_name").distinct()
      .withColumn("index", row_number().over(Window.orderBy("tag_id")))
  }

  /**
    * 标签按频次截断
    * @param DF DF("tag_id", "tag_name", "sid", "rating")
    * @param truncNum
    * @return
    */
  def tagFrequencyTruncation(DF: DataFrame, truncNum: Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val tagFreqDF = DF.select("tag_id", "sid").groupBy("tag_id")
      .agg(count("sid").as("freq")).sort(col("freq").desc).limit(truncNum)

    DF.as("a")
      .join(tagFreqDF.as("b"), expr("a.tag_id = b.tag_id"), "inner")
      .drop("freq").drop(expr("a.tag_id"))
  }

  /**
    * 将视频标签reIndex后转化为特征
    *
    * @param DF DF("tag_id", "sid", "rating")
    * @return DF("sid", "features")
    */
  def tagReIndex2VideoFeatures(DF: DataFrame, tagIndexDF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val tagsNum = tagIndexDF.count()
    println(s"tags.length = $tagsNum")

    val size = tagsNum.toInt + 1

    DF.as("a").join(tagIndexDF.as("b"), expr("a.tag_id = b.tag_id")).groupBy("sid")
      .agg(collect_list(concat_ws("%", col("index"), col("rating"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("sid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(size, sequences).toSparse

        (sid, features)
      }).toDF("sid", "features")
  }

  /**
    * 基于标签的视频稀疏向量ReIndex
    * @param videoFeature DF("sid", "features")
    * @param tagReIndexMap Map("tag_id", "reIndex")
    * @return
    */
  def videoTagFeatureReIndex(videoFeature: DataFrame, tagReIndexMap: Map[Int, Int]): DataFrame = {
    val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val bcReIndexMap = ss.sparkContext.broadcast(tagReIndexMap)
    val size = tagReIndexMap.size + 1
    videoFeature.map(r => {
      val sid = r.getAs[String]("sid")
      val indices = r.getAs[SparseVector]("features").indices
      val values = r.getAs[SparseVector]("features").values
      val sequence = indices.map(index => {
        val reIndex = bcReIndexMap.value.getOrElse(index, -1)
        val value = values(indices.indexOf(index))
        (reIndex, value)
      }).filter(_._1 != -1)

      val features = Vectors.sparse(size, sequence)
      (sid, features)
    }).toDF("sid", "features")
  }

  /**
    *
    * @param videoFeaturesDF DF("sid", "features")
    * @param scoreDF         DF("uid", "sid", "score")
    * @return
    */
  def union2userFeatures(videoFeaturesDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val size = videoFeaturesDF.head().getAs[SparseVector]("features").size

    scoreDF.as("a").join(videoFeaturesDF.as("b"), expr("a.sid = b.sid"), "inner").drop(expr("a.sid")).flatMap(r => {
      val score = r.getAs[Double]("score")
      val featureVector = r.getAs[SparseVector]("features")
      val values = featureVector.values.map(e => e * score * score)
      val indices = featureVector.indices
      val uid = r.getAs[String]("uid")

      indices.map(i => (uid, i, values(indices.indexOf(i))))
    })
      .toDF("uid", "featureName", "featureRating")
      .groupBy("uid", "featureName").agg(sum("featureRating").as("featureRating"))
      .groupBy("uid").agg(collect_list(concat_ws("%", col("featureName"), col("featureRating"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("uid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(size, sequences).toSparse

        (sid, features)
      }).toDF("uid", "features")
  }

  /**
    *
    * @param videoFeaturesDF DF("sid", "features")
    * @param scoreDF         DF("uid", "sid", "score")
    * @return
    */
  def union2userFeaturesWithDecay(videoFeaturesDF: DataFrame,
                                  scoreDF: DataFrame,
                                  weightDecay: Double,
                                  unionFunc: String): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val size = videoFeaturesDF.head().getAs[SparseVector]("features").size

    val score = scoreDF.withColumn("rank", row_number().over(Window.partitionBy("uid").orderBy(col("optime").desc)))
      .map(r => (r.getAs[Long]("uid"), r.getAs[String]("sid"), r.getAs[String]("optime"),
        r.getAs[Double]("score"), math.max(math.pow(weightDecay, r.getAs[Int]("rank")), 0.9)))
      .toDF("uid", "sid", "optime", "score", "weight")

    val DF1 = score.as("a").join(videoFeaturesDF.as("b"), expr("a.sid = b.sid"), "inner").drop(expr("a.sid")).flatMap(r => {
      val score = r.getAs[Double]("score")
      val featureVector = r.getAs[SparseVector]("features")
      val weight = r.getAs[Double]("weight")
      val values = featureVector.values.map(e => e * score * weight)
      val indices = featureVector.indices
      val uid = r.getAs[Long]("uid")

      indices.map(i => (uid, i, values(indices.indexOf(i))))
    })
      .toDF("uid", "featureName", "featureRating")
      .groupBy("uid", "featureName")

    val DF2 = unionFunc match {
      case "sum" => DF1.agg(sum("featureRating").as("featureRating"))
      case "avg" => DF1.agg(avg("featureRating").as("featureRating"))
      case "max" => DF1.agg(max("featureRating").as("featureRating"))
    }

    DF2.groupBy("uid").agg(collect_list(concat_ws("%", col("featureName"), col("featureRating"))).as("features"))
      .map(r => {
        val sid = r.getAs[Long]("uid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(size, sequences).toSparse

        (sid, features)
      }).toDF("uid", "features")

    /*score.as("a").join(videoFeaturesDF.as("b"), expr("a.sid = b.sid"), "inner").drop(expr("a.sid")).flatMap(r => {
      val score = r.getAs[Double]("score")
      val featureVector = r.getAs[SparseVector]("features")
      val weight = r.getAs[Double]("weight")
      val values = featureVector.values.map(e => e * score * weight)
      val indices = featureVector.indices
      val uid = r.getAs[Long]("uid")

      indices.map(i => (uid, i, values(indices.indexOf(i))))
    })
      .toDF("uid", "featureName", "featureRating")
      .groupBy("uid", "featureName").agg(sum("featureRating").as("featureRating"))
      .groupBy("uid").agg(collect_list(concat_ws("%", col("featureName"), col("featureRating"))).as("features"))
      .map(r => {
        val sid = r.getAs[Long]("uid")
        val sequences = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)
        val features = Vectors.sparse(size, sequences).toSparse

        (sid, features)
      }).toDF("uid", "features")*/
  }

  /**
    *
    * @param userFeatures
    * @param videoFeatures
    * @param scoreDF
    * @return
    */
  def featureProjection(userFeatures: DataFrame, videoFeatures: DataFrame, scoreDF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    scoreDF.as("a").join(userFeatures.as("b"), expr("a.uid = b.uid"), "inner")
      .join(videoFeatures.as("c"), expr("a.sid = c.sid"), "inner")
      .selectExpr("a.uid as uid", "a.sid as sid", "b.features as userFeatures", "c.features as videoFeatures", "a.score as score").repartition(20000)
      .map(r => {
        val uid = r.getAs[String]("uid")
        val sid = r.getAs[String]("sid")
        val userFeatures = r.getAs[SparseVector]("userFeatures")
        val videoFeatures = r.getAs[SparseVector]("videoFeatures")
        val label = r.getAs[Double]("score")
        val videoSize = videoFeatures.size
        val videoSequences = videoFeatures.indices.map(i => {
          val value = if (userFeatures.indices.indexOf(i) != -1) {
            val value1 = videoFeatures.values(videoFeatures.indices.indexOf(i))
            val value2 = userFeatures.values(userFeatures.indices.indexOf(i))
            value1 * value2
          } else {
            0.0
          }
          (i, value)
        })
        val features = Vectors.sparse(videoSize, videoSequences).toSparse

        (uid, sid, features, label)
      }).toDF("uid", "sid", "features", "label")
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid", "sid", "features", "label")
    * @return DF("uid", "sid", "label", "features")
    */
  def normalizationStd(DF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._

    val featureSize = DF.head.getAs[SparseVector]("features").size

    val DF1 = DF.flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val uid = r.getAs[String]("uid")
      val sid = r.getAs[String]("sid")
      val label = r.getAs[Double]("label")

      sparseFeature.indices.map(e => (uid, sid, label, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("uid", "sid", "label", "fieldIndex", "featureRating")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val DF2 = DF1.select("fieldIndex", "featureRating").
      groupBy("fieldIndex").agg(avg("featureRating").as("avgRating"), stddev("featureRating").as("stddevRating"))
      .na.drop()

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldIndex = b.fieldIndex"), "left").drop(expr("b.fieldIndex"))
      .withColumn("featureRating", expr("case when  stddevRating >= 0.1 then (featureRating - avgRating) / stddevRating else featureRating end"))
      .distinct()
      .groupBy("uid", "sid", "label").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[Double]("label"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF("uid", "sid", "label", "features")

    DF3
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid", "sid", "features", "label")
    * @return DF("uid", "sid", "label", "features")
    */
  def normalization(DF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val featureSize = DF.head.getAs[SparseVector]("features").size

    val DF1 = DF.repartition(20000).flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val uid = r.getAs[String]("uid")
      val sid = r.getAs[String]("sid")
      val label = r.getAs[Double]("label")

      sparseFeature.indices.map(e => (uid, sid, label, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("uid", "sid", "label", "fieldIndex", "featureRating")
      .persist(StorageLevel.DISK_ONLY)

    val DF2 = DF1.select("fieldIndex", "featureRating").
      groupBy("fieldIndex").agg(max("featureRating").as("maxRating"))
      .filter("maxRating > 0")

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldIndex = b.fieldIndex"), "inner").repartition(20000)
      .drop(expr("b.fieldIndex"))
      .withColumn("featureRating", col("featureRating") / col("maxRating"))
      .distinct()
      .groupBy("uid", "sid", "label").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[Double]("label"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF("uid", "sid", "label", "features")

    DF3
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid", "features")
    * @return DF("uid",  "features")
    */
  def featureNormalization(DF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val featureSize = DF.head.getAs[SparseVector]("features").size
    val columnName = DF.columns(0)

    val DF1 = DF.repartition(20000).flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val id = r.getAs[Long](columnName)

      sparseFeature.indices.map(e => (id, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("id", "fieldIndex", "featureRating")
      .persist(StorageLevel.DISK_ONLY)

    val DF2 = DF1.select("fieldIndex", "featureRating").
      groupBy("fieldIndex").agg(max("featureRating").as("maxRating"))
      .filter("maxRating > 0")

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldIndex = b.fieldIndex"), "inner").repartition(20000)
      .drop(expr("b.fieldIndex"))
      .withColumn("featureRating", col("featureRating") / col("maxRating"))
      .distinct()
      .groupBy("id").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[Long]("id"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF(columnName, "features")

    DF3
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid" or "sid, "features")
    * @return DF("uid" or "sid",  "features")
    */
  def featureNormalizationByQuantile(DF: DataFrame,
                                     lowerQuantile: Double,
                                     upperQuantile: Double): DataFrame = {
    val columnName = DF.columns(0)

    columnName match {
      case "uid" => userFeatureNormalizationByQuantile(DF, lowerQuantile, upperQuantile)
      case "sid" => videoFeatureNormalizationByQuantile(DF, lowerQuantile, upperQuantile)
    }
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid" or "sid, "features")
    * @return DF("uid" or "sid",  "features")
    */
  def userFeatureNormalizationByQuantile(DF: DataFrame,
                                         lowerQuantile: Double,
                                         upperQuantile: Double): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val featureSize = DF.head.getAs[SparseVector]("features").size

    val DF1 = DF.repartition(20000).flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val id = r.getAs[Long]("uid")

      sparseFeature.indices.map(e => (id, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("uid", "fieldIndex", "featureRating")
      .withColumn("percentRank", percent_rank().over(Window.partitionBy("fieldIndex").orderBy("featureRating")))
      .filter(s"percentRank >= $lowerQuantile and percentRank <= $upperQuantile")
      .persist(StorageLevel.DISK_ONLY)

    val DF2 = DF1.select("fieldIndex", "featureRating").groupBy("fieldIndex")
      .agg(max("featureRating").as("maxRating"), min("featureRating").as("minRating"))

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldIndex = b.fieldIndex"), "inner").repartition(20000)
      .drop(expr("b.fieldIndex"))
      .withColumn("featureRating", (col("featureRating") - col("minRating")) / (col("maxRating") - col("minRating")))
      .distinct()
      .groupBy("uid").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[Long]("uid"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF("uid", "features")

    DF3
  }

  /**
    * 将特征按维度进行标准化
    *
    * @param DF DF("uid" or "sid, "features")
    * @return DF("uid" or "sid",  "features")
    */
  def videoFeatureNormalizationByQuantile(DF: DataFrame,
                                          lowerQuantile: Double,
                                          upperQuantile: Double): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    val featureSize = DF.head.getAs[SparseVector]("features").size

    val DF1 = DF.repartition(20000).flatMap(r => {
      val sparseFeature = r.getAs[SparseVector]("features")
      val id = r.getAs[String]("sid")

      sparseFeature.indices.map(e => (id, e, sparseFeature.values(sparseFeature.indices.indexOf(e))))
    })
      .toDF("sid", "fieldIndex", "featureRating")
      .withColumn("percentRank", percent_rank().over(Window.partitionBy("fieldIndex").orderBy("featureRating")))
      .filter(s"percentRank >= $lowerQuantile and percentRank <= $upperQuantile")
      .persist(StorageLevel.DISK_ONLY)

    val DF2 = DF1.select("fieldIndex", "featureRating").groupBy("fieldIndex")
      .agg(max("featureRating").as("maxRating"), min("featureRating").as("minRating"))

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldIndex = b.fieldIndex"), "inner").repartition(20000)
      .drop(expr("b.fieldIndex"))
      .withColumn("featureRating", (col("featureRating") - col("minRating")) / (col("maxRating") - col("minRating")))
      .distinct()
      .groupBy("sid").agg(collect_list(concat_ws("%", col("fieldIndex"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[String]("sid"),
        Vectors.sparse(featureSize, r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0).toInt, s.split("%")(1).toDouble)).sortBy(_._1)).toSparse))
      .toDF("sid", "features")

    DF3
  }

  /**
    * 特征交叉
    *
    * @param DF DF("uid", "sid", "label", "features")
    * @return
    */
  def featureIntersection(DF: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    DF.map(r => {
      val uid = r.getAs[String]("uid")
      val sid = r.getAs[String]("sid")
      val label = r.getAs[Double]("label")
      val size = r.getAs[SparseVector]("features").size
      val indices = r.getAs[SparseVector]("features").indices
      val values = r.getAs[SparseVector]("features").values
      val newSparseVectorSize = ((size * (size + 1)) / 2.0).toInt + size

      val originalLength = indices.size
      val interLength = ((originalLength * (originalLength + 1)) / 2.0).toInt + originalLength
      val array = new Array[(Int, Double)](interLength)

      for (i <- 0 until originalLength) {
        array(i) = (indices(i), values(i))
        for (j <- 0 until originalLength) {
          if (i <= j) {
            val idx = (i + 1) * originalLength + j - ((i * (i + 1)) / 2.0).toInt
            val index = (indices(i) + 1) * size + indices(j) - ((indices(i) * (indices(i) + 1)) / 2.0).toInt
            val value = values(i) * values(j)
            array(idx) = (index, value)
          }
        }
      }

      val features = Vectors.sparse(newSparseVectorSize, array).toSparse
      (uid, sid, features, label)

    }).toDF("uid", "sid", "features", "label")
  }

  /**
    * 向量降维
    * @param vec 原始向量
    * @param reserveDims 要保留的维度 Array[Index]
    * @param newSize 降维后的向量维度
    * @return
    */
  def vectorReduction(vec: SparseVector, reserveDims: Array[Int], newSize: Int): SparseVector = {
    val indices = vec.indices
    val values = vec.values
    val sequence = reserveDims.map(index => {
      val valueIndex = indices.indexOf(index)
      if(valueIndex >= 0 && valueIndex < values.size)
        values(valueIndex)
      else
        0.0
    }).zipWithIndex.map(e => (e._2, e._1)).filter(e => e._2 > 0).toSeq
    Vectors.sparse(newSize, sequence).toSparse
  }

  /**
    * 特征降维
    * @param DF DF("uid", "sid", "label", "features")
    * @param featureImportance Vector[Double]
    * @return
    */
  def featureReduction(DF: DataFrame, featureImportance: Vector[Double], newSize: Int): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    require(featureImportance.size >= newSize, "The dimensions of original vector and new vector are not appropriate!")

    val featureArray = featureImportance.map(e => (math.random, e)).toArray
    val reserveDims = featureArray.map(e => (featureArray.indexOf(e), e._2)).sortBy(e => -e._2)
        .take(newSize).sortBy(e => e._1).map(e => e._1)

    reserveDims.foreach(println)

    DF.map(r => {
      val uid = r.getAs[String]("uid")
      val sid = r.getAs[String]("sid")
      val label = r.getAs[Double]("label")
      val features = vectorReduction(r.getAs[SparseVector]("features"), reserveDims, newSize)

      (uid, sid, label, features)
    })
      .toDF("uid", "sid", "label", "features")
  }


  /**
    * 特征重要性合并
    * @param DF1 DF("index", "value")
    * @param DF2 DF("index", "value")
    * @return
    */
  def featureImportanceUnion(DF1: DataFrame, DF2: DataFrame): DataFrame = {
    val ss: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import ss.implicits._
    require(DF1.count() == DF2.count(), "The feature dimensions are not equal!")
    DF1.as("a").join(DF2.as("b"), expr("a.index = b.index"), "inner")
      .drop(expr("b.index"))
      .withColumn("temp", expr("a.value + b.value"))
      .drop(expr("a.value")).drop(expr("b.value")).withColumnRenamed("temp", "value")
  }

  /**
    * 用IDF对特征进行Scaling
    * @param DF DF("uid", "features")
    * @param threshold Double
    * @return DF("uid", "features")
    */
  def featureScalingByIDF(DF: DataFrame, threshold: Double): DataFrame = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    val df = DF.flatMap(r => {
      val uid = r.getAs[Long]("uid")
      val features = r.getAs[SparseVector]("features")
      val indices = features.indices
      val values = features.values
      indices.map(index => (uid, index, values(indices.indexOf(index))))
    }).toDF("uid", "feature_id", "feature_value")

    val totalUser = df.select("uid").distinct().count().toDouble

    val featureIDFMap = df.filter(s"feature_value > $threshold")
      .groupBy("feature_id").agg(collect_list("uid").as("uidList"))
      .map(r => {
        val feature_id = r.getAs[Int]("feature_id")
        val count = r.getAs[Seq[Long]]("uidList").size
        val feature_idf = if(count > 0) math.log(totalUser / count) else 1.0
        (feature_id, feature_idf)
      }).rdd.collectAsMap()

    val featureSize = DF.head().getAs[SparseVector]("features").size

    df.rdd.map(e => (e.getLong(0), (e.getInt(1), e.getDouble(2))))
      .groupByKey().map(e => {
      val uid = e._1
      val sequence = e._2.map(x => (x._1, featureIDFMap.getOrElse(x._1, 1.0) * x._2)).toSeq
      val features = Vectors.sparse(featureSize, sequence)
      (uid, features)
    }).toDF("uid", "features")
  }
}

