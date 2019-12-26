package cn.featureEngineering.dataUtils

import cn.moretv.doraemon.biz.util.BizUtils
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2018/11/7.
  */
object ETL {
  val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import ss.implicits._

  /**
    *
    * @param DF DF("sid", "meizi value")
    * @param fieldMap: Map[String, (String, String, Int, Int)]
    * @return
    */
  def split(DF: DataFrame, fieldMap: Map[String, (String, String, Int, Int)]): DataFrame = {
    if(DF.columns.length != 2) {
      throw new IllegalArgumentException("DataFrame必须是两列！")
    }
    val fieldName = DF.columns(1)
    val truncationNum = fieldMap.getOrElse(fieldName, ("error", "error", -1, -1))._4
    require(truncationNum != -1, s"fieldMap of $fieldName is undefined!")
    DF.rdd.map(row => {
      val sid = row.getString(0)
      val value = row.getString(1).split("\\|").filter(x => !x.equals("")).take(truncationNum)
      (sid, value ++ new Array[String](truncationNum - value.length).map(e => "missing"))
    })
      .toDF("sid", s"${DF.columns(1)}")
  }

  /**
    *
    * @param DF DF("uid or sid", Array["value"])
    * @param fieldMap Map[fieldName, (dataType, numericalType，uidTruncationNum, sidTruncationNum)]
    * @return DF("uid or sid", "fieldName", "numericalType", "dataType", "value", "rating")
    */
  def flat(DF: DataFrame, fieldMap: Map[String, (String, String, Int, Int)]): DataFrame = {
    if(DF.columns.length != 2) {
      throw new IllegalArgumentException("DataFrame必须是两列！")
    }

    val colName = DF.schema.fieldNames(1)
    val dataType = fieldMap.getOrElse(colName, ("error", "error", -1, -1))._1
    val numericalType = fieldMap.getOrElse(colName, ("error", "error", -1, -1))._2
    require(dataType != "error", s"fieldMap of $colName is undefined!")
    DF.rdd.map(row => (row.getString(0), row.getSeq[String](1)))
      .flatMap(e => e._2.map(x => (e._1, s"$colName", numericalType, dataType, x, x match { case "missing" => 0.0; case _ => 1.0 })))
      .toDF(s"${DF.columns(0)}", "fieldName", "numericalType", "dataType", "value", "rating")
  }

  /**
    *
    * @param DF DF("uid or sid", "fieldName", "numericalType", "dataType", "value", "rating")
    * @param fieldMap Map[fieldName, (dataType, numericalType，uidTruncationNum, sidTruncationNum)]
    * @return DF("uid or sid", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")
    */
  def truncation(DF: DataFrame, fieldMap: Map[String, (String, String, Int, Int)]): DataFrame = {
    val DfType = DF.columns(0)
    DF.rdd.map(r => ((r.getString(0), r.getString(1), r.getString(2), r.getString(3)), (r.getString(4), r.getDouble(5))))
      .groupByKey().map(e => {
      val fieldName = e._1._2
      val truncationNum = DfType match {
        case "uid" =>  fieldMap.getOrElse(fieldName, ("useless", "useless", 1, 1))._3
        case "sid" => fieldMap.getOrElse(fieldName, ("useless", "useless", 1, 1))._4
      }

      val truncatedArray = e._2.toArray.sortBy(-_._2).take(truncationNum)
      val result = truncatedArray ++: (new Array[(String, Double)](truncationNum - truncatedArray.length))
        .map(e => ("missing", 0.0))
      var index = 0

      (e._1, result.map(x => {index += 1; (x._1, x._2, index)}))
    })
      .flatMap(e => e._2.map(x => {
        val id = e._1._1
        val fieldName = e._1._2
        val numericalType = e._1._3
        val dataType = e._1._4

        val value = x._1
        val rating = x._2
        val fieldIndex = x._3
        (id, fieldName, fieldIndex, numericalType, dataType, value, rating)}))
      .toDF(s"$DfType", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")
  }

  /**
    *
    * @param DF
    * @param fieldMap Map[fieldName, (dataType, numericalType，uidTruncationNum, sidTruncationNum)]
    * @return DF("sid", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")
    */
  def meiZiDataETL(DF: DataFrame, fieldMap: Map[String, (String, String, Int, Int)]): DataFrame = {
    if(DF.columns.length < 2) {
      throw new IllegalArgumentException("DataFrame必须大于两列！")
    }

    val colsNames = DF.columns
    var unionDF = Array.empty[(String, String, Int, String, String, String, Double)].toList
      .toDF("sid", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")

    (1 until DF.columns.length).foreach(index => {
      val partData = DF.selectExpr(colsNames(0), colsNames(index))
      val splitDF = split(partData, fieldMap)
      val flatDF = flat(splitDF, fieldMap)
      val partDF = truncation(flatDF, fieldMap)
      unionDF = unionDF.union(partDF)
    })
    unionDF
  }

  /**
    *
    * @param videoFeature DF("sid", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")
    * @param scoreData DF("uid", "sid", "score")
    * @return DF("uid", "fieldName", "numericalType", "dataType", "value", "rating")
    */
  def userFeatureCal(videoFeature:DataFrame,
                     scoreData:DataFrame,
                     fieldMap: Map[String, (String, String, Int, Int)]): DataFrame = {
    val DF = scoreData.as("a").join(videoFeature.as("b"), expr("a.sid = b.sid"), "cross")
      .drop("sid", "id")
      .withColumn("rating", expr("a.score * a.score * b.rating"))
      .groupBy("uid", "fieldName", "numericalType", "dataType", "value")
      .agg(sum("rating").as("rating"))

    val result = truncation(DF, fieldMap).select("uid", "fieldName", "numericalType", "dataType", "value", "rating")
    result
  }

  /**
    * 特征投影
    * @param userDF DF("uid", "fieldName", "numericalType", "dataType", "value", "rating")
    * @param videoDF DF("sid", "fieldName", "fieldIndex", "numericalType", "dataType", "value", "rating")
    * @param scoreDF DF("uid", "sid", "score")
    * @return
    */
  def projection(userDF: DataFrame, videoDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    val userRDD = userDF.rdd.map(r => (r.getString(0), (r.getString(1), r.getString(2), r.getString(3), r.getString(4), r.getDouble(5))))
      .groupByKey().map(e => (e._1, e._2.toList)).persist(StorageLevel.DISK_ONLY)
    val videoRDD = videoDF.rdd.map(r => (r.getString(0), (r.getString(1), r.getInt(2), r.getString(3), r.getString(4), r.getString(5), r.getDouble(6))))
      .groupByKey().map(e => (e._1, e._2.toList)).persist(StorageLevel.DISK_ONLY)
    val scoreRDD = scoreDF.rdd.map(r => (r.getString(0), (r.getString(1), r.getDouble(2)))).persist(StorageLevel.DISK_ONLY)

    println(s"userRDD.count = ${userRDD.count()}")
    println(s"videoRDD.count = ${videoRDD.count()}")
    println(s"scoreRDD.count = ${scoreRDD.count()}")

    val splitNum = 10
    var unionDF = List.empty[(String, String, List[(String, Double)], Double)]
      .toDF("uid", "sid", "feature", "score")

    (1 to splitNum).foreach(split => {
      println(s"split = $split")
      val scoreSplit = scoreRDD.filter(e => e._1.toLong % splitNum == 0)
      val userSplit = userRDD.filter(e => e._1.toLong % splitNum == 0)

      val splitDF = scoreSplit.join(userSplit).map(e => {
        val uid = e._1
        val sid = e._2._1._1
        val score = e._2._1._2
        val userPrefer = e._2._2
        (sid, (uid, userPrefer, score))
      }).join(videoRDD).map(e => {
        val uid = e._2._1._1
        val sid = e._1
        val score = e._2._1._3
        val userPrefer = e._2._1._2
        val videoAttribute = e._2._2
        val feature = videoAttribute.map(x => {
          var rating = 0.0
          userPrefer.foreach(y => {
            if(x._1 == y._1 && x._5 == y._4) rating = x._6 * y._5
          })
          (x._1, x._2, x._3, x._4, x._5, rating)
        }).map(x => (x._1 + "_" + x._2, x._6)).sortBy(y => y._1)

        (uid, sid, feature, score)
      }).toDF("uid", "sid", "feature", "score")

      unionDF = unionDF.union(splitDF)
      println(s"unionDF.count = ${unionDF.count()}")
    })

    userRDD.unpersist()
    videoRDD.unpersist()
    scoreRDD.unpersist()
    unionDF
  }

  /**
    * 将特征按维度进行标准化
    * @param DF DF("uid", "sid", "features", "label")
    * @return DF("uid", "sid", "label", "features")
    */
  def normalization(DF: DataFrame): DataFrame = {
    val DF1 = DF.flatMap(r => r.getAs[Seq[Row]]("features").map(x => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[Double]("label"),
      x.getString(0), x.getDouble(1))))
      .toDF("uid", "sid", "label", "fieldName", "featureRating")

    val DF2 = DF.flatMap(r => r.getAs[Seq[Row]]("features").map(x => (x.getString(0), x.getDouble(1))))
      .toDF("fieldName", "featureRating").groupBy("fieldName").agg(max("featureRating").as("maxRating"), avg("featureRating").as("avgRating"))

    val DF3 = DF1.as("a").join(DF2.as("b"), expr("a.fieldName = b.fieldName"), "left").drop(expr("b.fieldName")).withColumn("featureRating", expr("featureRating / (maxRating + 0.000001)"))
      .distinct()
      .groupBy("uid", "sid", "label").agg(collect_list(concat_ws("%", col("fieldName"), col("featureRating"))).as("features"))
      .map(r => (r.getAs[String]("uid"), r.getAs[String]("sid"), r.getAs[Double]("label"),
        r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0), s.split("%")(1).toDouble)).sortBy(_._1)))
      .toDF("uid", "sid", "label", "features")

    DF3
  }

  /**
    * 将视频标签转化为特征
    * @param DF DF("tag", "sid", "rating")
    * @return DF("sid", "features")
    */
  def tag2VideoFeatures(DF: DataFrame): DataFrame = {
    val tags = DF.select("tag").distinct().map(r => r.getAs[String]("tag")).collect().sorted
    println(s"tags.length = ${tags.length}")

    DF.groupBy("sid").agg(collect_list(concat_ws("%", col("tag"), col("rating"))).as("features"))
        .map(r => {
          val sid = r.getAs[String]("sid")
          val featureMap = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
            .map(s => (s.split("%")(0), s.split("%")(1).toDouble)).toMap
          val features = tags.map(x => (x, featureMap.getOrElse(x, 0.0)))
          (sid, features)
        })
      .toDF("sid", "features")
  }

  /**
    *
    * @param videoFeaturesDF DF("sid", "features")
    * @param scoreDF DF("uid", "sid", "score")
    * @return
    */
  def union2userFeatures(videoFeaturesDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    scoreDF.as("a").join(videoFeaturesDF.as("b"), expr("a.sid = b.sid"), "inner").drop(expr("a.sid")).flatMap(r =>
      r.getAs[Seq[Row]]("features").map(e => (r.getAs[String]("uid"),
        e.getString(0), e.getDouble(1) * r.getAs[Double]("score") * r.getAs[Double]("score"))))
      .toDF("uid", "featureName", "featureRating")
      .groupBy("uid", "featureName").agg(sum("featureRating").as("featureRating"))
      .groupBy("uid").agg(collect_list(concat_ws("%", col("featureName"), col("featureRating"))).as("features"))
      .map(r => {
        val sid = r.getAs[String]("uid")
        val features = r.getAs[Seq[String]]("features").filter(s => s.split("%").length == 2)
          .map(s => (s.split("%")(0), s.split("%")(1).toDouble)).sortBy(_._1)

        (sid, features)
      })
      .toDF("uid", "features")
  }

  /**
    *
    * @param userFeaturesDF
    * @param videoFeaturesDF
    * @param scoreDF
    * @return
    */
  def featureProjection(userFeaturesDF: DataFrame, videoFeaturesDF: DataFrame, scoreDF: DataFrame): DataFrame = {
    val userFeaturesHead = userFeaturesDF.head.getAs[Seq[Row]]("userFeatures").map(e => (e.getString(0), e.getDouble(1))).sortBy(_._1)
    val videoFeaturesHead = videoFeaturesDF.head.getAs[Seq[Row]]("videoFeatures").map(e => (e.getString(0), e.getDouble(1))).sortBy(_._1)
    val joinFeatureNames = userFeaturesHead.map(e => e._1).distinct.toSet.intersect(videoFeaturesHead.map(e => e._1).distinct.toSet)

    userFeaturesDF.as("a").join(videoFeaturesDF.as("b"))
      .join(scoreDF.as("c"), expr("a.uid = c.uid and b.sid = c.sid"), "inner")
      .selectExpr("a.uid as uid", "b.sid as sid", "a.features as userFeatures", "b.features as videoFeatures", "c.score as label")
      .repartition(20000)
      .map(r => {
        val uid = r.getAs[String]("uid")
        val sid = r.getAs[String]("sid")
        val label = r.getAs[Double]("label")
        val userFeatures = r.getAs[Seq[Row]]("userFeatures").map(e => (e.getString(0), e.getDouble(1)))
        val videoFeatures = r.getAs[Seq[Row]]("videoFeatures").map(e => (e.getString(0), e.getDouble(1)))
        val joinedUserFeatures = userFeatures.filter(e => joinFeatureNames.contains(e._1))
        val joinedVideoFeatures = videoFeatures.filter(e => joinFeatureNames.contains(e._1))
        val features = joinedUserFeatures.map(e => {
          val index = joinedUserFeatures.indexOf(e)
          val featureName = e._1
          val score = e._2 * joinedVideoFeatures(index)._2
          (featureName, score)
        })

        (uid, sid, features, label)
      }).toDF("uid", "sid", "features", "label")
  }
}
