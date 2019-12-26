package cn.featureEngineering.featureExtraction

import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.{HdfsPath, MysqlPath}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap

/**
  * Created by Edison_Zhou on 2019/09/18.
  */
object VideoMetadataFeature extends BaseClass{
  val ss:SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
  import ss.implicits._
  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    //说明：videoType字段与parentID相关联，如videoType为单剧集或剧头，则parentID为0，
    // 因在查询过滤条件中会限定类型及相关videoType，故无需在特征中再体现parentID字段
    val featureInfoOfValidProgram = DataReader.read(new MysqlPath("bigdata-appsvr-130-4", 3306,
      "tvservice", "mtv_program", "bislave", "slave4bi@whaley",
      Array("sid", "year", "cast", "director", "actor", "duration", "tags", "area", "score", "localism", "language",
        "supply_type", "risk_flag"), "contentType = 'movie' and videoType = 0 and type = 1 and status = 1"))

    val featureProcessing = featureInfoOfValidProgram.map(r => {
      val sid = r.getString(0)
      val year = if (r.getInt(1)==209) 2019 else r.getInt(1)
      val castArr = if (r.getString(2) ==""|| r.getString(2) == null) Array("missing") else r.getString(2).split("\\|")
      val directorArr = if (r.getString(3) ==""|| r.getString(3) == null) Array("missing") else r.getString(3).split("\\|")
      val actorArr = if (r.getString(4) ==""|| r.getString(4) == null) Array("missing") else r.getString(4).split("\\|")
      val duration = if (r.getInt(5) > 220) 90 else r.getInt(5)
      val tagsArr = if (r.getString(6) ==""|| r.getString(6) == null) Array("missing") else r.getString(6).split("\\|")
      val area = if (r.getString(7) =="null"|| r.getString(7) == null) "" else if (r.getString(7).contains("|") && r.getString(7).contains("/"))
        r.getString(7).replaceAll("\\|", "") else if (r.getString(7).contains("，") && !r.getString(7).contains("|"))
        r.getString(7).replaceAll("，", "/") else r.getString(7).replaceAll("\"", "")
        .replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("&nbsp;", "")
        .replaceAll(";", "/").replaceAll("。", "/").replaceAll(" ", "")
      val areaArr = if (area == "") Array("missing") else if (area.contains("/")) area.split("\\/") else if (area.contains("|"))
        area.split("\\|") else Array(area)
      var allEleSingleChar = true
      for (i <- areaArr if allEleSingleChar) {if (i.length != 1) allEleSingleChar = false}
      val resAreaArr = if (allEleSingleChar) {
        var resStr = ""
        for (i <- areaArr) resStr += i
        Array(resStr)
      } else areaArr
      val score = r.getDouble(8)
      val isCantonese = r.getInt(9)
      val language = if (r.getString(10) =="null" || r.getString(10) == null) "" else r.getString(10).replaceAll("\"", "")
        .replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "")
      val languageArr = if (language =="" || language == null) Array("missing") else if (language.contains(",")) language.split(",")
      else if (language.contains("|")) language.split("\\|") else Array(language)
      val supplyType = if (r.getString(11) ==""|| r.getString(11) == null) "free" else r.getString(11)
      val isVip = if (supplyType == "vip") 1 else 0
      val riskFlag = if (r.getInt(12)==1) 0.5 else if (r.getInt(12)==2) 1 else 0
      (sid, year, castArr.distinct, directorArr.distinct, actorArr.distinct, duration, tagsArr.distinct,
        resAreaArr.distinct, score, isCantonese, languageArr.distinct, isVip, riskFlag)
    }).toDF("sid", "year", "castArr", "directorArr", "actorArr", "duration", "tagsArr", "areaArr",
      "score", "isCantonese", "languageArr", "isVip", "riskFlag").persist(StorageLevel.MEMORY_AND_DISK)

    BizUtils.getDataFrameInfo(featureProcessing, "featureProcessing")


    val allCastArr = featureProcessing.select("castArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of castArr is " + allCastArr.length)
    val castHashMap = enumeration(allCastArr)
    val allDirectorArr = featureProcessing.select("directorArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of directorArr is " + allDirectorArr.length)
    val directorHashMap = enumeration(allDirectorArr)
    val allActorArr = featureProcessing.select("actorArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of actorArr is " + allActorArr.length)
    val actorHashMap = enumeration(allActorArr)
    val allTagsArr = featureProcessing.select("tagsArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of tagsArr is " + allTagsArr.length)
    val tagsHashMap = enumeration(allTagsArr)
    val allAreaArr = featureProcessing.select("areaArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of areaArr is " + allAreaArr.length)
    val areaHashMap = enumeration(allAreaArr)
    val allLanguageArr = featureProcessing.select("languageArr").flatMap(r => r.getSeq[String](0)).collect().distinct
    println("The length of languageArr is " + allLanguageArr.length)
    val languageHashMap = enumeration(allLanguageArr)

    val maxDuration = featureProcessing.select("duration").map(r => r.getInt(0)).collect().max
    println(maxDuration)
    val maxYear = featureProcessing.select("year").map(r => r.getInt(0)).collect().max
    val minYear = featureProcessing.select("year").filter("year != 0").map(r => r.getInt(0)).collect().min
    println(maxYear)
    println(minYear)

    val castStartIndex = 1
    val directorStartIndex = castStartIndex + allCastArr.length
    val actorStartIndex = directorStartIndex + allDirectorArr.length
    val durationStartIndex = actorStartIndex + allActorArr.length
    val tagsStartIndex = durationStartIndex + 1
    val areaStartIndex = tagsStartIndex + allTagsArr.length
    val scoreStartIndex = areaStartIndex + allAreaArr.length
    val isCantoneseStartIndex = scoreStartIndex + 1
    val languageStartIndex = isCantoneseStartIndex + 1
    val isVipStartIndex = languageStartIndex + allLanguageArr.length
    val riskFlagStartIndex = isVipStartIndex + 1
    println("directorStartIndex: " + directorStartIndex)
    println("actorStartIndex: " + actorStartIndex)
    println("durationStartIndex: " + durationStartIndex)
    println("tagsStartIndex: " + tagsStartIndex)
    println("areaStartIndex: " + areaStartIndex)
    println("scoreStartIndex: " + scoreStartIndex)
    println("isCantoneseStartIndex: " + isCantoneseStartIndex)
    println("languageStartIndex: " + languageStartIndex)
    println("isVipStartIndex: " + isVipStartIndex)
    val castHashMapNewIndex = enumeration(allCastArr, castStartIndex)
    val directorHashMapNewIndex = enumeration(allDirectorArr, directorStartIndex)
    val actorHashMapNewIndex = enumeration(allActorArr, actorStartIndex)
    val tagsHashMapNewIndex = enumeration(allTagsArr, tagsStartIndex)
    val areaHashMapNewIndex = enumeration(allAreaArr, areaStartIndex)
    val languageHashMapNewIndex = enumeration(allLanguageArr, languageStartIndex)


    val normalizedFeature = featureProcessing.map(r => {
      val sid = r.getString(0)
      val normalizedYear = 0.9*(r.getInt(1)-minYear)/(maxYear-minYear) + 0.1
      val castSeq = r.getSeq[String](2).map(e => castHashMap.getOrElse(e, allCastArr.length)).map(e => (e, 1.0))
      val castSparseVec = Vectors.sparse(allCastArr.length, castSeq)
      val directorSeq = r.getSeq[String](3).map(e => directorHashMap.getOrElse(e, allDirectorArr.length)).map(e => (e, 1.0))
      val directorSparseVec = Vectors.sparse(allDirectorArr.length, directorSeq)
      val actorSeq = r.getSeq[String](4).map(e => actorHashMap.getOrElse(e, allActorArr.length)).map(e => (e, 1.0))
      val actorSparseVec = Vectors.sparse(allActorArr.length, actorSeq)
      val normalizedDuration = r.getInt(5).toDouble / maxDuration
      val tagsSeq = r.getSeq[String](6).map(e => tagsHashMap.getOrElse(e, allTagsArr.length)).map(e => (e, 1.0))
      val tagsSparseVec = Vectors.sparse(allTagsArr.length, tagsSeq)
      val areaSeq = r.getSeq[String](7).map(e => areaHashMap.getOrElse(e, allAreaArr.length)).map(e => (e, 1.0))
      val areaSparseVec = Vectors.sparse(allAreaArr.length, areaSeq)
      val originalScore = if (r.getDouble(8) == 0) {math.random*2+3.5} else r.getDouble(8)
      val normalizedScore = if (originalScore*100%10 >= 5) (originalScore*10+1).toInt/100.0 else (originalScore*10).toInt/100.0
      val isCantonese = r.getInt(9)
      val languageSeq = r.getSeq[String](10).map(e => languageHashMap.getOrElse(e, allLanguageArr.length)).map(e => (e, 1.0))
      val languageSparseVec = Vectors.sparse(allLanguageArr.length, languageSeq)
      val isVip = r.getInt(11)
      val riskFlag = r.getDouble(12)
      (sid, normalizedYear, castSparseVec, directorSparseVec, actorSparseVec, normalizedDuration, tagsSparseVec, areaSparseVec,
        normalizedScore, isCantonese, languageSparseVec, isVip, riskFlag)
    }).toDF("sid", "year", "cast", "director", "actor", "duration", "tags", "area", "score", "isCantonese", "language", "isVip", "riskFlag")

    println("normalizedFeature.count():"+normalizedFeature.count())
    println("normalizedFeature.printSchema:")
    normalizedFeature.printSchema()
    normalizedFeature.show(100, false)

    val movieMetadataSparseFeature = featureProcessing.map(r => {
      val sid = r.getString(0)
      val year = 0.9*(r.getInt(1)-minYear)/(maxYear-minYear) + 0.1
      val castSeq = r.getSeq[String](2).map(e => castHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val directorSeq = r.getSeq[String](3).map(e => directorHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val actorSeq = r.getSeq[String](4).map(e => actorHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val duration = r.getInt(5).toDouble / maxDuration
      val tagsSeq = r.getSeq[String](6).map(e => tagsHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val areaSeq = r.getSeq[String](7).map(e => areaHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val originalScore = if (r.getDouble(8) == 0) {math.random*2+3.5} else r.getDouble(8)
      val score = if (originalScore*100%10 >= 5) (originalScore*10+1).toInt/100.0 else (originalScore*10).toInt/100.0
      val isCantonese = r.getInt(9).toDouble
      val languageSeq = r.getSeq[String](10).map(e => languageHashMapNewIndex.getOrElse(e, 0)).map(e => (e, 1.0))
      val isVip = r.getInt(11).toDouble
      val riskFlag = r.getDouble(12)
      val featureSequence = Seq[(Int, Double)]((0, year)) ++ castSeq ++ directorSeq ++ actorSeq ++ Seq[(Int, Double)]((durationStartIndex, duration)) ++
        tagsSeq ++ areaSeq ++ Seq[(Int, Double)]((scoreStartIndex, score)) ++ Seq[(Int, Double)]((isCantoneseStartIndex, isCantonese)) ++
        languageSeq ++ Seq[(Int, Double)]((isVipStartIndex, isVip)) ++ Seq[(Int, Double)]((riskFlagStartIndex, riskFlag))
      val vectorlength = riskFlagStartIndex + 1
      (sid, Vectors.sparse(vectorlength, featureSequence))
    }).toDF("sid", "features")

    println("movieMetadataSparseFeature.count():"+movieMetadataSparseFeature.count())
    println("movieMetadataSparseFeature.printSchema:")
    movieMetadataSparseFeature.printSchema()
    movieMetadataSparseFeature.show(100, false)

    BizUtils.outputWrite(movieMetadataSparseFeature, "featureEngineering/metadata/videoFeature")


    /*val fillingData = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/fillingData/ALS/Latest"))
      .selectExpr("cast (uid as string)", "sid", "case when score >= 1 then 1.0 else score end as score")
      .filter("score >= 0.8")
      .withColumn("score", col("score") * 0.6)*/

      /**
        * 稀疏向量生成
        */
      /*val videoTags = dataUtils.Read.getVideoTag2("movie").persist(StorageLevel.MEMORY_ONLY)

      val tagReIndexDF = ETLSparse.tagReIndex(videoTags.select("tag_id", "tag_name"))

    BizUtils.outputWrite(tagReIndexDF, "featureEngineering/tagReIndex")

      val videoFeaturesDF = ETLSparse.tagReIndex2VideoFeatures(videoTags.select("tag_id", "sid", "rating"), tagReIndexDF)
        .persist(StorageLevel.MEMORY_ONLY)
      //BizUtils.getDataFrameInfo(videoFeaturesDF, "videoFeaturesDF")
      BizUtils.outputWrite(videoFeaturesDF, "featureEngineering/features/videoFeaturesReIndexSparseAll")

      val userFeaturesDF = ETLSparse.union2userFeatures(videoFeaturesDF, trainData)
        .persist(StorageLevel.MEMORY_ONLY)
      //BizUtils.getDataFrameInfo(userFeaturesDF, "userFeaturesDF")
      BizUtils.outputWrite(userFeaturesDF, "featureEngineering/features/userFeaturesReIndexSparseD10")*/

    /*val videoFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/videoFeaturesReIndexSparseAll/Latest"))

    val userFeaturesDF = DataReader.read(new HdfsPath("/ai/tmp/output/test/medusa/featureEngineering/features/userFeaturesReIndexSparseD10/Latest"))

    val projectionFeaturesDF = ETLSparse.featureProjection(userFeaturesDF, videoFeaturesDF, testData)
    //BizUtils.getDataFrameInfo(projectionFeatures, "projectionFeatures")
    //userFeaturesDF.unpersist()
    //videoFeaturesDF.unpersist()

    //BizUtils.getDataFrameInfo(projectionFeaturesDF, "projectionFeaturesDF")
    BizUtils.outputWrite(projectionFeaturesDF, "featureEngineering/features/projectionFeaturesReIndexSparseTestD10_2")*/


    /*val intersectedFeaturesDF = ETLSparse.featureIntersection(normalizedFeaturesDF)
      .persist(StorageLevel.MEMORY_ONLY)
    BizUtils.getDataFrameInfo(intersectedFeaturesDF, "intersectedFeaturesDF")
    BizUtils.outputWrite(intersectedFeaturesDF, "featureEngineering/features/intersectedFeaturesSparse6000")*/

    /**
      * tag_id，tag_name映射
      */
    /*val tagDF = dataUtils.Read.getVideoTag2("movie").select("tag_id", "tag_name").distinct()
    BizUtils.outputWrite(tagDF, "featureEngineering/movieTag")*/

  }

  def enumeration(arrayToBeNumbered: Array[String], startIndex: Int = 0):HashMap[String, Int] = {
    var newIndex = startIndex
    val orderedHashMap = HashMap[String, Int]()
    for (i <- arrayToBeNumbered.indices) {
      orderedHashMap.put(arrayToBeNumbered(i), newIndex)
      newIndex += 1
    }
    orderedHashMap
  }


}
