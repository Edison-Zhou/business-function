package cn.moretv.doraemon.biz.search

import cn.moretv.doraemon.common.enum.EnvEnum
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

import scala.collection.mutable


class EsForeachWriter(alg: String, biz: String)(implicit env: EnvEnum.Value, spark: SparkSession) extends ForeachWriter[Row] {

  val mapper = new ObjectMapper()
  val resultMapLowRisk = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
  val resultMapMiddleRisk = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
  val resultMapHighRisk = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
  val resultMapLowRiskDelete = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
  val resultMapMiddleRiskDelete = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()
  val resultMapHighRiskDelete = mutable.HashMap[String, mutable.HashMap[String, Seq[String]]]()


  def open(partitionId: Long, version: Long): Boolean = {
    println("开始输出")
    true
  }

  def process(row: Row): Unit = {
    val searchKey = row.getAs[String]("searchKey")
    val contentType = row.getAs[String]("contentType")

    val content = row.getAs[Seq[String]]("content")
      .map(s => s.split("_")) //格式 sid_highlight_score_riskFlag
      .filter(s => s.length == 4)

    val contentAdd = content.filter(s => s(1).nonEmpty) //highlight不为空

    val contentDelete = content.filter(s => s(1).isEmpty) //highlight为空

    val eachKeyLowRisk = resultMapLowRisk.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())
    val eachKeyMiddleRisk = resultMapMiddleRisk.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())
    val eachKeyHighRisk = resultMapHighRisk.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())

    val contentHigh = contentAdd.filter(s => (s(3).toInt + 2) <= 2)
      .map(s => s(0) + "_" + s(1) + "_" + s(2))

    val contentMiddle = contentAdd.filter(s => (s(3).toInt + 1) <= 2)
      .map(s => s(0) + "_" + s(1) + "_" + s(2))

    val contentLow = contentAdd.filter(s => (s(3).toInt + 0) <= 2)
      .map(s => s(0) + "_" + s(1) + "_" + s(2))

    eachKeyLowRisk.put(contentType, eachKeyLowRisk.getOrElse(searchKey, Seq[String]()) ++ contentLow)
    eachKeyMiddleRisk.put(contentType, eachKeyMiddleRisk.getOrElse(searchKey, Seq[String]()) ++ contentMiddle)
    eachKeyHighRisk.put(contentType, eachKeyHighRisk.getOrElse(searchKey, Seq[String]()) ++ contentHigh)

    val eachKeyLowRiskDelete = resultMapLowRiskDelete.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())
    val eachKeyMiddleRiskDelete = resultMapMiddleRiskDelete.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())
    val eachKeyHighRiskDelete = resultMapHighRiskDelete.getOrElseUpdate(searchKey, mutable.HashMap[String, Seq[String]]())

    val contentHighDelete = contentDelete.filter(s => (s(3).toInt + 2) <= 2)
      .map(s => s(0))

    val contentMiddleDelete = contentDelete.filter(s => (s(3).toInt + 1) <= 2)
      .map(s => s(0))

    val contentLowDelete = contentDelete.filter(s => (s(3).toInt + 0) <= 2)
      .map(s => s(0))

    eachKeyLowRiskDelete.put(contentType, eachKeyLowRiskDelete.getOrElse(searchKey, Seq[String]()) ++ contentLowDelete)
    eachKeyMiddleRiskDelete.put(contentType, eachKeyMiddleRiskDelete.getOrElse(searchKey, Seq[String]()) ++ contentMiddleDelete)
    eachKeyHighRiskDelete.put(contentType, eachKeyHighRiskDelete.getOrElse(searchKey, Seq[String]()) ++ contentHighDelete)

  }

  def close(errorOrNull: Throwable): Unit = {
    (resultMapLowRisk.keys ++ resultMapLowRiskDelete.keys).toSeq.distinct.foreach(key => {
      SearchUtil.updateSearchKey(key, "moretv_search_low_risk", "IndexKeyWordKey:MINDEXLOW_", biz, alg,
        resultMapLowRisk.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap,
        resultMapLowRiskDelete.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap)
    })
    (resultMapMiddleRisk.keys ++ resultMapMiddleRiskDelete.keys).toSeq.distinct.foreach(key => {
      SearchUtil.updateSearchKey(key, "moretv_search_middle_risk", "IndexKeyWordKey:MINDEXHIGH_", biz, alg,
        resultMapMiddleRisk.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap,
        resultMapMiddleRiskDelete.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap)
    })
    (resultMapHighRisk.keys ++ resultMapHighRiskDelete.keys).toSeq.distinct.foreach(key => {
      SearchUtil.updateSearchKey(key, "moretv_search_high_risk", "IndexKeyWordKey:MINDEXSUPERHIGH_", biz, alg,
        resultMapHighRisk.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap,
        resultMapHighRiskDelete.getOrElse(key, mutable.HashMap[String, Seq[String]]()).toMap)
    })
  }
}
