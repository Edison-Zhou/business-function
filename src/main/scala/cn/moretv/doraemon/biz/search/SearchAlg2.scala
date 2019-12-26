package cn.moretv.doraemon.biz.search

import com.github.stuxuhai.jpinyin.{PinyinFormat, PinyinHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json.JSONObject

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by stephen on 2019/9/18.
  */
@deprecated
object SearchAlg2 {


  /**
    * 输入多种来源的数据，提取对应搜索词，把搜索词转换成拼音，并且计算匹配分
    * @param longVideoDf 字段 sid，contentType，riskFlag， title, associativeWord
    * @param subjectDf  字段code，title, associativeWord
    * @param starDf  字段sid，name, associativeWord
    * @param spark
    * @return DF(sid, contentType, riskFlag, searchKey, highlight, matchScore, associativeWord, title)
    */
  def recall(longVideoDf: DataFrame, shortVideoDf: DataFrame, subjectDf: DataFrame, starDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val longVideoSearchWordDf = longVideoDf.map(r => {
      val sid = r.getAs[String]("sid")
      val contentType = r.getAs[String]("contentType")
      val riskFlag = r.getAs[Int]("riskFlag")
      val associativeWord = r.getAs[String]("associativeWord")
      val title = r.getAs[String]("title")
      val searchWord = if(associativeWord.equals(title)) {
        SearchAlg2.getLongVideoSearchWord(associativeWord, 20)
      } else {
        Array[(String, Double)]((associativeWord, 1.0))
      }

      (sid, contentType, riskFlag, searchWord, associativeWord, title)
    })
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k._1, k._2, s._5, s._6)))
    //(sid, contentType, 危险等级, 搜索词，搜索词匹配分数, 联想词, 标题)

    val shortVideoSearchWordDf = shortVideoDf.map(r => {
      val sid = r.getAs[String]("sid")
      val contentType = r.getAs[String]("contentType")
      val riskFlag = r.getAs[Int]("riskFlag")
      val title = r.getAs[String]("title")
      val keyWord = SearchAlg2.getKeyword(title)

      (sid, contentType, riskFlag, keyWord, title)
    })
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k, 0.5, k, s._5)))

    val subjectSearchWordDf = subjectDf.map(r => {
      val sid = r.getAs[String]("code")
      val contentType = "subject"
      val riskFlag = r.getAs[Int]("riskFlag")
      val associativeWord = r.getAs[String]("associativeWord")
      val title = r.getAs[String]("title")
      val searchWord = if(associativeWord.equals(title)) {
        SearchAlg2.getLongVideoSearchWord(associativeWord, 20)
      } else {
        Array[(String, Double)]((associativeWord, 1.0))
      }

      (sid, contentType, riskFlag, searchWord, associativeWord, title)
    })
      .flatMap(s => s._4.map(k => (Base64Util.base64Encode(s._1), s._2, s._3, k._1, k._2, s._5, s._6)))

    val starVideoSearchWordDf = starDf.map(r => {
      val sid = r.getAs[String]("sid")
      val contentType = "personV2"
      val riskFlag = 0
      val searchWord = r.getAs[String]("name").replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9]+", "")
      val wordMatchScore = 1.0
      val associativeWord = r.getAs[String]("associativeWord")
      val title = searchWord
      (sid, contentType, riskFlag, searchWord, wordMatchScore, associativeWord, title)
    })

    val searchWordUnionDf = longVideoSearchWordDf.union(shortVideoSearchWordDf).union(subjectSearchWordDf).union(starVideoSearchWordDf)
      .repartition(50)

    val searchWordDf = searchWordUnionDf
      .map(s => (s._1, s._2, s._3, SearchAlg2.splitPinyinAndGetScoreArray(s._4), s._5, s._6, s._7))
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k._1, k._2, s._5 * 0.7 + k._3 * 0.3, s._6, s._7)))
      .toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore", "associativeWord", "title")
    //搜索词匹配分数*0.7 + 拼音完整度*0.3

    searchWordDf.dropDuplicates("sid", "searchKey")
  }

  /**
    *
    * @param searchWordDf DF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore", "associativeWord", "title")
    * @param videoScore DF("sid", "video_score")
    * @param clickScore DF("sid", "click_score")
    * @param spark
    * @return
    */
  def computeScore(searchWordDf: DataFrame, videoScore: DataFrame, clickScore: DataFrame)(implicit spark: SparkSession): DataFrame = {
    searchWordDf.as("a").join(videoScore.as("b"), expr("a.sid = b.sid"), "leftouter")
        .join(clickScore.as("c"), expr("a.sid = c.sid"), "leftouter")
      .selectExpr("a.sid as sid", "contentType", "riskFlag", "searchKey", "highlight",
        "matchScore",
        "case when video_score is null then 0 else video_score end as video_score",
        "case when click_score is null then 0 else video_score end as click_score",
        "associativeWord", "title")
      .selectExpr("sid", "contentType", "riskFlag", "searchKey", "highlight",
        "0.3 * matchScore + 0.4 * video_score + 0.3 * click_score",
        "associativeWord", "title")
  }

  /**
    * 利用评分进行排序，保证各危险等级的数量
    * @param beforeReorderDf
    * @param spark
    * @return
    */
  def reorder(beforeReorderDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val orderDf = beforeReorderDf.
      map(r => (r.getAs[String]("searchKey"), r.getAs[String]("contentType"), reorderContent(r.getAs[Seq[String]]("content"))))
      .toDF("searchKey", "contentType", "content")
    orderDf
  }

  /**
    * 对指定搜索词，指定类型的节目进行排序
    * @param input
    * @return
    */
  private def reorderContent(input: Seq[String]*): Seq[String] = {
    val allInput = input.fold(Seq[String]())((a, i) => a ++ i)
      .map(s => s.split("_"))
      .filter(s => s.length == 6)

    val array = allInput.filter(s => s(1).trim.nonEmpty).distinct // highlight字段不能为空

    val deleteArray = allInput.filter(s => s(1).trim.isEmpty).distinct //highlight字段为空的是要被删除的sid（仅在流式时）

    val lowRisk = array
      .filter(s => s(3).toInt == 0)
      .sortBy(s => -1 * s(2).toDouble).take(25)

    val middleRisk = array.filter(s => s(3).toInt <= 1)
      .sortBy(s => -1 * s(2).toDouble).take(25)

    val highRisk = array.filter(s => s(3).toInt <= 2)
      .sortBy(s => -1 * s(2).toDouble).take(25)

    val union = (lowRisk ++ middleRisk ++ highRisk ++ deleteArray).sortBy(s => -1 * s(2).toDouble)
    val sidSet = ArrayBuffer.empty[String]

    union.map(s => {
      val sid = s(0)
      if(!sidSet.contains(sid)) {
        sidSet += sid
        s.mkString("_")
      }else{ //用于过滤重复sid
        "%_%_%_%_%_%"
      }
    }).filter(s => !s.equals("%_%_%_%_%_%"))
  }

  /**
    * 把指定的搜索词转换成拼音(改用支持多音字的方法)，并计算得分
    * 拼音包括首字母和全拼前缀
    * @param searchWord
    * @return 格式 （拼音，高亮，得分）
    */
  def splitPinyinAndGetScoreArray(searchWord: String): Array[(String, String, Double)] = {
    if (searchWord == null || searchWord.trim.isEmpty) {
      return Array()
    }
    var pinyinScore = new ArrayBuffer[(String, String, Double)]()
    val pinyinArr = ObtainPinYinUtils.polyPinYinQuanPin(searchWord).toArray
    //调上述方法后,"如懿传第2季"得到的结果为['ru yi chuan di 2 ji', 'ru yi zhuan di 2 ji']
    pinyinArr.foreach(pinyin => {
      val pinyins = pinyin.toString.split(" ")
      //得到['ru', 'yi', 'zhuan', 'di', '2', 'ji']
      val shortPinyins = pinyins.map(s => s.substring(0, 1))
      //得到['r','y','z','d','2','j']

      pinyinScore = pinyinScore ++ (getPinyinScore(pinyins, searchWord) ++ getPinyinScore(shortPinyins, searchWord)).toArray
    })
    pinyinScore.toArray.distinct
  }

  /**
    * 对一个搜索词对应的拼音，拆分成不同长度的前缀，并且计算匹配分
    * @param pinyins 一个搜索词对应的拼音，数组的每个元素对应一个字的拼音
    * @param searchWord 搜索词
    * @return  List['searchKey', 'highLight', '拼音完整度分数']
    */
  private def getPinyinScore(pinyins: Array[String], searchWord: String): List[(String, String, Double)] = {
    val pinyinCount = pinyins.length
    var count = 0
    val str = new StringBuilder
    val list = new ListBuffer[(String, String, Double)]
    pinyins.foreach(p => {
      count = count + 1
      val score = count * 1.0 / pinyinCount
      list.+=((str.toString + p, searchWord.substring(0, count), score))
      if (p.length > 1) {
        list.+=((str.toString + p.substring(0, 1), searchWord.substring(0, count), score))
      }
      str.append(p)
    })

    list.toList
  }

  //长视频标题切词，标题最多取15个字
  def getLongVideoSearchWord(title: String, maxLength: Int): Array[(String, Double)] = {
    val title1 = title.replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9]+", "")
    val title2 = title1.substring(0, math.min(maxLength, title1.size))
    //清除掉所有特殊字符，仅保留中文、数字、字母
    val wordSeg = getWordSeg(title2)
    //限定最大长度,若title＜最大长度选title长度进行分词,否则用最大长度分
    if (wordSeg == null || wordSeg.isEmpty) {
      println(s"title = $title2")
      require(wordSeg == null || wordSeg.isEmpty, "分词结果为空")
      new Array[(String, Double)](0)
    } else {
      val segIndex = new Array[Int](wordSeg.length)
      //wordSeg为title传入接口得到的分词结果所组成的List[String],new Array[Int]的元素初始值为0
      val wordSize = wordSeg.map(_.length)
      (1 until wordSeg.length).foreach(index => segIndex(index) = segIndex(index - 1) + wordSize(index - 1))
      //这一步为核心,segIndex中首个元素为0,此后依次为0+第一个分词结果的长度;再加第二个;再加第三个...直加到倒数第二个的总长度
      //下一步取字符串发现,分别得到整个title2,第二个分词到最后的部分title, ...直到最后一个分词
      // 不用考虑最后一个词加上后的长度,因为加上后是全部title,title2.substring(i)的结果为null
      segIndex.map(i => (title2.substring(i), 1 - i * 1.0 / title2.length))
    }
  }

  private def getWordSeg(title: String): List[String] = {
    val regex = "[^\\u4e00-\\u9fa5a-zA-Z0-9]+"
    val url = "http://nlp.moretv.cn/wordseg?text=" + title.replaceAll(regex, "")
    val list: ListBuffer[String] = new ListBuffer[String]()
    try {
      val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
      val jsonArray = json.getJSONArray("wordseg")
      (0 until jsonArray.length).foreach(index => list.+=(jsonArray.getString(index)))
      list.toList
    } catch {
      case _: Exception => list.toList
    }
  }

  //短视频获取关键词
  private def getKeyword(title: String): List[String] = {
    val regex = "[^\\u4e00-\\u9fa5a-zA-Z0-9]+"
    val url = "http://nlp.moretv.cn/keyword?text=" + title.replaceAll(regex, "")
    val list: ListBuffer[String] = new ListBuffer[String]()
    try {
      val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
      val jsonArray = json.getJSONArray("keyword")
      (0 until jsonArray.length).foreach(index => list.+=(jsonArray.getString(index)))
      list.toList
    } catch {
      case _: Exception => list.toList
    }
  }
}


