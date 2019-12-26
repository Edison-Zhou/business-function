package cn.moretv.doraemon.biz.search

import cn.moretv.doraemon.biz.BaseClass
import com.github.stuxuhai.jpinyin.{PinyinFormat, PinyinHelper}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json.JSONObject
import java.net.URLEncoder

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by lituo on 2018/11/16.
  */
object SearchAlg{


  /**
    * 输入多种来源的数据，提取对应搜索词，把搜索词转换成拼音，并且计算匹配分
    *
    * @param longVideoDf 字段 sid，contentType，riskFlag， title
    * @param subjectDf   字段code，title
    * @param starDf      字段sid，name
    * @param spark
    * @return
    */
  def recall(longVideoDf: DataFrame, shortVideoDf: DataFrame, subjectDf: DataFrame, starDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val longVideoSearchWordDf = longVideoDf.map(r => (r.getAs[String]("sid"), r.getAs[String]("contentType"),
      r.getAs[Int]("riskFlag"), SearchAlg.getLongVideoSearchWord(r.getAs[String]("title"), 20)))
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k._1, k._2)))
    //(sid, contentType, 危险等级, 搜索词SearchWord，搜索词匹配分数)

    val shortVideoSearchWordDf = shortVideoDf.map(r => (r.getAs[String]("sid"), r.getAs[String]("contentType"),
      r.getAs[Int]("riskFlag"), SearchAlg.getKeyword(r.getAs[String]("title"))))
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k, 0.5)))

    val subjectSearchWordDf = subjectDf.map(r => (r.getAs[String]("code"), "subject",
      r.getAs[Int]("riskFlag"), SearchAlg.getLongVideoSearchWord(r.getAs[String]("title"), 10)))
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k._1, k._2)))

    val starVideoSearchWordDf = starDf.map(r => (r.getAs[String]("sid"), "personV2", 0, r.getAs[String]("name")
      .replaceAll("[^\\u4e00-\\u9fa5a-zA-Z0-9]+", ""), 1.0))

    val searchWordUnionDf = longVideoSearchWordDf.union(shortVideoSearchWordDf).union(subjectSearchWordDf).union(starVideoSearchWordDf)
      .repartition(50)

    val searchWordDf = searchWordUnionDf
      .map(s => (s._1, s._2, s._3, SearchAlg.splitPinyinAndGetScoreArray(s._4), s._5))
      .flatMap(s => s._4.map(k => (s._1, s._2, s._3, k._1, k._2, s._5 * 0.7 + k._3 * 0.3)))
      //此处严格意义上为searchWord而非searchKey
      .toDF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore")
    //搜索词匹配分数*0.7 + 拼音完整度*0.3

    searchWordDf.dropDuplicates("sid", "searchKey")
  }

  def recall4AssociativeWord(longVideoDf: DataFrame, subjectDf: DataFrame, starDf: DataFrame,
                             programPersonMappingDf: DataFrame)(implicit spark: SparkSession):DataFrame = {
    import spark.implicits._
    val longVideoAssociativeWordDf = longVideoDf.map(r => (r.getAs[String]("sid"), r.getAs[String]("title"),
      r.getAs[String]("contentType"), r.getAs[Int]("riskFlag"),
      SearchAlg.getAssociativeWordArray(r.getAs[String]("title"), 20)))
      .flatMap(r => r._5.map(e => (r._1, r._2, r._3, r._4, e)))


    val subjectAssociativeWordDf = subjectDf.map(r => (r.getAs[String]("code"), r.getAs[String]("title"),"subject",
      r.getAs[Int]("riskFlag"), SearchAlg.getAssociativeWordArray(r.getAs[String]("title"), 10)))
      .flatMap(r => r._5.map(e => (r._1, r._2, r._3, r._4, e)))

    val starDetailDf = starDf.map(r => (r.getAs[String]("sid"), r.getAs[String]("name"), "personV2",
      0, r.getAs[String]("name")))

    val programPersonAssociatedDf = programPersonMappingDf.map(r => (r.getAs[String]("sid"), r.getAs[String]("title"),
      r.getAs[String]("contentType"), r.getAs[Int]("riskFlag"), r.getAs[String]("name")))

    val associativeWordUnionDf = longVideoAssociativeWordDf.union(subjectAssociativeWordDf)
      .union(starDetailDf).union(programPersonAssociatedDf).repartition(50)

    val associativeWordDf = associativeWordUnionDf.map(r => (r._1, r._2, r._3, r._4, r._5, SearchAlg.splitPinyinAndGetScoreArray(r._5)))
      .flatMap(s => s._6.map(k => (s._1, s._2, s._3, s._4, s._5, k._1.substring(0, math.min(12, k._1.length)))))
      .toDF("sid", "title", "contentType", "riskFlag", "associativeWord", "searchKey")

    associativeWordDf.dropDuplicates("sid", "associativeWord", "searchKey")
  }

  /**
    * 利用节目评分和匹配分计算最终排序得分
    *
    * @param searchWordDf DF("sid", "contentType", "riskFlag", "searchKey", "highlight", "matchScore")
    * @param videoScore
    * @param spark
    * @return
    */
  def computeScore(searchWordDf: DataFrame, videoScore: DataFrame)(implicit spark: SparkSession): DataFrame = {
    searchWordDf.as("a").join(videoScore.as("b"), expr("a.sid = b.sid"), "leftouter")
      .selectExpr("a.sid", "contentType", "riskFlag", "searchKey", "highlight",
        "case when video_score is null then 0.4 * a.matchScore else 0.4 * a.matchScore + 0.6 * b.video_score end as score")
  }

  /**
    * 对searchKey长度小于等于2的，以videoScore为主
    *
    * @param searchWordDf
    * @param videoScore
    * @param spark
    * @return
    */
  def computeScore2(searchWordDf: DataFrame, videoScore: DataFrame, popularityDf:DataFrame)(implicit spark: SparkSession): DataFrame = {
    searchWordDf.as("a").join(videoScore.as("b"), expr("a.sid = b.sid"), "leftouter")
      .join(popularityDf.as("c"), expr("a.sid = c.sid"), "leftouter")
      .selectExpr("a.sid", "contentType", "riskFlag", "searchKey", "highlight",
        "case when c.num is not null and b.video_score is not null then 0.4 * a.matchScore + 0.6 * b.video_score + 0.001 * num " +
          "when c.num is null and b.video_score is not null then 0.4 * a.matchScore + 0.6 * b.video_score " +
          "when c.num is not null and b.video_score is null then 0.4 * a.matchScore + 0.001 * num " +
          "else 0.4 * a.matchScore end as score")
  }

  /**
    * 利用评分进行排序，保证各危险等级的数量
    *
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
    *
    * @param input
    * @return
    */
  private def reorderContent(input: Seq[String]*): Seq[String] = {
    val allInput = input.fold(Seq[String]())((a, i) => a ++ i)
      .map(s => s.split("_"))
      .filter(s => s.length == 4)

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
      if (!sidSet.contains(sid)) {
        sidSet += sid
        s.mkString("_")
      } else { //用于过滤重复sid
        "%_%_%_%"
      }
    }).filter(s => !s.equals("%_%_%_%"))
  }

  /**
    * 把指定的搜索词转换成拼音，并计算得分
    * 拼音包括首字母和全拼前缀
    *
    * @param searchWord
    * @return 格式 （拼音，高亮，得分）
    */
  private def splitPinyinAndGetScore(searchWord: String): Array[(String, String, Double)] = {
    if (searchWord == null || searchWord.trim.isEmpty) {
      return Array()
    }
    val pinyin = PinyinHelper.convertToPinyinString(searchWord, " ", PinyinFormat.WITHOUT_TONE).toLowerCase
    val pinyins = pinyin.split(" ")
    val shortPinyin = PinyinHelper.getShortPinyin(searchWord).mkString(" ").toLowerCase
    val shortPinyins = shortPinyin.split(" ")

    (getPinyinScore(pinyins, searchWord) ++ getPinyinScore(shortPinyins, searchWord)).distinct.toArray
  }

  /**
    * 把指定的搜索词转换成拼音(改用支持多音字的方法)，并计算得分
    * 拼音包括首字母和全拼前缀
    *
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
    *
    * @param pinyins    一个搜索词对应的拼音，数组的每个元素对应一个字的拼音
    * @param searchWord 搜索词
    * @return List['searchKey', 'highLight', '拼音完整度分数']
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

  def getAssociativeWordArray(title: String, maxLength: Int):Array[String] = {
    val title1 = title.substring(0, math.min(maxLength, title.size))
    val title2 = URLEncoder.encode(title1, "utf-8")
    val associativeWord = getAssociativeWord(title2)
    if (associativeWord == null || associativeWord.isEmpty) {
      println(s"title = $title2")
      require(associativeWord == null || associativeWord.isEmpty, "联想词结果为空")
      new Array[String](0)
    } else associativeWord.toArray
  }

  def getAssociativeWord(title: String): List[String] = {
      val url = "http://nlp.moretv.cn/searchword?text=" + title
      val list: ListBuffer[String] = new ListBuffer[String]()
      try {
        val json = new JSONObject(scala.io.Source.fromURL(url).mkString)
        val jsonArray = json.getJSONArray("entities")
        (0 until jsonArray.length).foreach(index => list.+=(jsonArray.getJSONObject(index).getString("entity")))
        list.toList
      } catch {
        case _: Exception => list.toList
      }
    }

  def getWordSeg(title: String): List[String] = {
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
    def getKeyword(title: String): List[String] = {
      val encodedTitle = URLEncoder.encode(title, "utf-8")
      val url = "http://nlp.moretv.cn/keyword?text=" + encodedTitle
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