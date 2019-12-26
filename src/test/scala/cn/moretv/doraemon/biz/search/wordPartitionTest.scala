package cn.moretv.doraemon.biz.search

import org.junit.Test

class wordPartitionTest {
  @Test
  def test() = {
    val pinyinArr = ObtainPinYinUtils.polyPinYinQuanPin("知否知否 应是绿肥红瘦").toArray
    val fullSpell = pinyinArr.head
    val fullSpell4Search = fullSpell.asInstanceOf[String]
    println(fullSpell4Search)
    val fullInitialSearchWord = fullSpell4Search.split(" ").map(e => e.substring(0,1)).reduce((x, y)=> x+y)
    println(fullInitialSearchWord)
    val searchText = if (fullInitialSearchWord.length >= 4) fullInitialSearchWord.substring(0,4) else fullInitialSearchWord
    println(searchText)
    println("分隔")
    val pinyinArr1 = ObtainPinYinUtils.polyPinYinQuanPin("如懿传").toArray
    pinyinArr1.foreach(println)
    println("分隔")
    println(pinyinArr1.takeRight(1)(0))
    println(pinyinArr1.last)
    println("分隔")
    val res = ObtainPinYinUtils.getAllPinYins("传说")
    res.foreach(println)
    println("分隔")
    val apitest = SearchAlg.splitPinyinAndGetScoreArray("杰米熊之魔瓶大冒险")
    apitest.foreach(println)
  }
}
