package cn.moretv.doraemon.biz.util

import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer

/**
  *
  * @author wang.baozhi 
  * @since 2018/9/30 上午11:32
  *        此类用于获取url接口的数据
  */
object UrlUtils {
  /**
    * 根据节目url获取节目列表
    *
    * @param url 接口url（最后的pageIndex为空）
    * @return (节目列表， 节目信息列表)
    */
  def getDataFromURL(url: String) = {
    val num = 1
    // 获取url的json列表
    println(url + num.toString)
    val json = HttpUtils.getForTreeSiteReOrder(url + num.toString)
    val jsonObj = new JSONObject(json)

    // 获取页面数
    val pageCount = jsonObj.getJSONArray("data").getJSONObject(0).get("pageCount").toString.toInt

    val videoList = new ArrayBuffer[String]()
    val videoJsonArray = new JSONArray()

    for (num <- 1 to pageCount) {
      val finalUrl = url + num.toString
      val data = HttpUtils.getForTreeSiteReOrder(finalUrl)
      val jsonData = new JSONObject(data)
      val videoListFromPage = jsonData.getJSONArray("data").getJSONObject(0).getJSONArray("items")
      val length = videoListFromPage.length()
      var i = 0
      for (j <- 0 until length) {
        val video = videoListFromPage.getJSONObject(i)
        try {
          videoList += video.getString("sid")
        } catch {
          case e: Exception => {
            println(finalUrl)
            println(e.toString)
            println(i)
            println(video)
          }
        }
        videoJsonArray.put(video)
        i = i + 1
      }
    }
    (videoList.toArray, videoJsonArray)
  }


  def main(args: Array[String]): Unit = {
    // val videoList=getDataFromURL("http://vod.tvmore.com.cn/Service/V3/container?contentType=movie&code=1_movie_tag_dongzuo&pageSize=10&desc=0000000000&pageIndex=")._1

    if (1 == 2) {
      val code = "movie_sohuyueting"
      val videoType = "movie"
      val desc = "0"
      val codeType = 3
      val url = codeType match {
        case 4 => s""
        case _ => s"http://vod.tvmore.com.cn/v/queryPrograms/detail?contentType=${videoType}&code=${code}&type=${codeType}&pageSize=20&desc=526${desc}884${desc}25&appVersion=4.0.2&pageIndex="
      }
      println(url)
      val videoList = getDataFromURL(url)._1
      println("videoList.length：" + videoList.length)
      for (e <- videoList) {
        println(e)
      }
    }

    if (1 == 1) {

      val code = "1_tv_area_neidi"
      val videoType = "tv"
      val desc = "0"
      val codeType = 4
      val url = codeType match {
        case 4 => s"http://vod.tvmore.com.cn/Service/V3/container?contentType=${videoType}&code=${code}&pageSize=10&desc=000${desc}000${desc}00&pageIndex="
        case _ => s"http://vod.aiseewhaley.aisee.tv/Service/V3/queryPrograms?contentType=${videoType}&code=${code}&pageSize=10&type=${codeType}&desc=526${desc}884${desc}25&pageIndex="
      }
      println(url)
      val videoList = getDataFromURL(url)._1
      println("videoList.length：" + videoList.length)
      for (e <- videoList) {
        println(e)
      }
    }
  }

}
