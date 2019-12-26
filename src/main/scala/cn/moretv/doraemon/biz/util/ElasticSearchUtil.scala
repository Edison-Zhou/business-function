package cn.moretv.doraemon.biz.util

import java.io.IOException

import cn.moretv.doraemon.common.enum.EnvEnum
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, StatusLine}

/**
  * Created by lituo
  */
object ElasticSearchUtil {

  private val httpClient = HttpClients.createDefault
//  private val ES_DOMAIN = "10.255.129.202:9200"
  def ES_DOMAIN(implicit env: EnvEnum.Value) = {
    ConfigUtil.get("search.es.host.1") + ":" + "9200"
  }

  def bulkCreateIndex(content: String)(implicit env: EnvEnum.Value): Boolean = {
    val httpPost = new HttpPost("http://" + ES_DOMAIN + "/_bulk")
    val stringEntity = new StringEntity(content, ContentType.create("application/json").withCharset("utf-8"))
    httpPost.setEntity(stringEntity)
    try {
      val response: CloseableHttpResponse = httpClient.execute(httpPost)
      val statusLine: StatusLine = response.getStatusLine
      val entity: HttpEntity = response.getEntity
      val str = EntityUtils.toString(entity)
      if (statusLine.getStatusCode >= 300) {
        println("HTTP FAILED!!!")
        println("StatusCode: " + statusLine.getStatusCode + "\n"
          + "Response: \n" + str)
        false
      } else {
        println("RESPONSE SUCCESS")
        println("StatusCode: " + statusLine.getStatusCode + "\n Response: \n" + str)
        true
      }

    }
    catch {
      case e: IOException => {
        println("HTTP EXCEPTION")
        false
      }
    }


  }

  //关闭cient
  def close = {
    if (httpClient != null)
      httpClient.close()
  }

  def bulkDeleteDocs(index: String, typeName: String, queryKey: String, queryVal: String)(implicit env: EnvEnum.Value) = {

    val url = s"http://${ES_DOMAIN}/$index/$typeName/_query?q=$queryKey:${queryVal}"

    HttpUtils.delete(url)

  }

  /**
    * 创建新index
    *
    * @param indexName
    * @param indexJson
    */
  def newIndex(indexName: String, indexJson: String)(implicit env: EnvEnum.Value): Unit = {
    val url = s"http://${ES_DOMAIN}/$indexName"
    HttpUtils.put(url, indexJson)
  }

  /**
    * 删除索引
    *
    * @param indexName
    */
  def deleteIndex(indexName: String)(implicit env: EnvEnum.Value): Unit = {
    val url = s"http://${ES_DOMAIN}/$indexName"
    HttpUtils.delete(url)

  }

  def indexExist(indexName: String)(implicit env: EnvEnum.Value): Boolean = {
    val url = s"http://${ES_DOMAIN}/$indexName"
    val result = HttpUtils.head(url)
    result == 200
  }

  /**
    * 先remove后add
    *
    * @param aliases
    * @param removeIndex
    * @param addIndex
    */
  def changeAliases(aliases: String, removeIndex: String, addIndex: String)(implicit env: EnvEnum.Value): Unit = {
    if (addIndex == null && removeIndex == null) {
      return
    }
    val url = s"http://${ES_DOMAIN}/_aliases"

    val removeContent = if (removeIndex != null) {
      "{ \"remove\" : { \"index\" : \"" + removeIndex + "\", \"alias\" : \"" + aliases + "\" } }"
    } else ""

    val addContent = if (addIndex != null)
      "{ \"add\" : { \"index\" : \"" + addIndex + "\", \"alias\" : \"" + aliases + "\" } }"
    else ""

    val content = "{\"actions\" : [" + removeContent +
      (if (removeIndex != null && addIndex != null) "," else "") + addContent + "]}"

    HttpUtils.post(url, content)._1 == 200

  }


  def aliasesExist(aliases: String, index: String)(implicit env: EnvEnum.Value): Boolean = {
    val url = s"http://${ES_DOMAIN}/$index/_alias/$aliases"
    val result = HttpUtils.head(url)
    result == 200
  }

  def beforeBulkIndex(indexName: String)(implicit env: EnvEnum.Value): Unit = {
    val url = s"http://${ES_DOMAIN}/$indexName/_settings"
    val content = "{\"index\" : {\"refresh_interval\" : \"120s\"}}"
    HttpUtils.put(url, content)._1 == 200
  }

  def afterBulkIndex(indexName: String)(implicit env: EnvEnum.Value): Unit = {
    val url = s"http://${ES_DOMAIN}/$indexName/_settings"
    val content = "{\"index\" : {\"refresh_interval\" : \"1s\"}}"
    HttpUtils.put(url, content)._1 == 200

    val url2 = s"http://${ES_DOMAIN}/$indexName/_forcemerge?max_num_segments=5"
    HttpUtils.post(url2, "")._1 == 200
  }

}
