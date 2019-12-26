package cn.moretv.doraemon.biz.util

import cn.moretv.doraemon.common.enum.EnvEnum
import org.elasticsearch.client.{RestClient, RestHighLevelClient}
import org.apache.http.HttpHost
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._



/**
  * Created by lituo on 2018/11/22.
  */
object ElasticSearchUtil2 {

  var client: RestHighLevelClient = _

  def init(implicit env: EnvEnum.Value) = {
    if(client == null) {
      client = new RestHighLevelClient(
        RestClient.builder(
          new HttpHost(ConfigUtil.get("search.es.host.1"), 9200, "http")))
    }
  }

  def get(indexName: String, indexType: String, docId: String)(implicit env: EnvEnum.Value): Map[String, Any] = {
    init
    val getRequest = new GetRequest(indexName, indexType, docId)
    val getResponse = client.get(getRequest)

    if (getResponse.isExists) {
      getResponse.getSourceAsMap.toMap
    }
    else {
      null
    }
  }

  def update(indexName: String, indexType: String, docId: String, content: Map[String, Any])(implicit env: EnvEnum.Value):Boolean  = {
    init
    val updateRequest = new UpdateRequest(indexName, indexType, docId)
    updateRequest.doc(content.asJava)
    val updateResponse = client.update(updateRequest)
    if(updateResponse.status().getStatus >= 400) {
      false
    } else {
      true
    }
  }

  def index(indexName: String, indexType: String, docId: String, content: Map[String, Any])(implicit env: EnvEnum.Value):Boolean  = {
    init
    val indexRequest = new IndexRequest(indexName, indexType, docId)
    indexRequest.source(content.asJava)
    val indexResponse = client.index(indexRequest)
    if(indexResponse.status().getStatus >= 400) {
      false
    } else {
      true
    }
  }


}
