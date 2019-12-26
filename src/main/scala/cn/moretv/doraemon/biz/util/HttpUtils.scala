package cn.moretv.doraemon.biz.util

import org.apache.http.client.methods._
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
 * Created by lituo.
 */
object HttpUtils {

  def get(url:String): (Int, String) = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
    try{
      val statusCode = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      (statusCode, EntityUtils.toString(entity))
    }finally {
      response.close()
    }
  }

  def getForTreeSiteReOrder(url:String) = {
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
    try{
      val entity = response.getEntity
      EntityUtils.toString(entity,"UTF-8")
    }finally {
      response.close()
    }
  }

  def post(url:String, jsonContent: String): (Int, String) = {
    val httpClient = HttpClients.createDefault()
    val requestEntity = new StringEntity(jsonContent,ContentType.APPLICATION_JSON)
    val httpPost = new HttpPost(url)
    httpPost.setEntity(requestEntity)
    val response = httpClient.execute(httpPost)
    try{
      val statusCode = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      (statusCode, EntityUtils.toString(entity))
    }finally {
      response.close()
    }
  }

  def put(url:String, jsonContent: String): (Int, String) = {
    val httpClient = HttpClients.createDefault()
    val requestEntity = new StringEntity(jsonContent,ContentType.APPLICATION_JSON)
    val httpPut = new HttpPut(url)
    httpPut.setEntity(requestEntity)
    val response = httpClient.execute(httpPut)
    try{
      val statusCode = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      (statusCode, EntityUtils.toString(entity))
    }finally {
      response.close()
    }
  }

  def delete(url:String): Int = {
    val httpClient = HttpClients.createDefault()
    val httpDelete = new HttpDelete(url)
    val response = httpClient.execute(httpDelete)
    response.getStatusLine.getStatusCode
  }

  def head(url:String): Int = {
    val httpClient = HttpClients.createDefault()
    val httpHead = new HttpHead(url)
    val response = httpClient.execute(httpHead)
    response.getStatusLine.getStatusCode
  }
}
