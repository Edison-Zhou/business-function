package cn.moretv.doraemon.biz.util

import java.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.{Consts, HttpStatus}

/**
  * Created by Administrator on 2016/7/19.
  */
object SendMailUtils {

  /**
    * 通过http://mailserver.whaleybigdata.com/方式，会自动发送给全组
    * 发送post请求
    * @param subject 文件标题
    * @param emailsName 邮件地址
    * @return
    */
  def sendMailWithAllDefault(content:String,subject:String,emailsName:Array[String])={
    val url = "http://mailserver.whaleybigdata.com/"
    val httpClient = HttpClients.createDefault()
    try{
      val httpPost = new HttpPost(url)
      //封装收件人 主题 内容
      val parameters = new JSONObject()
      val emails = new JSONArray()
      for(email<- emailsName) emails.add(email)
      parameters.put("to",emails)
      parameters.put("body",content)
      parameters.put("subject",subject)
      //设置参数
      val se = new StringEntity(parameters.toString,"UTF-8")
      se.setContentType("application/json")
      se.setContentEncoding("UTF-8")
      httpPost.setEntity(se)

      val requestConfig = RequestConfig.custom()
        .setSocketTimeout(3000)
        .setConnectTimeout(3000)
        .setConnectionRequestTimeout(3000)
        .build()
      httpPost.setConfig(requestConfig)

      val res = httpClient.execute(httpPost)
      // println(EntityUtils.toString(res.getEntity,"utf-8"))
      //如果没发送成功 尝试再次发送
      if(res.getStatusLine.getStatusCode != HttpStatus.SC_OK){
        var flag = 0
        while (flag < 3){
          val response = httpClient.execute(httpPost)
          if(response.getStatusLine.getStatusCode == HttpStatus.SC_OK)
            flag = 3
          else {
            flag = flag + 1
          }
          response.close()
        }
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      httpClient.close()
    }
  }

  /**
    *   <!-- https://mvnrepository.com/artifact/org.scalaj/scalaj-http -->
        <dependency>
            <groupId>org.scalaj</groupId>
            <artifactId>scalaj-http_2.11</artifactId>
            <version>2.4.1</version>
        </dependency>
    */
  def send(subject: String,content:String,users:Array[String]): Unit ={
    var data:Map[String,String] = Map()
    data += ("sub"->subject)
    data += ("content"->content)
    data += ("sendto"->users.mkString(","))
    //Http("http://mailservice.whaleybigdata.com/mail/api/v2.0/send").postForm(data.toSeq).asString
  }


  /**
    * 邮件发送给个人
    * 发送post请求
    * @param subject 文件标题
    * @param content 文件内容
    * @param emailNames 邮件地址
    * @return
    */
  def sendMailToPerson(subject:String,content:String,emailNames:Array[String]): Unit ={
    val httpClient:CloseableHttpClient = HttpClients.createDefault()
    try{
    val form = new util.ArrayList[BasicNameValuePair]
    form.add(new BasicNameValuePair("sub", subject))
    form.add(new BasicNameValuePair("content",content))
    form.add(new BasicNameValuePair("sendto", emailNames.mkString(",")))
    val entity = new UrlEncodedFormEntity(form, Consts.UTF_8)
    val  httpPost:HttpPost = new HttpPost("http://mailservice.whaleybigdata.com/mail/api/v2.0/send")
    httpPost.setEntity(entity)
    println("Executing request " + httpPost.getRequestLine())
    val res=httpClient.execute(httpPost)
    println(res)
    println(res.getStatusLine.getStatusCode)
    if(res.getStatusLine.getStatusCode != HttpStatus.SC_OK){
      var flag = 0
      while (flag < 3){
        val response = httpClient.execute(httpPost)
        if(response.getStatusLine.getStatusCode == HttpStatus.SC_OK)
          flag = 3
        else {
          flag = flag + 1
        }
        response.close()
      }
    }else{
      res.close()
    }
  }catch {
    case e:Exception => e.printStackTrace()
  }finally {
      httpClient.close()
  }
  }

  def main(args: Array[String]): Unit = {
    val to=Array[String]("app-bigdata_group@moretv.com.cn")
    val subject="test123"
    val content="qqqq"
    SendMailUtils.sendMailToPerson(subject,content,to)
  }
}