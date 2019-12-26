package cn.mapping

import java.util.Properties

import cn.whaley.sdk.utils.TransformUDF
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guohao on 2018/8/29.
  * 现网节目库sid与
  * 测试网节目库sid映射
  *
  */
object SidMappingABHelios {
  def main(args: Array[String]): Unit = {

    implicit val ss = SparkSession.builder().getOrCreate()

    TransformUDF.registerUDFSS

    val sidMappingUrl = getUrl("bigdata-appsvr-130-7","mapping")
    val sidMappingProp = getProperty(sidMappingUrl,"bigdata","bigdata@whaley666")
    val sql = "TRUNCATE TABLE sid_mapping_a_b_helios"
    val db = MySqlOps(sidMappingProp)
    //清空记录
    db.truncate(sql)
    db.destory()
    //现网数据库
    val proUrl = getUrl("10.255.130.5","tvservice")
    val proProp = getProperty(proUrl,"bislave","slave4bi@whaley")

    getDateFrame(ss,proProp,proUrl,"pro").dropDuplicates(Array("sidA")).createOrReplaceTempView("A")

    //测试网数据库
    val testUrl = getUrl("10.10.33.107","helios_tvservice")
    val testProp = getProperty(testUrl,"temp","temp@whaley654321")

    val testDF = getDateFrame(ss,testProp,testUrl,"test").persist(StorageLevel.MEMORY_AND_DISK)

    //以
    testDF.dropDuplicates(Array("sidB","titleB","contentTypeB")).createOrReplaceTempView("B")
    val resultDF1 = ss.sql("select A.sidA,A.idA,A.contentTypeA,B.sidB,B.idB from  A left join  B on  A.sidA = B.sidB and  A.titleA = B.titleB and A.contentTypeA=B.contentTypeB where B.sidB is not null ")
   ss.sql("select A.* from  A left join  B on  A.sidA = B.sidB and  A.titleA = B.titleB and A.contentTypeA=B.contentTypeB where B.sidB is  null ").createOrReplaceTempView("A")



    testDF.dropDuplicates(Array("titleB","contentTypeB")).createOrReplaceTempView("B")
    val resultDF2 = ss.sql("select A.sidA,A.idA,A.contentTypeA,B.sidB,B.idB from  A left join  B on   A.titleA = B.titleB and A.contentTypeA=B.contentTypeB where B.sidB is not null ")
    ss.sql("select A.* from  A left join  B on    A.titleA = B.titleB and A.contentTypeA=B.contentTypeB where B.sidB is  null ").createOrReplaceTempView("A")


    testDF.dropDuplicates(Array("contentTypeB")).createOrReplaceTempView("B")
    val resultDF3 = ss.sql("select A.sidA,A.idA,A.contentTypeA,B.sidB,B.idB from  A left join  B on   A.contentTypeA=B.contentTypeB where B.sidB is not null ")

    resultDF1.union(resultDF2).union(resultDF3).foreachPartition(par=>{
      val insertSql = "insert into sid_mapping_a_b_helios(sidA,idA,sidB,idB,contentType) values(?,?,?,?,?) "
      val db = MySqlOps(sidMappingProp)
      par.foreach(row=>{
        val sidA = row.getAs[String]("sidA")
        val idA = row.getAs[Integer]("idA")
        val sidB = row.getAs[String]("sidB")
        val idB = row.getAs[Integer]("idB")
        val contentType = row.getAs[String]("contentTypeA")
        db.insert(insertSql,sidA,idA,sidB,idB,contentType)
      })
      db.destory()
    })


  }

  def getProperty(url:String,user:String,password:String): Properties ={
    val props = new Properties()
    props.setProperty("driver","com.mysql.jdbc.Driver")
    props.setProperty("url",url)
    props.setProperty("user",user)
    props.setProperty("password",password)
    props
  }

  def getUrl(host:String,database:String): String ={
    s"jdbc:mysql://${host}:3306/${database}?useUnicode=true&characterEncoding=utf-8&autoReconnect=true"
  }

  def getDateFrame(ss:SparkSession,props:Properties,url:String,env:String): DataFrame ={
    //最大最小id
    val sqlMinMaxId = s"select min(id),max(id) from mtv_program"
    val sqlInfo = s"select id from mtv_program where id >= ? and id <= ? "
    val data = MySqlOps.getJdbcRDD(ss.sparkContext,props,sqlMinMaxId,sqlInfo,100,rs => rs.getLong(1)).sortBy(x=>x).collect()
    val predicates = getPredicates(data,"id")
    if(env.equals("test")){
      ss.read.jdbc(url,"mtv_program",predicates,props)
        .selectExpr("sid as sidB ","  transformSid(sid) as idB ","trim(title) as titleB ","trim(contentType) as contentTypeB")
    }else{
      ss.read.jdbc(url,"mtv_program",predicates,props)
        .selectExpr("sid as sidA "," transformSid(sid) as idA ","trim(title) as titleA ","trim(contentType) as contentTypeA")
    }

  }




  /**
    * 获取条件分区
    * @param data 使用数组而不是链表结果。当data数据量非常大的时候，链表耗时非常大
    * @param parField
    * @return
    */
  private def getPredicates(data:Array[Long],parField:String): Array[String] ={
    val totalSize = data.length
    val partitionNum = getPartitionNum(totalSize)
    val batchSize = totalSize / partitionNum
    //按照batchSize分多个id区间，每个区间的记录数相同
    val arrayBuffer = new ArrayBuffer[String]
    if(totalSize> batchSize && batchSize!=0){
      val times = partitionNum
      var start = 0
      for(i<- 1 to times ){
        val startValue = data(start)
        val endValue = data(batchSize * i-1)
        arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and videoType !=2 and length(sid)=12 and contentType is not null and  contentType!='' "
        start = batchSize *i
      }
      //余数
      val remain = totalSize - times * batchSize
      if(remain >0){
        val startValue = data(start)
        val endValue = data(totalSize -1)
        arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and videoType !=2  and length(sid)=12  and contentType is not null and  contentType!=''"
      }
    }else{
      val startValue = data(0)
      val endValue = data(totalSize -1)
      arrayBuffer+=s"${parField} >=${startValue} and ${parField} <=${endValue} and videoType !=2  and length(sid)=12 and contentType is not null and  contentType!=''"
    }
    arrayBuffer.toArray
  }

  /**
    * 获取partition数 大小
    * @param totalSize
    * @return
    */
  private def getPartitionNum(totalSize:Long): Int ={
    val partitionNum = totalSize match {
      case i if i <= 10*10000 => 10
      case i if (i >= 10*10000 && i <= 50*10000)  => 20
      case i if (i >= 50*10000 && i <= 100*10000)  => 50
      case i if (i >= 100*10000 && i <= 500*10000)  => 100
      case i if (i >= 500*10000 && i <= 1000*10000)  => 200
      case i if (i >= 1000*10000 && i <= 10000*10000)  => 500
      case _=> 1000
    }
    partitionNum
  }

}
