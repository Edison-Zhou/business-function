package cn.moretv.doraemon.biz.peoplealsolike

import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.MysqlPath
import org.apache.spark.sql.DataFrame

/**
  * Created by guohao on 2018/10/8.
  */
object ReaderUtil {
  private val host:String = "10.255.130.4"
  private val port:Integer = 3306
  private val database:String = "tvservice"
  private val tableName:String = "mtv_program"
  private val user:String = "bislave"
  private val password:String = "slave4bi@whaley"
  private val parField:String = "id"
  private val selectFiled:Array[String] = Array("sid","risk_flag","status","type","contentType")
  private val filterContition:String = "contenttype='movie' and status = 1 and type = 1 "

  def getMysqlDF(): DataFrame ={
    val mysqlPath = new MysqlPath(host,port,database,tableName,user,password,selectFiled,filterContition)
    DataReader.read(mysqlPath)
  }

}
