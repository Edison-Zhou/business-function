package cn.moretv.doraemon.biz.util

import java.util.Properties

import cn.moretv.doraemon.common.enum.EnvEnum

import scala.collection.mutable

/**
  * Created by lituo on 2018/9/28.
  */
object ConfigUtil {

  val propMap = new mutable.HashMap[EnvEnum.Value, Properties]()

  private def init: Unit = {
    if(propMap.isEmpty) {
      EnvEnum.values.foreach(v => {
        val envName = v.toString
        val fileName = "env_" + envName.toLowerCase + ".properties"
        val inputStream = ConfigUtil.getClass.getClassLoader.getResourceAsStream(s"conf/$fileName")
        if(inputStream == null) {
          throw new RuntimeException("配置文件：" + fileName + "不存在")
        }
        val prop = new Properties()
        prop.load(inputStream)
        propMap.put(v, prop)
      })
    }
  }

  def get(key: String)(implicit env: EnvEnum.Value): String = {
    init
    val result = propMap(env).getProperty(key)
    if(result == null || result.trim.isEmpty) {
      throw new IllegalArgumentException("未发现指定的配置：" + key)
    }
    result
  }

  def getInt(key: String)(implicit env: EnvEnum.Value): Int = {
    val result = get(key)
    result.toInt
  }

  def getLong(key: String)(implicit env: EnvEnum.Value): Long = {
    val result = get(key)
    result.toLong
  }

  def getBoolean(key: String)(implicit env: EnvEnum.Value): Boolean = {
    val result = get(key)
    result.toBoolean
  }
}
