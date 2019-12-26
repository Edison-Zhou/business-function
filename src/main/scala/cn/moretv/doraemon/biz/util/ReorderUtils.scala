package cn.moretv.doraemon.biz.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  *
  * @author wang.baozhi 
  * @since 2018/9/26 下午7:14 
  */
object ReorderUtils {

  def sample(a: Array[(String,Double)], s: Double):Int={
    val normed = a.map(_._2).sum
    val array = a.map(x=>x._2/normed)
    var sum = 0.0
    var topic = 0
    var find = true
    while(find&&topic<array.length){
      sum+=array(topic)
      if(s<sum){
        find=false
      }else{
        topic+=1
      }
    }
    math.min(topic,(array.length-1))
  }


  def multiSample(a: Array[(String,Double)],num: Int):Array[(String,Double)]={
    val aMap = a.toMap
    var array = a.toBuffer
    val result = (0 until num).map(i=>{
      val tmp = array.toArray
      val index = sample(tmp,math.random.toFloat)
      val value = tmp(index)
      array.remove(index)
      value
    })
    result.toArray
  }

  //随机重排
  def randomReorder(recommendsData: RDD[(Long,Array[(String, Double)])],RecommendsNum: Int):RDD[(Long,Array[(String,Double)])]={
    recommendsData.map(x=>(x._1,multiSample(x._2,math.min(x._2.length, RecommendsNum))))
  }

  /**
    * 根据对各用户精选推荐的信息对原推荐列表重排序
    *
    * @param recommendsData 用户初始化推荐列表
    * @param userInfo 用户精选推荐信息
    * @param weight 权重因子
    */
  def reorderByUserInfo(recommendsData: RDD[(Long,Array[(Int,Float)])], userInfo: RDD[(Long, Set[Int])],
                        weight: Float): RDD[(Long,Array[(Int,Float)])] = {

    recommendsData.leftOuterJoin(userInfo).map(r => {
      val uid = r._1
      val videoScoreArrayByALS = r._2._1
      r._2._2 match {
        case None => (uid, videoScoreArrayByALS)
        case Some(p) => (uid , videoScoreArrayByALS.map(x => (x._1, {
          if (p.contains(x._1)) x._2* weight
          else x._2
        })))
      }
    })
  }

  /**
    * 根据精选节目信息对原推荐列表重排序
    *
    * @param recommendsData 用户初始化推荐列表
    * @param videoInfo 精选节目
    * @param weight 权重因子
    */
  def reorderByVideoInfo(recommendsData: RDD[(Long,Array[(Int,Float)])], videoInfo: Broadcast[Set[Int]],
                         weight: Float): RDD[(Long,Array[(Int,Float)])] = {
    recommendsData.map(r => {
      val uid = r._1
      val videScoreArray = r._2.map(x => (x._1, {
        if(videoInfo.value.contains(x._1))
          x._2 * weight
        else
          x._2
      }))
      (uid, videScoreArray)
    })
  }
}
