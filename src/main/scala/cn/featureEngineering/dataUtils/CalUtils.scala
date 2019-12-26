package cn.featureEngineering.dataUtils

import org.apache.spark.ml.linalg.{SparseVector, Vectors}

import scala.collection.mutable.ArrayBuffer

/**
  * 特征工程预研使用，实际成熟后迁移至doraemon算法包
  * Created by cheng_huan on 2019/6/4.
  */
object CalUtils {
  /**
    * 计算稀疏向量的逐点乘积
    * @param V1
    * @param V2
    * @return
    */
  def vectorPointWiseProduct(V1:SparseVector, V2:SparseVector):SparseVector = {
    require(V1.size == V2.size, "The two SparseVector have different sizes!")
    val size = V1.size
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length

    while(i < length1 && j < length2) {
      if(V1.indices(i) < V2.indices(j)) {
        i = i + 1
      }
      else if(V1.indices(i) > V2.indices(j)) {
        j = j + 1
      }
      else {
        sequence += ((V1.indices(i), V1.values(i) * V2.values(j)))
        i = i + 1
        j = j + 1
      }
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 计算稀疏向量的逐点乘积
    * @param V1
    * @param V2
    * @return
    */
  def vectorPointWiseSqrtProduct(V1:SparseVector, V2:SparseVector):SparseVector = {
    require(V1.size == V2.size, "The two SparseVector have different sizes!")
    val size = V1.size
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length

    while(i < length1 && j < length2) {
      if(V1.indices(i) < V2.indices(j)) {
        i = i + 1
      }
      else if(V1.indices(i) > V2.indices(j)) {
        j = j + 1
      }
      else {
        sequence += ((V1.indices(i), math.sqrt(V1.values(i) * V2.values(j))))
        i = i + 1
        j = j + 1
      }
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 将两个稀疏向量拼接起来
    * @param V1
    * @param V2
    * @return
    */
  def vectorConcat(V1:SparseVector, V2:SparseVector):SparseVector = {
    val size1 = V1.size
    val size = V1.size + V2.size
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = V1.indices.length
    val length2 = V2.indices.length

    while(i < length1) {
      sequence += ((V1.indices(i), V1.values(i)))
      i = i + 1
    }

    while(j < length2) {
      sequence += ((V2.indices(j) + size1, V2.values(j)))
      j = j + 1
    }

    Vectors.sparse(size, sequence).toSparse
  }

  /**
    * 将两个指定size的稀疏向量拼接起来，允许V1或V2为null
    * @param V1
    * @param size1
    * @param V2
    * @param size2
    * @return
    */
  def vectorConcat(V1:SparseVector,
                   size1:Int,
                   V2:SparseVector,
                   size2:Int):SparseVector = {
    if(V1 != null) {
      require(V1.size == size1, "The size of vector1 is error")
    }
    if(V2 != null) {
      require(V2.size == size2, "The size of vector2 is error")
    }
    val sequence = new ArrayBuffer[(Int, Double)]()

    var i = 0
    var j = 0
    val length1 = if(V1 != null) V1.indices.length else 0
    val length2 = if(V2 != null) V2.indices.length else 0

    while(i < length1) {
      sequence += ((V1.indices(i), V1.values(i)))
      i = i + 1
    }

    while(j < length2) {
      sequence += ((V2.indices(j) + size1, V2.values(j)))
      j = j + 1
    }

    Vectors.sparse(size1 + size2, sequence).toSparse
  }

  /**
    * 取稀疏向量分量的最大值
    * @param V
    * @return
    */
  def vectorMaxByDim(V:SparseVector):Double = {
    V.values.sortBy(e => -e).head
  }
}
