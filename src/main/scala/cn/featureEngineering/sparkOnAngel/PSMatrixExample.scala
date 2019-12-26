package cn.featureEngineering.sparkOnAngel

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by cheng_huan on 2019/7/15.
  */
object PSMatrixExample {
  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("PSVector Examples")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {

    start()

    // randomly initialize a dense PSMatrix (5x10)
    val dense = PSMatrix.rand(5, 10)

    // pull the first row
    println(dense.pull(0).sum())
    // pull the first with indices (0, 1)
    println(dense.pull(0, Array(0, 1)).sum())

    // update the first row with indices(0, 1)
    val update = VFactory.sparseDoubleVector(10, Array(0, 1), Array(1.0, 1.0))
    dense.update(0, update)

    println(dense.pull(0, Array(0, 1)).sum())

    // reset the first row
    dense.reset(0)
    println(dense.pull(0).sum())

    stop()
  }
}
