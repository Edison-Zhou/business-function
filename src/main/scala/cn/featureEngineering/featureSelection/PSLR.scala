package cn.featureEngineering.featureSelection

import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.HdfsPath
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{Row, SparkSession}
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.ml.math2.vector.IntDoubleVector
import com.tencent.angel.spark.util.VectorUtils
/**
  * Created by cheng_huan on 2019/7/10.
  */
object PSLR {
  def main(args: Array[String]): Unit = {

    val ITERATIONS = 5
    val input = "/ai/tmp/output/test/medusa/featureEngineering/features/projectionFeaturesSparseD10/Latest"
    val mode = "yarn-cluster"
    val lr = 0.1

    val ss = SparkSession.builder()
      .master(mode)
      .appName("LRExample").getOrCreate()
    val sc = ss.sparkContext
    val psc = PSContext.getOrCreate(sc)

    val data = DataReader.read(new HdfsPath(input)).select("label", "features")
    val firstRow = data.select("features").take(1)
    val numFeatures = firstRow(0).getAs[MLVector](0).size
    val trainData = data.rdd.map { case Row(label: Double, v: MLVector) =>
      (VFactory.sparseDoubleVector(numFeatures, v.toSparse.indices, v.toSparse.values), label)
    }.cache()
    val psW = PSVector.dense(numFeatures) // weights
    val psG = PSVector.duplicate(psW) // gradients of weights

    println("Initial psW: " + psW.dimension)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val localW = psW.pull()
      println(s"localW: ${localW.getSize}")
      trainData.map { case (x, label) =>
        val g = x.mul(-label * (1 - 1.0 / (1.0 + math.exp(-label * localW.dot(x)))))
        psG.increment(g)
      }.count()
      VectorUtils.axpy(-lr / numFeatures, psG, psW)
      psG.reset
    }
    println(s"Final psW: ${psW.pull().asInstanceOf[IntDoubleVector].getStorage.getValues.mkString(" ")}")
    PSContext.stop()
    sc.stop()
  }
}
