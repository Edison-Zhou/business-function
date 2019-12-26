package cn.featureEngineering.sparkOnAngel

import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.tree.gbdt.trainer.GBDTTrainer
import com.tencent.angel.spark.ml.tree.param.GBDTParam
import com.tencent.angel.spark.ml.tree.util.Maths
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cheng_huan on 2019/7/15.
  */
object GBDTRegression {
  def main(args: Array[String]): Unit = {

    @transient val conf = new SparkConf().setMaster("local").setAppName("gbdt")

    val param = new GBDTParam

    // spark conf
    val numExecutor = 1
    val numCores = 1
    param.numWorker = numExecutor
    param.numThread = numCores
    conf.set("spark.task.cpus", numCores.toString)
    conf.set("spark.locality.wait", "0")
    conf.set("spark.memory.fraction", "0.7")
    conf.set("spark.memory.storageFraction", "0.8")
    conf.set("spark.task.maxFailures", "1")
    conf.set("spark.yarn.maxAppAttempts", "1")
    conf.set("spark.network.timeout", "1000")
    conf.set("spark.executor.heartbeatInterval", "500")

    val params = ArgsUtil.parse(args)

    //val trainPath = "data/dna/dna.scale"  //dimension=181
    //val validPath = "data/dna/dna.scale.t"
    val trainPath = "data/abalone/abalone_8d_train.libsvm"  //dimension=8
    val validPath = "data/abalone/abalone_8d_train.libsvm"
    val modelPath = "tmp/gbdt/abalone"

    // dataset conf
    param.taskType = "regression"
    param.numClass = 2
    param.numFeature = 8

    // loss and metric
    param.lossFunc = "rmse"
    param.evalMetrics = Array("rmse")
    param.multiGradCache = false

    // major algo conf
    param.featSampleRatio = 1.0f
    param.learningRate = 0.1f
    param.numSplit = 10
    param.numTree = 10
    param.maxDepth = 7
    val maxNodeNum = Maths.pow(2, param.maxDepth + 1) - 1
    param.maxNodeNum = 4096 min maxNodeNum

    // less important algo conf
    param.histSubtraction = true
    param.lighterChildFirst = true
    param.fullHessian = false
    param.minChildWeight = 0.0f
    param.minNodeInstance = 10
    param.minSplitGain = 0.0f
    param.regAlpha = 0.0f
    param.regLambda = 1.0f
    param.maxLeafWeight = 0.0f

    println(s"Hyper-parameters:\n$param")

    @transient implicit val sc = new SparkContext(conf)

    try {
      val trainer = new GBDTTrainer(param)
      trainer.initialize(trainPath, validPath)
      val model = trainer.train()
      trainer.save(model, modelPath)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
    }
  }
}
