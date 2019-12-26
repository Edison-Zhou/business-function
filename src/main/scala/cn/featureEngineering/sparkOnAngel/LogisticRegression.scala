package cn.featureEngineering.sparkOnAngel

import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.path.HdfsPath
import cn.moretv.doraemon.data.writer.DataWriter2Hdfs
import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.spark.ml.classification.LogisticRegression
import com.tencent.angel.spark.ml.core.{GraphModel, OfflineLearner}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession

/**
  * Created by cheng_huan on 2019/7/15.
  */
object LogisticRegression {

  def start(ss:SparkSession): Unit = {
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    start(ss)

    val inputPath = "/ai/tmp/output/test/medusa/featureEngineering/cluster/testConcatNormalizedSqrt0.5Feature/Latest"
    val input = DataReader.read(new HdfsPath(inputPath)).select("features", "label").rdd
      //.repartition(SparkUtils.getNumCores(ss.sparkContext.getConf))
      .map(r => {
        val label = r.getAs[Double]("label")
        val features = r.getAs[SparseVector]("features")
        val dim = features.size
        val keys = features.indices
        val vals = features.values
        val vector = VFactory.sparseDoubleVector(dim, keys, vals)
      (new LabeledData(vector, label), label.toString)
      })

    SharedConf.get()
    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_DOUBLE_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, 3072)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.5)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 5)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.1)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.0)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.2)

    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

    val model = new LogisticRegression
    val learner = new OfflineLearner

    model.init(input.getNumPartitions)

    /*learner.train(input, model)
    model.save("/ai/tmp/PSLogisticRegModel")*/

    model.load("/ai/tmp/PSLogisticRegModel")

    new DataWriter2Hdfs().write(learner.predict(input, model).toDF("prediction", "label"),new HdfsPath("/ai/tmp/PSLogisticRegPredict"))

    stop()
  }


}
