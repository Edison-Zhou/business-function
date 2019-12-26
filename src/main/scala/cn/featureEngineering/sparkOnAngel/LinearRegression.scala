package cn.featureEngineering.sparkOnAngel

import cn.moretv.doraemon.biz.BaseClass
import cn.moretv.doraemon.biz.util.BizUtils
import cn.moretv.doraemon.common.data.DataReader
import cn.moretv.doraemon.common.enum.ProductLineEnum
import cn.moretv.doraemon.common.path.HdfsPath
import com.tencent.angel.RunningMode
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.OfflineLearner
import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by cheng_huan on 2019/8/1.
  */
object LinearRegression extends BaseClass{
  def start(ss: SparkSession): Unit = {
    val sc = ss.sparkContext
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  override implicit val productLine: _root_.cn.moretv.doraemon.common.enum.ProductLineEnum.Value = ProductLineEnum.medusa

  override def execute(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().getOrCreate()
    import ss.implicits._

    start(ss)

    val inputPath = "/ai/tmp/output/test/medusa/featureEngineering/tag/trainFeaturesReIndexNormalize2/Latest"
    val testPath = "/ai/tmp/output/test/medusa/featureEngineering/tag/testFeaturesReIndexNormalize2/Latest"
    val modelPath = "/ai/tmp/PSLinearRegModelEpoch2_tag"
    val predictionPath = "PSLinearRegPredictEpoch2_tag"
    val featureSize = 527595

    val input = DataReader.read(new HdfsPath(inputPath)).select("features", "label")
      .rdd.repartition(20000).map(r => {
      val label = r.getAs[Double]("label")
      val features = r.getAs[SparseVector]("features")
      val dim = features.size
      val keys = features.indices
      val vals = features.values
      val vector = VFactory.sparseDoubleVector(dim, keys, vals)
      new LabeledData(vector, if(label > 1.0) 1.0 else label)
    })

    SharedConf.get().set(MLConf.ML_MODEL_TYPE, RowType.T_DOUBLE_DENSE.toString)
    SharedConf.get().setInt(MLConf.ML_FEATURE_INDEX_RANGE, featureSize)
    SharedConf.get().setDouble(MLConf.ML_LEARN_RATE, 0.01)
    SharedConf.get().set(MLConf.ML_DATA_INPUT_FORMAT, "libsvm")
    SharedConf.get().setInt(MLConf.ML_EPOCH_NUM, 1)
    SharedConf.get().setDouble(MLConf.ML_VALIDATE_RATIO, 0.05)
    SharedConf.get().setDouble(MLConf.ML_REG_L1, 0.001)
    SharedConf.get().setDouble(MLConf.ML_REG_L2, 0.001)
    SharedConf.get().setDouble(MLConf.ML_BATCH_SAMPLE_RATIO, 0.04)

    SharedConf.get().set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString)

    val model = new LinearRegressionPS
    val learner = new OfflineLearner

    model.init(input.getNumPartitions)

    learner.train(input, model)
    model.save(modelPath)

    /**
      * 以下为预测过程
      */
    model.load(modelPath)

    val test = DataReader.read(new HdfsPath(testPath)).select("features", "label").filter("label >= 0.2")
      .rdd.map(r => {
      val label = r.getAs[Double]("label")
      val labelModify = if(label > 1.0) 1.0 else label
      val features = r.getAs[SparseVector]("features")
      val dim = features.size
      val keys = features.indices
      val vals = features.values
      val vector = VFactory.sparseDoubleVector(dim, keys, vals)
      (new LabeledData(vector, labelModify), labelModify.toString)
    })

    val prediction = learner.predict(test, model).toDF("prediction", "label")

    BizUtils.outputWrite(prediction, predictionPath)
    stop()
  }
}
