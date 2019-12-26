package cn.featureEngineering.sparkOnAngel

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.Layer
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.FCLayer
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.optimizer.loss.L2Loss
import com.tencent.angel.ml.core.network.transfunc.{Identity, Relu}
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.spark.ml.core.GraphModel

/**
  * Created by cheng_huan on 2019/9/29.
  */
class WideAndDeepPS extends GraphModel{
  val numFields: Int = SharedConf.get().getInt(MLConf.ML_FIELD_NUM)
  val numFactors: Int = SharedConf.get().getInt(MLConf.ML_RANK_NUM)
  val lr: Double = SharedConf.get().getDouble(MLConf.ML_LEARN_RATE)
  val gamma: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_GAMMA)
  val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_BETA)

  override def network(): Unit = {
    val optimizer = new Adam(lr, gamma, beta)

    val wide = new SimpleInputLayer("input", 1, new Identity(), optimizer)

    val embedding = new Embedding("embedding", numFields * numFactors, numFactors, optimizer)
    val hidden1 = new FCLayer("hidden1", 128, embedding, new Relu, optimizer)
    val hidden2 = new FCLayer("hidden2", 64, hidden1, new Relu, optimizer)
    val deep = new FCLayer("hidden3", 1, hidden2, new Identity, optimizer)

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, deep))
    new SimpleLossLayer("simpleLossLayer", join, new L2Loss)
  }

}
