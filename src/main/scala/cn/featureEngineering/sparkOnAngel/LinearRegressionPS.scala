package cn.featureEngineering.sparkOnAngel

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.verge.{SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.{AdaDelta, Adam, Momentum}
import com.tencent.angel.ml.core.optimizer.loss.L2Loss
import com.tencent.angel.spark.ml.core.GraphModel
/**
  * Created by cheng_huan on 2019/8/1.
  */
class LinearRegressionPS extends GraphModel{
  val lr = sharedConf.getDouble(MLConf.ML_LEARN_RATE)
  val gamma: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_GAMMA)
  val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_BETA)
  /*val momentum: Double = SharedConf.get().getDouble(MLConf.ML_OPT_MOMENTUM_MOMENTUM)
  val alpha: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADADELTA_ALPHA)
  val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADADELTA_BETA)*/

  override
  def network(): Unit = {
    //val optimizer = new AdaDelta(lr, alpha, beta)
    //val optimizer = new Momentum(lr, momentum)
    val optimizer = new Adam(lr, gamma, beta)
    val input = new SimpleInputLayer("input", 1, new Identity(), optimizer)
    new SimpleLossLayer("simpleLossLayer", input, new L2Loss)
  }
}
