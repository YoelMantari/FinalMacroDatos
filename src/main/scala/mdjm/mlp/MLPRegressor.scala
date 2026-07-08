package mdjm.mlp

import org.apache.spark.rdd.RDD

/** Multilayer Perceptron para REGRESION, derivado a mano porque Spark MLlib
  * solo trae `MultilayerPerceptronClassifier` (clasificacion, salida softmax).
  * Arquitectura: 1 capa oculta (tanh) + salida lineal, perdida MSE, entrenada
  * con descenso de gradiente distribuido (forward+backward por particion,
  * sumados via `treeAggregate`, actualizacion de pesos en el driver).
  */
object MLPRegressor {

  case class Weights(w1: Array[Array[Double]], b1: Array[Double], w2: Array[Double], b2: Double)

  private case class Grad(
    gw1: Array[Array[Double]],
    gb1: Array[Double],
    gw2: Array[Double],
    gb2: Double,
    loss: Double,
    n: Long
  )

  private def zeroGrad(hidden: Int, input: Int): Grad =
    Grad(Array.fill(hidden, input)(0.0), Array.fill(hidden)(0.0), Array.fill(hidden)(0.0), 0.0, 0.0, 0L)

  def initWeights(inputSize: Int, hiddenSize: Int, seed: Long = 42L): Weights = {
    val rnd = new scala.util.Random(seed)
    val scale = math.sqrt(1.0 / inputSize)
    Weights(
      Array.fill(hiddenSize, inputSize)(rnd.nextGaussian() * scale),
      Array.fill(hiddenSize)(0.0),
      Array.fill(hiddenSize)(rnd.nextGaussian() * scale),
      0.0
    )
  }

  /** Forward pass: devuelve (activaciones de la capa oculta, prediccion). */
  def forward(w: Weights, x: Array[Double]): (Array[Double], Double) = {
    val hidden = w.b1.length
    val a1 = new Array[Double](hidden)
    var h = 0
    while (h < hidden) {
      var s = w.b1(h)
      var i = 0
      while (i < x.length) { s += w.w1(h)(i) * x(i); i += 1 }
      a1(h) = math.tanh(s)
      h += 1
    }
    var yhat = w.b2
    h = 0
    while (h < hidden) { yhat += w.w2(h) * a1(h); h += 1 }
    (a1, yhat)
  }

  def predict(w: Weights, x: Array[Double]): Double = forward(w, x)._2

  private def accumulateSample(w: Weights, x: Array[Double], y: Double, acc: Grad): Grad = {
    val (a1, yhat) = forward(w, x)
    val hidden = w.b1.length
    val dyhat = 2.0 * (yhat - y)
    var h = 0
    while (h < hidden) {
      val dz1 = dyhat * w.w2(h) * (1 - a1(h) * a1(h))
      acc.gw2(h) += dyhat * a1(h)
      acc.gb1(h) += dz1
      var i = 0
      while (i < x.length) { acc.gw1(h)(i) += dz1 * x(i); i += 1 }
      h += 1
    }
    Grad(acc.gw1, acc.gb1, acc.gw2, acc.gb2 + dyhat, acc.loss + (yhat - y) * (yhat - y), acc.n + 1)
  }

  private def combineGrads(a: Grad, b: Grad): Grad = {
    val hidden = a.gb1.length
    val input = a.gw1.headOption.map(_.length).getOrElse(0)
    Grad(
      Array.tabulate(hidden, input)((h, i) => a.gw1(h)(i) + b.gw1(h)(i)),
      Array.tabulate(hidden)(h => a.gb1(h) + b.gb1(h)),
      Array.tabulate(hidden)(h => a.gw2(h) + b.gw2(h)),
      a.gb2 + b.gb2,
      a.loss + b.loss,
      a.n + b.n
    )
  }

  private def applyUpdate(w: Weights, g: Grad, n: Double, lr: Double): Weights = {
    val hidden = w.b1.length
    val input = w.w1.headOption.map(_.length).getOrElse(0)
    Weights(
      Array.tabulate(hidden, input)((h, i) => w.w1(h)(i) - lr * (g.gw1(h)(i) / n)),
      Array.tabulate(hidden)(h => w.b1(h) - lr * (g.gb1(h) / n)),
      Array.tabulate(hidden)(h => w.w2(h) - lr * (g.gw2(h) / n)),
      w.b2 - lr * (g.gb2 / n)
    )
  }

  /** Entrena por `epochs` epocas de descenso de gradiente batch distribuido.
    * Devuelve los pesos finales y la curva de perdida (epoca -> MSE).
    */
  def train(
    data: RDD[(Array[Double], Double)],
    inputSize: Int,
    hiddenSize: Int = 16,
    epochs: Int = 100,
    lr: Double = 0.05,
    seed: Long = 42L
  ): (Weights, Seq[(Int, Double)]) = {
    val cached = data.cache()
    val sc = cached.sparkContext
    var weights = initWeights(inputSize, hiddenSize, seed)
    val lossHistory = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    var epoch = 1
    while (epoch <= epochs) {
      val bc = sc.broadcast(weights)
      val grad = cached.treeAggregate(zeroGrad(hiddenSize, inputSize))(
        seqOp = (acc, sample) => accumulateSample(bc.value, sample._1, sample._2, acc),
        combOp = combineGrads
      )
      bc.destroy()
      val n = math.max(grad.n, 1L).toDouble
      weights = applyUpdate(weights, grad, n, lr)
      lossHistory += ((epoch, grad.loss / n))
      epoch += 1
    }
    (weights, lossHistory.toSeq)
  }
}
