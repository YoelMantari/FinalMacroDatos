package mdjm.svr

import org.apache.spark.rdd.RDD

/** Support Vector Regression (SVR) lineal, derivada a mano porque Spark
  * MLlib solo trae SVM de clasificacion (`LinearSVC`, hinge loss). Usa
  * perdida epsilon-insensitiva con regularizacion L2, entrenada con
  * subgradiente distribuido (misma estrategia `treeAggregate` que el MLP).
  *
  * Perdida por muestra: max(0, |y - (w.x + b)| - epsilon)
  * Perdida total:       (1/n) * sum(perdida_i) + (lambda/2) * ||w||^2
  */
object SVRegressor {

  case class Weights(w: Array[Double], b: Double)

  private case class Grad(gw: Array[Double], gb: Double, loss: Double, n: Long)

  private def zeroGrad(inputSize: Int): Grad = Grad(Array.fill(inputSize)(0.0), 0.0, 0.0, 0L)

  def initWeights(inputSize: Int, seed: Long = 42L): Weights = {
    val rnd = new scala.util.Random(seed)
    val scale = math.sqrt(1.0 / inputSize)
    Weights(Array.fill(inputSize)(rnd.nextGaussian() * scale), 0.0)
  }

  private def dot(a: Array[Double], b: Array[Double]): Double = {
    var s = 0.0
    var i = 0
    while (i < a.length) { s += a(i) * b(i); i += 1 }
    s
  }

  def predict(w: Weights, x: Array[Double]): Double = dot(w.w, x) + w.b

  private def accumulateSample(w: Weights, x: Array[Double], y: Double, eps: Double, acc: Grad): Grad = {
    val f = predict(w, x)
    val r = y - f
    val absR = math.abs(r)
    val sampleLoss = math.max(0.0, absR - eps)
    if (absR <= eps) {
      Grad(acc.gw, acc.gb, acc.loss + sampleLoss, acc.n + 1)
    } else {
      val sign = math.signum(r)
      var i = 0
      while (i < x.length) { acc.gw(i) += -sign * x(i); i += 1 }
      Grad(acc.gw, acc.gb - sign, acc.loss + sampleLoss, acc.n + 1)
    }
  }

  private def combineGrads(a: Grad, b: Grad): Grad =
    Grad(Array.tabulate(a.gw.length)(i => a.gw(i) + b.gw(i)), a.gb + b.gb, a.loss + b.loss, a.n + b.n)

  private def applyUpdate(w: Weights, g: Grad, n: Double, lr: Double, lambda: Double): Weights = {
    val newW = Array.tabulate(w.w.length) { i =>
      val reg = lambda * w.w(i)
      w.w(i) - lr * ((g.gw(i) / n) + reg)
    }
    Weights(newW, w.b - lr * (g.gb / n))
  }

  /** Entrena por `epochs` epocas de subgradiente batch distribuido.
    * Devuelve los pesos finales y la curva de perdida (epoca -> perdida epsilon-insensitiva).
    */
  def train(
    data: RDD[(Array[Double], Double)],
    inputSize: Int,
    epochs: Int = 100,
    lr: Double = 0.05,
    epsilon: Double = 0.1,
    lambda: Double = 1e-3,
    seed: Long = 42L
  ): (Weights, Seq[(Int, Double)]) = {
    val cached = data.cache()
    val sc = cached.sparkContext
    var weights = initWeights(inputSize, seed)
    val lossHistory = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    var epoch = 1
    while (epoch <= epochs) {
      val bc = sc.broadcast(weights)
      val grad = cached.treeAggregate(zeroGrad(inputSize))(
        seqOp = (acc, sample) => accumulateSample(bc.value, sample._1, sample._2, epsilon, acc),
        combOp = combineGrads
      )
      bc.destroy()
      val n = math.max(grad.n, 1L).toDouble
      weights = applyUpdate(weights, grad, n, lr, lambda)
      lossHistory += ((epoch, grad.loss / n))
      epoch += 1
    }
    (weights, lossHistory.toSeq)
  }
}
