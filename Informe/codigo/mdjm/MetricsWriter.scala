package mdjm

import org.apache.spark.sql.SparkSession

/** Escribe las tablas de metricas y las curvas de perdida de entrenamiento
  * a JSON, con un esquema uniforme para que mas adelante se puedan unir
  * (`unionByName`) con los resultados de la corrida en cluster real.
  */
object MetricsWriter {

  case class RegressionMetricsRow(model: String, rmse: Double, mse: Double, mae: Double, r2: Double)
  case class ClassificationMetricsRow(model: String, accuracy: Double, precision: Double, recall: Double, f1: Double)
  case class LossPoint(epoch: Int, loss: Double)

  def writeRegressionMetrics(spark: SparkSession, rows: Seq[RegressionMetricsRow], outputPath: String): Unit = {
    import spark.implicits._
    rows.toDF().coalesce(1).write.mode("overwrite").json(s"$outputPath/metrics/regression_metrics.json")
  }

  def writeClassificationMetrics(spark: SparkSession, rows: Seq[ClassificationMetricsRow], outputPath: String): Unit = {
    import spark.implicits._
    rows.toDF().coalesce(1).write.mode("overwrite").json(s"$outputPath/metrics/classification_metrics.json")
  }

  def writeLossCurve(spark: SparkSession, modelName: String, curve: Seq[(Int, Double)], outputPath: String): Unit = {
    import spark.implicits._
    val rows = curve.map { case (epoch, loss) => LossPoint(epoch, loss) }
    rows.toDF().coalesce(1).write.mode("overwrite").json(s"$outputPath/loss_curves/${modelName}_loss.json")
  }
}
