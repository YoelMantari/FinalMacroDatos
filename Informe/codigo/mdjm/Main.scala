package mdjm

import mdjm.classification.RFAndMLPClassification
import mdjm.mlp.MLPRegressor
import mdjm.svr.SVRegressor
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Orquestador: recibe todo por argumentos de linea de comandos, nunca
  * hardcodea el master ni las rutas, para que el mismo jar corra hoy en
  * `local[*]`/`file://` y despues (sin recompilar) contra el cluster real
  * (`hdfs://<master>:9000`), pasando solo --master y las rutas distintas al
  * invocar `spark-submit`.
  */
object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Uso: spark-submit ... mdjm.Main <inputCsvPath> <outputBasePath> <etl|sql|regression|classification|all>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val command = args(2)

    val spark = SparkSession.builder().appName("ArbitriosMDJM").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      command match {
        case "etl"            => ETL.run(spark, inputPath, outputPath)
        case "sql"            => runSql(spark, outputPath)
        case "regression"     => runRegression(spark, outputPath)
        case "classification" => runClassification(spark, outputPath)
        case "all" =>
          ETL.run(spark, inputPath, outputPath)
          runSql(spark, outputPath)
          runRegression(spark, outputPath)
          runClassification(spark, outputPath)
        case other =>
          System.err.println(s"Comando desconocido: $other")
          System.exit(1)
      }
    } finally {
      spark.stop()
    }
  }

  private def loadEmisiones(spark: SparkSession, outputPath: String): DataFrame =
    spark.read.json(s"$outputPath/tables/emisiones.json")

  private def runSql(spark: SparkSession, outputPath: String): Unit =
    SqlQueries.run(spark, loadEmisiones(spark, outputPath), outputPath)

  private def runRegression(spark: SparkSession, outputPath: String): Unit = {
    val emisiones = loadEmisiones(spark, outputPath)
    val assembled = FeaturePipeline.assembleFeatures(emisiones)
    val scaler = FeaturePipeline.fitScaler(assembled)
    val scaled = scaler.transform(assembled)

    val labelScaler = FeaturePipeline.computeLabelScaler(scaled, FeaturePipeline.RegressionLabelCol)
    val scaleLabelUdf = udf((y: Double) => labelScaler.scale(y))
    val withScaledLabel = scaled.withColumn("scaledLabel", scaleLabelUdf(col(FeaturePipeline.RegressionLabelCol)))

    val (train, test) = FeaturePipeline.trainTestSplit(withScaledLabel)
    val inputSize = FeaturePipeline.FeatureCols.length

    val trainRDD = FeaturePipeline.toArrayLabelRDD(train, "scaledFeatures", "scaledLabel").cache()
    val testRDD = FeaturePipeline.toArrayLabelRDD(test, "scaledFeatures", FeaturePipeline.RegressionLabelCol).cache()

    val (mlpWeights, mlpLoss) = MLPRegressor.train(trainRDD, inputSize, hiddenSize = 16, epochs = 100, lr = 0.05)
    val mlpMetrics = evaluateRegression(spark, "MLPRegressor", testRDD, x => MLPRegressor.predict(mlpWeights, x), labelScaler)

    val (svrWeights, svrLoss) = SVRegressor.train(trainRDD, inputSize, epochs = 400, lr = 0.2, epsilon = 0.1, lambda = 1e-4)
    val svrMetrics = evaluateRegression(spark, "SVRegressor", testRDD, x => SVRegressor.predict(svrWeights, x), labelScaler)

    MetricsWriter.writeRegressionMetrics(spark, Seq(mlpMetrics, svrMetrics), outputPath)
    MetricsWriter.writeLossCurve(spark, "mlp_regressor", mlpLoss, outputPath)
    MetricsWriter.writeLossCurve(spark, "svr_regressor", svrLoss, outputPath)

    println(s"[Regresion] MLP: $mlpMetrics")
    println(s"[Regresion] SVR: $svrMetrics")
  }

  private def evaluateRegression(
    spark: SparkSession,
    name: String,
    testRDD: RDD[(Array[Double], Double)],
    predictScaled: Array[Double] => Double,
    labelScaler: FeaturePipeline.LabelScaler
  ): MetricsWriter.RegressionMetricsRow = {
    import spark.implicits._
    val predDF = testRDD.map { case (x, yOriginal) =>
      (labelScaler.unscale(predictScaled(x)), yOriginal)
    }.toDF("prediction", "label")

    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction")
    val rmse = evaluator.setMetricName("rmse").evaluate(predDF)
    val mse = evaluator.setMetricName("mse").evaluate(predDF)
    val mae = evaluator.setMetricName("mae").evaluate(predDF)
    val r2 = evaluator.setMetricName("r2").evaluate(predDF)
    MetricsWriter.RegressionMetricsRow(name, rmse, mse, mae, r2)
  }

  private def runClassification(spark: SparkSession, outputPath: String): Unit = {
    val emisiones = loadEmisiones(spark, outputPath)
    val metrics = RFAndMLPClassification.run(spark, emisiones)
    MetricsWriter.writeClassificationMetrics(spark, metrics, outputPath)
    metrics.foreach(m => println(s"[Clasificacion] $m"))
  }
}
