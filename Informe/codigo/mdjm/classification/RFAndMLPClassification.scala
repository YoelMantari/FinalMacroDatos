package mdjm.classification

import mdjm.{FeaturePipeline, MetricsWriter}
import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Las 2 consultas de clasificacion pedidas: RandomForest y Multilayer
  * Perceptron. Ambos existen "por defecto" en Spark MLlib
  * (`RandomForestClassifier`, `MultilayerPerceptronClassifier`), asi que se
  * usan directamente via Pipeline, sin necesidad de derivarlos.
  *
  * Label: TIPO_PREDIO (Exclusivo si PORCENTAJE_CONDOMINIO=100, si no Compartido).
  */
object RFAndMLPClassification {

  def run(spark: SparkSession, emisiones: DataFrame): Seq[MetricsWriter.ClassificationMetricsRow] = {
    val labeled = FeaturePipeline.withClassificationLabel(emisiones)

    val indexer = new StringIndexer()
      .setInputCol(FeaturePipeline.ClassificationLabelCol)
      .setOutputCol("label")
      .fit(labeled)
    val indexed = indexer.transform(labeled)

    val assembled = FeaturePipeline.assembleFeatures(indexed, FeaturePipeline.ClassificationFeatureCols)
    val (train, test) = FeaturePipeline.trainTestSplit(assembled)

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(100)
      .setMaxDepth(8)
      .setSeed(42L)
    val rfModel = rf.fit(train)
    val rfMetrics = evaluate("RandomForest", rfModel.transform(test))

    val numClasses = indexer.labels.length
    val layers = Array(FeaturePipeline.ClassificationFeatureCols.length, 8, 4, numClasses)
    val mlp = new MultilayerPerceptronClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setLayers(layers)
      .setSeed(42L)
      .setMaxIter(200)
    val mlpModel = mlp.fit(train)
    val mlpMetrics = evaluate("MLPClassifier", mlpModel.transform(test))

    Seq(rfMetrics, mlpMetrics)
  }

  private def evaluate(name: String, predictions: DataFrame): MetricsWriter.ClassificationMetricsRow = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
    val precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
    val recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
    val f1 = evaluator.setMetricName("f1").evaluate(predictions)

    MetricsWriter.ClassificationMetricsRow(name, accuracy, precision, recall, f1)
  }
}
