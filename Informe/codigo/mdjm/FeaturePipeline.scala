package mdjm

import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel, VectorAssembler}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/** Preparacion de features/labels compartida por los 4 modelos de ML
  * (regresion MLP/SVR derivados y clasificacion RandomForest/MLP built-in).
  */
object FeaturePipeline {

  // 8 features (decimales + PERIODO) para regresion.
  val FeatureCols: Seq[String] = Seq(
    "PERIODO", "PORCENTAJE_CONDOMINIO", "MONTO_PARQUE_JARDIN",
    "MONTO_RESIDUO_SOLIDO", "MONTO_BARRIDO_CALLE",
    "DESCUENTO_TOPE_PARQUE_JARDIN", "MONTO_TOPE_RESIDUO_SOLIDO",
    "DESCUENTO_TOPE_BARRIO_CALLE"
  )

  // Para clasificacion se excluye PORCENTAJE_CONDOMINIO: la etiqueta se
  // deriva directamente de ese campo, asi que incluirlo como feature seria
  // fuga de datos (el modelo aprenderia el umbral exacto, no un patron real).
  val ClassificationFeatureCols: Seq[String] = FeatureCols.filterNot(_ == "PORCENTAJE_CONDOMINIO")

  val RegressionLabelCol = "MONTO_SERENAZGO"
  val ClassificationLabelCol = "TIPO_PREDIO"

  /** Deriva la etiqueta categorica: Exclusivo (100% condominio) vs Compartido. */
  def withClassificationLabel(df: DataFrame): DataFrame =
    df.withColumn(
      ClassificationLabelCol,
      when(col("PORCENTAJE_CONDOMINIO") === 100.0, "Exclusivo").otherwise("Compartido")
    )

  def assembleFeatures(df: DataFrame, featureCols: Seq[String] = FeatureCols, outputCol: String = "features"): DataFrame =
    new VectorAssembler()
      .setInputCols(featureCols.toArray)
      .setOutputCol(outputCol)
      .setHandleInvalid("skip")
      .transform(df)

  def fitScaler(df: DataFrame, inputCol: String = "features", outputCol: String = "scaledFeatures"): StandardScalerModel =
    new StandardScaler()
      .setInputCol(inputCol)
      .setOutputCol(outputCol)
      .setWithMean(true)
      .setWithStd(true)
      .fit(df)

  def trainTestSplit(df: DataFrame, seed: Long = 42L): (DataFrame, DataFrame) = {
    val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed)
    (train.cache(), test.cache())
  }

  case class LabelScaler(mean: Double, std: Double) {
    def scale(y: Double): Double = (y - mean) / std
    def unscale(yScaled: Double): Double = yScaled * std + mean
  }

  def computeLabelScaler(df: DataFrame, labelCol: String): LabelScaler = {
    val row: Row = df.agg(avg(col(labelCol)).as("m"), stddev_pop(col(labelCol)).as("s")).head()
    LabelScaler(row.getAs[Double]("m"), math.max(row.getAs[Double]("s"), 1e-6))
  }

  /** DataFrame(featuresCol, labelCol) -> RDD[(Array[Double], Double)] para los
    * modelos derivados (MLP/SVR) que entrenan con treeAggregate a mano.
    */
  def toArrayLabelRDD(df: DataFrame, featuresCol: String, labelCol: String): RDD[(Array[Double], Double)] =
    df.select(featuresCol, labelCol).rdd.map { row =>
      (row.getAs[MLVector](0).toArray, row.getDouble(1))
    }
}
