package mdjm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/** Carga el CSV de arbitrios, limpia los decimales (coma como separador de
  * miles) y separa el dataset en las 2 tablas normalizadas requeridas por el
  * enunciado: `contribuyentes` (dimension) y `emisiones` (hechos).
  *
  * Granularidad real verificada sobre el CSV completo: (COD_PREDIO,
  * COD_CONTRIBUYENTE, PERIODO) es casi una clave primaria (222,873 combos
  * distintos sobre 222,874 filas). Ni COD_CONTRIBUYENTE ni
  * PORCENTAJE_CONDOMINIO son estaticos por predio (copropiedad / cambios de
  * titularidad entre periodos), por lo que la tabla de hechos usa esa
  * granularidad en vez de asumir un predio como entidad estatica.
  */
object ETL {

  val DecimalCols: Seq[String] = Seq(
    "PORCENTAJE_CONDOMINIO",
    "MONTO_PARQUE_JARDIN",
    "MONTO_SERENAZGO",
    "MONTO_RESIDUO_SOLIDO",
    "MONTO_BARRIDO_CALLE",
    "DESCUENTO_TOPE_PARQUE_JARDIN",
    "DESCUENTO_TOPE_SERENAZGO",
    "MONTO_TOPE_RESIDUO_SOLIDO",
    "DESCUENTO_TOPE_BARRIO_CALLE"
  )

  val EmisionesCols: Seq[String] = Seq(
    "FECHA_CORTE", "PERIODO", "COD_PREDIO", "COD_CONTRIBUYENTE",
    "PORCENTAJE_CONDOMINIO", "MONTO_PARQUE_JARDIN", "MONTO_SERENAZGO",
    "MONTO_RESIDUO_SOLIDO", "MONTO_BARRIDO_CALLE",
    "DESCUENTO_TOPE_PARQUE_JARDIN", "DESCUENTO_TOPE_SERENAZGO",
    "MONTO_TOPE_RESIDUO_SOLIDO", "DESCUENTO_TOPE_BARRIO_CALLE", "UBIGEO"
  )

  private val cleanNumber = udf { s: String =>
    if (s == null || s.isEmpty) null.asInstanceOf[java.lang.Double]
    else java.lang.Double.valueOf(s.replace(",", ""))
  }

  /** Lee el CSV crudo (delimitado por `;`) y castea/limpia los tipos. */
  def loadRaw(spark: SparkSession, inputPath: String): DataFrame = {
    val raw = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath)

    val withClean = DecimalCols.foldLeft(raw) { (acc, c) =>
      acc.withColumn(c, cleanNumber(col(c)).cast(DoubleType))
    }

    withClean
      .withColumn("FECHA_CORTE", col("FECHA_CORTE").cast(IntegerType))
      .withColumn("PERIODO", col("PERIODO").cast(IntegerType))
  }

  /** Separa el DataFrame limpio en (contribuyentes, emisiones). */
  def buildTables(df: DataFrame): (DataFrame, DataFrame) = {
    val latestNamePerContribuyente = Window
      .partitionBy("COD_CONTRIBUYENTE")
      .orderBy(col("PERIODO").desc, col("FECHA_CORTE").desc)

    val contribuyentes = df
      .select("COD_CONTRIBUYENTE", "NOM_CONTRIBUYENTE", "PERIODO", "FECHA_CORTE")
      .withColumn("rn", row_number().over(latestNamePerContribuyente))
      .filter(col("rn") === 1)
      .select("COD_CONTRIBUYENTE", "NOM_CONTRIBUYENTE")

    // La granularidad natural es (COD_PREDIO, COD_CONTRIBUYENTE, PERIODO);
    // se descarta el unico duplicado exacto detectado sobre esa clave.
    val emisiones = df
      .dropDuplicates("COD_PREDIO", "COD_CONTRIBUYENTE", "PERIODO")
      .select(EmisionesCols.map(col): _*)

    (contribuyentes, emisiones)
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    val df = loadRaw(spark, inputPath).cache()
    val (contribuyentes, emisiones) = buildTables(df)

    contribuyentes.write.mode("overwrite").json(s"$outputPath/tables/contribuyentes.json")
    emisiones.write.mode("overwrite").json(s"$outputPath/tables/emisiones.json")

    println(s"[ETL] filas crudas       : ${df.count()}")
    println(s"[ETL] contribuyentes.json: ${contribuyentes.count()}")
    println(s"[ETL] emisiones.json     : ${emisiones.count()}")
  }
}
