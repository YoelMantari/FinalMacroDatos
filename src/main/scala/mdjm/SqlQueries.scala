package mdjm

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Las 2 consultas Spark SQL pedidas en el enunciado:
  *   1. API DataFrame con `groupBy` + funciones de `org.apache.spark.sql.functions`.
  *   2. Vista temporal + `spark.sql` con `ORDER BY` combinado con `WHERE`/`LIMIT`.
  */
object SqlQueries {

  /** Total y promedio recaudado por concepto, agrupado por periodo (grupo + funciones). */
  def query1GroupByPeriodo(emisiones: DataFrame): DataFrame =
    emisiones.groupBy("PERIODO")
      .agg(
        round(sum("MONTO_PARQUE_JARDIN"), 2).as("total_parque_jardin"),
        round(avg("MONTO_PARQUE_JARDIN"), 2).as("avg_parque_jardin"),
        round(sum("MONTO_SERENAZGO"), 2).as("total_serenazgo"),
        round(avg("MONTO_SERENAZGO"), 2).as("avg_serenazgo"),
        round(sum("MONTO_RESIDUO_SOLIDO"), 2).as("total_residuo_solido"),
        round(avg("MONTO_RESIDUO_SOLIDO"), 2).as("avg_residuo_solido"),
        round(sum("MONTO_BARRIDO_CALLE"), 2).as("total_barrido_calle"),
        round(avg("MONTO_BARRIDO_CALLE"), 2).as("avg_barrido_calle"),
        count(lit(1)).as("num_emisiones")
      )
      .orderBy("PERIODO")

  /** Top 20 predios (propiedad exclusiva, periodo 2025) por monto total neto pagado,
    * via vista temporal + SQL con WHERE + ORDER BY + LIMIT.
    */
  def query2TopPrediosTempView(spark: SparkSession, emisiones: DataFrame): DataFrame = {
    emisiones.createOrReplaceTempView("emisiones")
    spark.sql(
      """
        |SELECT
        |  COD_PREDIO,
        |  COD_CONTRIBUYENTE,
        |  PERIODO,
        |  ROUND(
        |    MONTO_PARQUE_JARDIN + MONTO_SERENAZGO + MONTO_RESIDUO_SOLIDO + MONTO_BARRIDO_CALLE
        |    - DESCUENTO_TOPE_PARQUE_JARDIN - DESCUENTO_TOPE_SERENAZGO
        |    - MONTO_TOPE_RESIDUO_SOLIDO - DESCUENTO_TOPE_BARRIO_CALLE, 2
        |  ) AS monto_total_neto
        |FROM emisiones
        |WHERE PERIODO = 2025 AND PORCENTAJE_CONDOMINIO = 100
        |ORDER BY monto_total_neto DESC
        |LIMIT 20
        |""".stripMargin)
  }

  def run(spark: SparkSession, emisiones: DataFrame, outputPath: String): Unit = {
    val q1 = query1GroupByPeriodo(emisiones)
    q1.write.mode("overwrite").json(s"$outputPath/queries/query1_groupby_periodo.json")
    println("[SQL Query 1] Recaudacion total/promedio por periodo:")
    q1.show(false)

    val q2 = query2TopPrediosTempView(spark, emisiones)
    q2.write.mode("overwrite").json(s"$outputPath/queries/query2_top_predios_2025.json")
    println("[SQL Query 2] Top 20 predios exclusivos 2025 por monto total neto:")
    q2.show(20, truncate = false)
  }
}
