# Proyecto Final CC531 - Arbitrios Municipales MDJM

Proyecto en Scala/Spark para procesar el dataset publico de emision de
arbitrios municipales de la Municipalidad Distrital de Jesus Maria (MDJM).
El flujo fue ejecutado en modo local y en un cluster Hadoop/Spark con YARN de
tres nodos.

Fuente del dataset:
https://www.datosabiertos.gob.pe/dataset/emisi%C3%B3n-de-arbitrios-municipales-de-la-municipalidad-distrital-de-jes%C3%BAs-mar%C3%ADa-mdjm

## Objetivo

Analizar la emision de arbitrios municipales por predio y periodo usando
Spark. El proyecto cubre:

- ETL desde CSV hacia dos tablas JSON.
- Consultas Spark SQL sobre la tabla de emisiones.
- Modelos de regresion para estimar `MONTO_SERENAZGO`.
- Modelos de clasificacion para estimar `TIPO_PREDIO`.
- Comparacion de ejecucion local frente a ejecucion distribuida en YARN.
- Informe y presentacion en LaTeX dentro de `Informe/`.

## Dataset

Archivo trabajado: `data/dataset_vf_3.csv`.

Resumen:

- Registros de entrada: 222,874 filas.
- Periodos: 2023, 2024 y 2025.
- Separador: `;`.
- Tema: emision de arbitrios municipales por predio.
- Conceptos monetarios: parques y jardines, serenazgo, residuos solidos y
  barrido de calles.

Campos principales usados:

- `PERIODO`
- `COD_PREDIO`
- `COD_CONTRIBUYENTE`
- `NOM_CONTRIBUYENTE`
- `PORCENTAJE_CONDOMINIO`
- `MONTO_PARQUE_JARDIN`
- `MONTO_SERENAZGO`
- `MONTO_RESIDUO_SOLIDO`
- `MONTO_BARRIDO_CALLE`
- descuentos y topes asociados a los conceptos anteriores
- `UBIGEO`

Los montos vienen con formato decimal tipo `1,257.60`; el ETL limpia esas
columnas para que Spark las trate como valores numericos.

## Arquitectura Probada

Ambientes usados:

| Ambiente | Motor | Nodos | Recursos observados | Salida |
|---|---|---:|---|---|
| Local / mononodo | `local[*]` | 1 | CPU local | `output/` |
| Cluster multinodo | Spark sobre YARN + HDFS | 3 | 8 GB YARN, 4 vcores | `output_multinodo_3nodes/` |

Cluster distribuido:

- `master`
- `slave1`
- `slave-base`
- Hadoop 3.3.0
- Spark 3.4.0
- Ejecucion con `--master yarn`
- Entrada HDFS: `hdfs:///mdjm/input/dataset_vf_3.csv`
- Salida HDFS verificada: `hdfs:///mdjm/output_3nodes_verify_20260707_2325`

## Flujo del Proyecto

El programa principal es `mdjm.Main` y recibe:

```bash
spark-submit --class mdjm.Main <jar> <inputCsvPath> <outputBasePath> <comando>
```

Comandos disponibles:

| Comando | Funcion | Salida |
|---|---|---|
| `etl` | Lee el CSV, limpia columnas y genera tablas JSON | `<output>/tables/` |
| `sql` | Ejecuta las dos consultas Spark SQL | `<output>/queries/` |
| `regression` | Entrena MLPRegressor y SVRegressor | `<output>/metrics/`, `<output>/loss_curves/` |
| `classification` | Entrena RandomForest y MLPClassifier | `<output>/metrics/` |
| `all` | Ejecuta todo el flujo en orden | todas las anteriores |

`sql`, `regression` y `classification` leen la tabla generada por `etl`:
`<output>/tables/emisiones.json`.

## Tablas JSON Generadas

El ETL genera dos tablas principales:

- `contribuyentes.json`: dimension de contribuyentes con codigo y nombre.
- `emisiones.json`: tabla de hechos con granularidad
  `(COD_PREDIO, COD_CONTRIBUYENTE, PERIODO)`.

La tabla `emisiones.json` concentra montos, descuentos, porcentaje de
condominio y ubigeo. Es la base de las consultas SQL y de los modelos ML.

## Consultas Spark SQL

### Consulta 1: emision por periodo

Archivo de salida:

```text
queries/query1_groupby_periodo.json
```

Calcula totales y promedios por `PERIODO` para:

- `MONTO_PARQUE_JARDIN`
- `MONTO_SERENAZGO`
- `MONTO_RESIDUO_SOLIDO`
- `MONTO_BARRIDO_CALLE`

Implementacion:

- `SqlQueries.query1GroupByPeriodo`
- usa `groupBy("PERIODO")`
- usa `sum`, `avg`, `count` y `round`

Interpretacion principal:

- `MONTO_SERENAZGO` es el concepto con mayor monto total en los tres periodos.
- El numero de emisiones aumenta hacia 2025.
- La consulta permite observar la composicion anual de la emision municipal.

### Consulta 2: top predios exclusivos 2025

Archivo de salida:

```text
queries/query2_top_predios_2025.json
```

Filtra predios con:

- `PERIODO = 2025`
- `PORCENTAJE_CONDOMINIO = 100`

Luego calcula `monto_total_neto` y devuelve los 20 predios con mayor monto.

Implementacion:

- `SqlQueries.query2TopPrediosTempView`
- usa vista temporal
- usa `WHERE`
- calcula monto neto
- usa `ORDER BY monto_total_neto DESC`
- usa `LIMIT 20`

Interpretacion principal:

- Permite identificar predios con mayor carga neta emitida.
- Sirve para revisar concentracion de montos, casos de alto impacto o posibles
  prioridades de revision municipal.

## Modelos de Machine Learning

Los modelos se entrenan sobre `emisiones.json`.

La division usada es 80/20:

- 80% entrenamiento
- 20% prueba

Esto permite evaluar los modelos sobre registros no usados durante el
entrenamiento.

### Regresion

Objetivo:

- Estimar `MONTO_SERENAZGO`.

Modelos:

- `MLPRegressor`: implementado manualmente con una capa oculta, salida lineal y
  perdida MSE.
- `SVRegressor`: implementado manualmente como SVR lineal con perdida
  epsilon-insensitiva.

Features principales:

- `PERIODO`
- `PORCENTAJE_CONDOMINIO`
- montos de otros conceptos
- descuentos/topes seleccionados

Codigo:

- `src/main/scala/mdjm/mlp/MLPRegressor.scala`
- `src/main/scala/mdjm/svr/SVRegressor.scala`
- orquestacion en `src/main/scala/mdjm/Main.scala`

### Clasificacion

Objetivo:

- Clasificar `TIPO_PREDIO`.

La etiqueta se construye asi:

- `Exclusivo` si `PORCENTAJE_CONDOMINIO = 100`
- `Compartido` en los demas casos

Modelos:

- `RandomForestClassifier`
- `MultilayerPerceptronClassifier`

Para evitar fuga de datos, `PORCENTAJE_CONDOMINIO` no se usa como feature en
clasificacion, porque es la columna que define directamente la etiqueta.

Codigo:

- `src/main/scala/mdjm/classification/RFAndMLPClassification.scala`
- features compartidas en `src/main/scala/mdjm/FeaturePipeline.scala`

## Resultados Obtenidos

### Rendimiento

| Ambiente | Nodos | Tiempo aproximado | Observacion |
|---|---:|---:|---|
| Local / mononodo | 1 | 7 min 02 s | Ejecucion `local[*]` |
| Multinodo YARN | 3 | 18 min 45 s | Ejecucion distribuida sobre HDFS/YARN |

El cluster funciono correctamente, pero no fue mas rapido para este volumen de
datos. La causa principal es el overhead de YARN, HDFS, shuffle y virtualizacion
frente a una data relativamente pequena. El valor del cluster en este proyecto
esta en validar la ejecucion distribuida real, no en obtener speedup con una
base pequena.

### Metricas de Regresion

| Modelo | Ambiente | RMSE | MSE | MAE | R2 |
|---|---|---:|---:|---:|---:|
| MLPRegressor | Local | 1352.55 | 1,829,399.43 | 281.91 | 0.2233 |
| MLPRegressor | Cluster | 1384.64 | 1,917,232.43 | 284.91 | 0.2302 |
| SVRegressor | Local | 1430.10 | 2,045,182.64 | 232.98 | 0.1317 |
| SVRegressor | Cluster | 1486.36 | 2,209,266.74 | 257.77 | 0.1130 |

Lectura:

- `MLPRegressor` obtiene menor RMSE que `SVRegressor`.
- Ambos modelos tienen R2 positivo pero moderado.
- Las variables disponibles explican parte del comportamiento de
  `MONTO_SERENAZGO`, pero no toda su variabilidad.

### Metricas de Clasificacion

| Modelo | Ambiente | Accuracy | Precision | Recall | F1 |
|---|---|---:|---:|---:|---:|
| RandomForest | Local | 0.8614 | 0.8833 | 0.8614 | 0.8421 |
| RandomForest | Cluster | 0.8593 | 0.8819 | 0.8593 | 0.8397 |
| MLPClassifier | Local | 0.7458 | 0.8367 | 0.7458 | 0.7615 |
| MLPClassifier | Cluster | 0.7371 | 0.5433 | 0.7371 | 0.6255 |

Lectura:

- `RandomForest` fue el clasificador mas estable.
- `MLPClassifier` funciono, pero fue mas sensible al particionamiento y a la
  configuracion de entrenamiento.
- Los resultados permiten reconocer patrones asociados al tipo de predio, no
  reemplazar una decision administrativa.

## Como Compilar

El proyecto no usa Maven ni sbt. Compila con el compilador Scala incluido en
Spark y empaqueta con `jar`.

```bash
make build
```

Esto genera:

```text
target/arbitrios-mdjm.jar
```

Si Spark esta en otra ruta:

```bash
make build SPARK_HOME=/ruta/a/spark
```

## Ejecucion Local

Desde la raiz del proyecto:

```bash
make run-all
```

Por defecto usa:

```text
SPARK_MASTER=local[*]
INPUT=file://<proyecto>/data/dataset_vf_3.csv
OUTPUT=file://<proyecto>/output
```

Tambien se puede ejecutar por etapas:

```bash
make run-etl
make run-sql
make run-regression
make run-classification
```

## Ejecucion Multinodo con YARN

En el nodo master del cluster:

```bash
hdfs dfs -mkdir -p /mdjm/input
hdfs dfs -put -f data/dataset_vf_3.csv /mdjm/input/dataset_vf_3.csv
```

Ejecucion completa:

```bash
/opt/spark/bin/spark-submit \
  --class mdjm.Main \
  --master yarn \
  target/arbitrios-mdjm.jar \
  hdfs:///mdjm/input/dataset_vf_3.csv \
  hdfs:///mdjm/output_3nodes_verify_20260707_2325 \
  all
```

Equivalente con `make`:

```bash
make run-all \
  SPARK_MASTER=yarn \
  INPUT=hdfs:///mdjm/input/dataset_vf_3.csv \
  OUTPUT=hdfs:///mdjm/output_3nodes_verify_20260707_2325
```

Para verificar nodos activos:

```bash
yarn node -list -showDetails
```

Para revisar salidas en HDFS:

```bash
hdfs dfs -ls -R /mdjm/output_3nodes_verify_20260707_2325
```

## Salidas del Proyecto

Salida local:

```text
output/
  tables/
    contribuyentes.json/
    emisiones.json/
  queries/
    query1_groupby_periodo.json/
    query2_top_predios_2025.json/
  metrics/
    regression_metrics.json/
    classification_metrics.json/
  loss_curves/
    mlp_regressor_loss.json/
    svr_regressor_loss.json/
```

Salida multinodo copiada desde HDFS:

```text
output_multinodo_3nodes/
  tables/
  queries/
  metrics/
  loss_curves/
```

Los directorios de salida de Spark contienen archivos `part-*` y `_SUCCESS`.
La presencia de `_SUCCESS` indica que la escritura termino correctamente.

## Estructura del Proyecto

```text
Final-Macrodatos/
  Makefile
  README.md
  DatasetContext.md
  Finalcc531_261-2.md
  data/
    dataset_vf_3.csv
  src/main/scala/mdjm/
    ETL.scala
    SqlQueries.scala
    FeaturePipeline.scala
    MetricsWriter.scala
    Main.scala
    mlp/MLPRegressor.scala
    svr/SVRegressor.scala
    classification/RFAndMLPClassification.scala
  output/
  output_multinodo_3nodes/
  Informe/
    main.tex
    Template.tex
    presentacion.tex
    sections/
    tables/
    ImgConfi/
    ImgNod1/
    ImgMultiNodo/
```

## Informe y Presentacion

La carpeta `Informe/` contiene:

- `main.tex`: informe principal.
- `Template.tex`: wrapper para compilar en Overleaf.
- `presentacion.tex`: presentacion Beamer.
- `sections/`: secciones del informe.
- `tables/`: tablas de resultados.
- `ImgConfi/`, `ImgNod1/`, `ImgMultiNodo/`: evidencias visuales.
- `DESCRIPCION_IMAGENES.md`: descripcion de las imagenes usadas.

## Interpretacion General

El proyecto transforma un dataset administrativo en una base analitica para
entender la emision municipal. Las consultas SQL permiten describir montos por
periodo y detectar predios con mayor carga neta. Los modelos ML permiten
evaluar si, a partir de variables de emision, es posible estimar el monto de
serenazgo o clasificar el tipo de predio.

El resultado mas importante no es solo el calculo final, sino la ejecucion del
flujo completo en Spark: lectura desde HDFS, procesamiento distribuido en YARN,
generacion de salidas JSON, metricas de modelos y evidencias de uso de los tres
nodos del cluster.
