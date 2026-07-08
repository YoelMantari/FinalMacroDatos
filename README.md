# Proyecto Final CC531 — Arbitrios Municipales MDJM (Spark/Scala)

Guia para reproducir esta parte del examen final en el cluster Spark+HDFS
multimodo. Escrita para que quien levante las VMs pueda correr esto sin
tener que releer todo el codigo primero.

## 1. Que es esto y que NO es

Este directorio implementa **la Parte 2 del examen** (`Finalcc531_261-2.md`)
en su parte de datos/codigo:

- Las 2 tablas en JSON exigidas por el enunciado ("para que no sea necesario
  usar postgres").
- Las 2 consultas Spark SQL (groupBy+functions, y vista temporal+orderBy).
- Las 2 consultas de regresion (MLP y SVM) — **derivadas a mano**, porque
  Spark MLlib no trae regresion MLP ni SVR por defecto (el enunciado permite
  esto: "si no hubiera por defecto en la libreria ML derivarlo o crearlo").
- Las 2 consultas de clasificacion (Random Forest y MLP) — estas si vienen
  por defecto en MLlib, se usan directo.
- Tablas de metricas (RMSE/MSE/MAE/R2 para regresion, Accuracy/Precision/
  Recall/F1 para clasificacion) y curvas de perdida de entrenamiento.

**Lo que falta y es tu parte:**

1. Levantar el cluster Spark+HDFS multimodo (1 master + 2+ esclavos) y una VM
   monomodo (o reusar la de la PC4).
2. Correr este mismo proyecto (sin tocar el codigo) en ambos, con
   `SPARK_MASTER`/`INPUT`/`OUTPUT` apuntando al cluster real — ver seccion 6.
3. Medir tiempos de ejecucion, speedup, uso de CPU/memoria (htop) por nodo —
   la tabla comparativa cluster-vs-monomodo del enunciado.
4. Capturas de pantalla de cada paso de configuracion del cluster.
5. Informe (LaTeX) y presentacion (beamer).

Todo lo de arriba (1-5) **no esta hecho todavia**. Lo de abajo (ETL, SQL, ML)
**ya esta implementado, compilado y verificado corriendo en `local[*]` en esta
maquina** — deberia correr en el cluster sin cambiar una linea de Scala,
solo cambiando parametros de `spark-submit`.

## 2. Que se esta ejecutando, exactamente

Un jar (`mdjm.Main`) con 4 comandos, cada uno un `spark-submit` independiente
(asi se puede medir tiempo/CPU/memoria por separado si se quiere, o correr
todo junto con `all`):

```
spark-submit --class mdjm.Main <jar> <inputCsvPath> <outputBasePath> <comando>
```

donde `<comando>` es uno de: `etl`, `sql`, `regression`, `classification`, `all`.

| Comando | Que hace | Codigo | Por que |
|---|---|---|---|
| `etl` | Lee el CSV, limpia decimales, separa en 2 tablas, escribe JSON en `<output>/tables/` | `ETL.scala` | Requisito: >=2 tablas JSON desde el dataset |
| `sql` | Lee `emisiones.json` (ya en HDFS), corre 2 consultas, escribe JSON en `<output>/queries/` | `SqlQueries.scala` | Requisito: 1 consulta groupBy+functions, 1 con vista temporal+orderBy |
| `regression` | Entrena MLP y SVR derivados sobre `emisiones.json`, escribe metricas y curvas de perdida | `mlp/MLPRegressor.scala`, `svr/SVRegressor.scala`, orquestado en `Main.scala` | Requisito: regresion MLP + SVM con campos decimales |
| `classification` | Entrena RandomForest y MLPClassifier (built-in) | `classification/RFAndMLPClassification.scala` | Requisito: clasificacion RF + MLP con campos decimales |
| `all` | Las 4 anteriores en una sola sesion de Spark | `Main.scala` | Conveniencia / para medir tiempo total de punta a punta |

`sql`/`regression`/`classification` **dependen de que `etl` haya corrido
antes** (leen `<output>/tables/emisiones.json`). `make run-all` ya hace todo
en orden.

## 3. Dataset y decisiones de diseño (el "por que")

`data/dataset_vf_3.csv` — Emision de arbitrios municipales de la
Municipalidad Distrital de Jesus Maria (PNDA), 222,874 filas, `;`-delimitado.
Diccionario completo en `DatasetContext.md` y `Formato_DiccionarioDatos_41.md`.

**Quirk de parseo:** los campos decimales usan coma como separador de miles
(ej. la celda literal `"1,257.60"` = 1257.60, NO 1.25760). Si alguna vez se
cambia el dataset o se agrega una columna decimal nueva, hay que sumarla a
`ETL.DecimalCols` o el parseo la va a dejar como texto.

**Por que 2 tablas y no un split obvio (predio/ubicacion):** el diseño
inicial era una tabla "predios" (estatica) + "emisiones" (variable en el
tiempo), pero al perfilar el CSV completo se encontro que **ni el
contribuyente ni el porcentaje de condominio son estaticos por predio**:
18.8% de los predios cambian de contribuyente entre 2023-2025 (copropiedad /
venta), y 5.6% cambian de `PORCENTAJE_CONDOMINIO`. La granularidad real
verificada es **(COD_PREDIO, COD_CONTRIBUYENTE, PERIODO)** — 222,873 filas
distintas sobre 222,874 (1 solo duplicado exacto, descartado). Por eso las
tablas quedaron:

- `contribuyentes.json` — dimension pura: `COD_CONTRIBUYENTE, NOM_CONTRIBUYENTE` (~47,175 filas, dedup por el nombre mas reciente).
- `emisiones.json` — hechos, a la granularidad real de arriba (~222,873 filas).

DEPARTAMENTO/PROVINCIA/DISTRITO no se guardan (son constantes: LIMA/LIMA/
JESUS MARIA en todo el dataset) — solo queda `UBIGEO` como codigo. Si tu
compañero descarga una version del dataset con mas de un distrito, hay que
revisar si sigue siendo valido omitirlos.

**Targets de ML elegidos:**

- Regresion: `MONTO_SERENAZGO` (continuo), 8 features: `PERIODO,
  PORCENTAJE_CONDOMINIO`, los 3 montos restantes y 3 de los 4 descuentos/topes
  (se excluye `DESCUENTO_TOPE_SERENAZGO` porque es directamente derivado del
  monto de serenazgo — fuga obvia).
- Clasificacion: `TIPO_PREDIO` (Exclusivo si `PORCENTAJE_CONDOMINIO=100`, si
  no Compartido), **7 features** — las mismas 8 de arriba **menos
  `PORCENTAJE_CONDOMINIO`**, porque ese campo es literalmente el que define la
  etiqueta. La primera version del pipeline lo dejaba adentro por error y
  `RandomForestClassifier` daba 99.99% de accuracy (aprendia el umbral exacto
  en 1 split) — ver seccion 5, "bugs encontrados", si tu compañero toca este
  codigo y ve un accuracy sospechosamente perfecto, es la primera sospecha.

Ambos targets usan las mismas columnas base (`FeaturePipeline.FeatureCols` /
`ClassificationFeatureCols`) para que las tablas de metricas sean
comparables entre si sin tener que reexplicar features distintas por modelo.

## 4. Consultas Spark SQL

1. **`query1_groupby_periodo.json`** — API DataFrame,
   `emisiones.groupBy("PERIODO").agg(sum(...), avg(...), count(...))` usando
   `org.apache.spark.sql.functions`. Responde: "¿cuanto se recaudo en total y
   en promedio por concepto (parques, serenazgo, residuos, barrido), por
   periodo (2023/2024/2025)?"
2. **`query2_top_predios_2025.json`** — `emisiones.createOrReplaceTempView(...)`
   + `spark.sql("SELECT ... WHERE PERIODO=2025 AND PORCENTAJE_CONDOMINIO=100
   ORDER BY monto_total_neto DESC LIMIT 20")`. Responde: "¿cuales son los 20
   predios de propiedad exclusiva que mas pagaron (neto de descuentos) en
   2025?"

## 5. Modelos de ML — que son, resultados obtenidos y bugs ya corregidos

Todos corridos localmente (`local[*]`, 1 sola maquina) para validar que el
codigo funciona antes de pasarlo al cluster. Estos numeros van a cambiar (un
poco) al correr en el cluster por el split aleatorio de train/test, pero el
orden de magnitud y las conclusiones deberian mantenerse:

| Modelo | Metricas (local, 20% test) | Nota |
|---|---|---|
| MLP Regresion (derivado) | RMSE=1381.6, MAE=285.0, **R2=0.223** | 100 epocas, 1 capa oculta (16, tanh) |
| SVR (derivado) | RMSE=1461.2, MAE=250.6, **R2=0.131** | 400 epocas, lr=0.2 — ver bug #1 abajo |
| RandomForest (clasificacion) | Accuracy=86.0%, F1=84.0% | baseline de clase mayoritaria = 74% |
| MLPClassifier (clasificacion) | Accuracy=76.9%, F1=69.1% | apenas sobre el baseline, ver nota abajo |

**Bug #1 — SVR con R2 negativo:** la primera corrida (100 epocas, lr=0.05,
lambda=1e-3) daba R2=-0.126 (peor que predecir el promedio). El gradiente
epsilon-insensitivo tiene magnitud casi constante (no proporcional al error,
a diferencia de MSE), asi que converge mucho mas lento — a la epoca 100 la
perdida de entrenamiento todavia bajaba notoriamente. Subir a 400 epocas y
lr=0.2 (y bajar lambda a 1e-4) lo arreglo (R2=0.131). Si se corre en el
cluster y el R2 de SVR sale negativo otra vez, subir `epochs`/`lr` en la
llamada a `SVRegressor.train(...)` dentro de `Main.scala` antes de sospechar
de otra cosa.

**Bug #2 — fuga de datos en clasificacion:** ver seccion 3 — accuracy de
99.99% en RandomForest fue la señal de alarma. Ya corregido excluyendo
`PORCENTAJE_CONDOMINIO` de `ClassificationFeatureCols`.

**MLPClassifier apenas sobre el baseline (76.9% vs 74%):** no es un bug, es
un resultado real — con `layers=[7,8,4,2]` y `maxIter=200` la red no
encuentra mucho mas señal que la clase mayoritaria, mientras que RandomForest
si (86%). Vale la pena mencionarlo en el informe como hallazgo (arboles
capturan mejor esta relacion que una red densa chica sin mas tuning), no
hace falta arreglarlo.

**Sobre las curvas de perdida:** `RandomForestClassifier` (basado en
arboles) y `MultilayerPerceptronClassifier` (usa LBFGS internamente) **no
exponen curva de perdida iterativa** en la API de Spark ML (a diferencia de
`LogisticRegressionModel`, que si tiene `.summary.objectiveHistory`). Por
eso el grafico de "perdida de entrenamiento" que pide el enunciado solo
aplica a los 2 modelos de regresion derivados (`mlp_regressor_loss.json`,
`svr_regressor_loss.json`), donde controlamos el loop de entrenamiento a
mano. Esto se puede explicar asi en el informe, no hace falta inventar una
curva para RF/MLPClassifier.

## 6. Build y ejecucion

Sin Maven ni sbt: se compila con el `scalac` que ya viene adentro de la
instalacion de Spark (`$SPARK_HOME/jars/scala-compiler-*.jar`) y se empaqueta
con el `jar` del JDK. Como Spark es de todas formas un requisito obligatorio
del examen, esto significa **cero herramientas nuevas que instalar y cero
Internet** para compilar en la VM del cluster.

### 6.1 Prerequisitos (ya verificados en esta maquina, confirmar en la VM)

- Java 21 (`java -version`, `jar --version`).
- Spark **4.1.2**, Scala **2.13.17** — confirmar con `spark-submit --version`.
  `$SPARK_HOME/jars/` debe incluir `scala-compiler-*.jar`,
  `scala-library-*.jar`, `scala-reflect-*.jar`, `spark-core_*.jar`,
  `spark-sql_*.jar`, `spark-mllib_*.jar` (vienen por defecto con cualquier
  distribucion oficial de Spark).
- Hadoop 3.3.3 con HDFS accesible (`hdfs dfs -ls /`).
- `make`.

Si la VM tiene Spark instalado en otra ruta que no sea `/opt/spark`, pasar
`SPARK_HOME=/ruta/real` a `make` (ver 6.2).

### 6.2 Compilar

```bash
cd Final-Macrodatos
make build                              # asume SPARK_HOME=/opt/spark
# o, si Spark esta en otra ruta:
make build SPARK_HOME=/ruta/a/spark
```

Internamente corre:

```bash
java -cp "$SPARK_HOME/jars/*" scala.tools.nsc.Main -classpath "$SPARK_HOME/jars/*" -d target/classes src/main/scala/mdjm/**/*.scala
jar cfe target/arbitrios-mdjm.jar mdjm.Main -C target/classes .
```

Genera `target/arbitrios-mdjm.jar` con el manifest apuntando a `mdjm.Main`.
Spark no viaja adentro del jar (se usa el del cluster via `spark-submit`).

### 6.3 Correr LOCAL primero (sanity check antes de ir al cluster)

```bash
make run-etl             # genera output/tables/{contribuyentes,emisiones}.json
make run-sql              # output/queries/query{1,2}_*.json
make run-regression      # output/metrics/regression_metrics.json + loss_curves/
make run-classification  # output/metrics/classification_metrics.json
# o todo junto:
make run-all
```

Por defecto usa `SPARK_MASTER=local[*]`, `INPUT=file://$(pwd)/data/dataset_vf_3.csv`,
`OUTPUT=file://$(pwd)/output` (variables del Makefile, ver `Makefile`).
Verificar que salieron los conteos esperados (ver seccion 7) antes de pasar
al cluster — si algo esta mal, es mas facil depurarlo en 1 sola maquina.

### 6.4 Correr en el cluster multimodo (tu parte)

Una vez la VM master tenga HDFS y el Spark Standalone master corriendo:

```bash
# 1. Copiar este directorio completo (o al menos target/*.jar, data/, Makefile) al master
# 2. Subir el dataset a HDFS:
hdfs dfs -mkdir -p /mdjm/input
hdfs dfs -put -f data/dataset_vf_3.csv /mdjm/input/dataset_vf_3.csv

# 3. Correr apuntando al cluster real (sin recompilar):
make run-all \
  SPARK_MASTER=spark://<master-host>:7077 \
  INPUT=hdfs://<master-host>:9000/mdjm/input/dataset_vf_3.csv \
  OUTPUT=hdfs://<master-host>:9000/mdjm/output
```

Si `spark-submit` no esta en `/opt/spark/bin/spark-submit` en la VM del
cluster, sobreescribir tambien esa variable:

```bash
make run-all SPARK_SUBMIT=/ruta/real/spark-submit SPARK_MASTER=spark://... INPUT=hdfs://... OUTPUT=hdfs://...
```

### 6.5 Correr en la VM monomodo (para la comparativa)

Mismo jar, mismo comando, cambiando solo `SPARK_MASTER` (normalmente
`local[*]` si es una sola maquina sin Spark standalone propio, o
`spark://<esa-vm>:7077` si tiene su propio master de 1 solo worker) e
`INPUT`/`OUTPUT` apuntando al HDFS de esa VM (o a `file://` si esa VM no
tiene HDFS y se corre directo desde disco local).

### 6.6 Midiendo tiempos/CPU/memoria para la tabla comparativa

Para cada corrida (monomodo y multimodo), correr con `time`:

```bash
time make run-all SPARK_MASTER=... INPUT=... OUTPUT=...
```

y en paralelo capturar `htop` en cada nodo (master y cada esclavo). Con esos
tiempos y capturas se arma la tabla de speedup/CPU/memoria que pide el
enunciado.

## 7. Como saber que salio bien (sanity checks)

Despues de `make run-all`, revisar (via `hdfs dfs -cat` o copiando a local
con `hdfs dfs -get`):

- `output/tables/contribuyentes.json` → ~47,175 filas.
- `output/tables/emisiones.json` → ~222,873 filas, sin comas en los campos
  decimales (ej. `"MONTO_RESIDUO_SOLIDO":27102.12`, no `"27,102.12"`).
- `output/queries/query1_groupby_periodo.json` → 3 filas (una por PERIODO),
  con totales del orden de 10-30 millones por concepto (ver los numeros de
  referencia en la seccion 4/logs de esta corrida local).
- `output/queries/query2_top_predios_2025.json` → 20 filas, ordenadas
  descendente por `monto_total_neto`.
- `output/metrics/regression_metrics.json` → 2 filas (MLPRegressor,
  SVRegressor), **R2 no deberia salir muy negativo** (ver bug #1); si sale
  muy negativo, es un problema de convergencia, no del dataset/cluster.
- `output/metrics/classification_metrics.json` → 2 filas (RandomForest,
  MLPClassifier), **accuracy NO deberia estar cerca de 99-100%** (eso
  significaria que se reintrodujo la fuga de datos del bug #2); el baseline
  de clase mayoritaria es 74%, asi que un accuracy razonable esta entre
  ~75% y ~90%.
- `output/loss_curves/{mlp_regressor,svr_regressor}_loss.json` → la columna
  `loss` deberia ser decreciente epoca a epoca (monotonica o casi).

## 8. Estructura del proyecto

```
Final-Macrodatos/
  Makefile                 # build (scalac+jar, sin Maven) + 5 targets de spark-submit
  README.md                 # este archivo
  src/main/scala/mdjm/
    ETL.scala                       # parseo CSV + escritura de las 2 tablas JSON
    SqlQueries.scala                 # las 2 consultas SQL
    FeaturePipeline.scala             # features/labels compartidos, StandardScaler, train/test split
    MetricsWriter.scala               # escritura de metricas y curvas de perdida a JSON
    Main.scala                          # CLI, orquestacion de las 4 etapas
    mlp/MLPRegressor.scala             # MLP de regresion derivado (treeAggregate)
    svr/SVRegressor.scala               # SVR derivado (treeAggregate)
    classification/RFAndMLPClassification.scala   # RandomForest + MLPClassifier (built-in)
  data/dataset_vf_3.csv        # copia local del CSV (para correr con file://)
  output/
    tables/    # contribuyentes.json, emisiones.json
    queries/   # query1_groupby_periodo.json, query2_top_predios_2025.json
    metrics/   # regression_metrics.json, classification_metrics.json
    loss_curves/  # mlp_regressor_loss.json, svr_regressor_loss.json
  target/                    # generado por `make build` (classes/ + arbitrios-mdjm.jar)
```

## 9. Mapeo item del examen -> codigo

| Item del examen (`Finalcc531_261-2.md`) | Objeto Scala | Salida |
|---|---|---|
| Generar >=2 tablas en JSON | `mdjm.ETL` | `output/tables/{contribuyentes,emisiones}.json` |
| Consulta SQL con groupBy + `org.apache.spark.sql.functions` | `mdjm.SqlQueries.query1GroupByPeriodo` | `output/queries/query1_groupby_periodo.json` |
| Consulta con vista temporal + OrderBy + otros operadores de seleccion | `mdjm.SqlQueries.query2TopPrediosTempView` | `output/queries/query2_top_predios_2025.json` |
| Regresion Multilayer Perceptron (derivarlo, no esta en MLlib) | `mdjm.mlp.MLPRegressor` | `output/metrics/regression_metrics.json` (model=MLPRegressor) + `output/loss_curves/mlp_regressor_loss.json` |
| Regresion SVM (derivarlo, MLlib solo trae SVM de clasificacion) | `mdjm.svr.SVRegressor` | `output/metrics/regression_metrics.json` (model=SVRegressor) + `output/loss_curves/svr_regressor_loss.json` |
| Clasificacion Random Forest | `mdjm.classification.RFAndMLPClassification` (rf) | `output/metrics/classification_metrics.json` (model=RandomForest) |
| Clasificacion Multilayer Perceptron | `mdjm.classification.RFAndMLPClassification` (mlp) | `output/metrics/classification_metrics.json` (model=MLPClassifier) |
| Orquestacion / parametrizacion cluster vs monomodo | `mdjm.Main` (args: input, output, comando) + `Makefile` (`SPARK_MASTER`, `INPUT`, `OUTPUT`, `SPARK_HOME`) | - |

**Pendiente (fuera de alcance de esta parte):** levantar las VMs (1 monomodo
+ N multimodo) y correr `make run-all` en cada una para los tiempos reales de
ejecucion, speedup, uso de CPU/memoria (htop) — comparativa cluster vs
monomodo — e informe (LaTeX) y presentacion (beamer).
