# Descripcion de imagenes del informe

Este archivo describe las capturas usadas o disponibles para el informe. Las rutas son relativas a la carpeta `Informe/`, por lo que pueden usarse directamente en Overleaf o en Markdown.

## ImgConfi

| Imagen | Descripcion |
|---|---|
| `ImgConfi/etc_hosts_master_workers.png` | Captura del archivo `/etc/hosts` en el nodo master. Muestra la resolucion de nombres entre `master`, `slave-base` y `slave1`, incluyendo las IPs usadas para que los nodos del cluster se comuniquen por nombre. Sirve como evidencia de conectividad interna entre master y workers. |
| `ImgConfi/hadoop_workers_config.png` | Captura del archivo `workers` de Hadoop ubicado en `/home/hadoop/hadoop-3.3.0/etc/hadoop/workers`. Muestra los nodos declarados como workers del cluster: `localhost`, `slave-base` y `slave1`. Sirve para evidenciar que Hadoop conoce los nodos que deben participar en DFS/YARN. |
| `ImgConfi/hdfs_spark_event_logs.png` | Captura de `hdfs dfs -ls /spark-logs`. Muestra los event logs de Spark guardados en HDFS, incluyendo aplicaciones ejecutadas en YARN y ejecuciones locales. Sirve como evidencia de que Spark tiene habilitado el registro de eventos en HDFS. |
| `ImgConfi/spark_env_defaults_yarn_config.png` | Captura de `spark-env.sh` y `spark-defaults.conf`. Muestra variables como `HADOOP_HOME`, `HADOOP_CONF_DIR`, `YARN_CONF_DIR`, `SPARK_LOCAL_DIRS`, y parametros como `spark.master yarn`, `spark.eventLog.enabled true` y `spark.eventLog.dir hdfs:///spark-logs`. Sirve como prueba de que Spark esta configurado para trabajar sobre Hadoop/YARN. |
| `ImgConfi/yarn_nodes_showdetails_config.png` | Captura de `yarn node -list -showDetails`. Muestra los tres nodos del cluster en estado `RUNNING`, con recursos configurados de memoria y vcores. Sirve como evidencia de que el ResourceManager reconoce al master y a los dos workers. |

## ImgMultiNodo

| Imagen | Descripcion |
|---|---|
| `ImgMultiNodo/Imagen pegada.png` | Captura de `watch yarn node -list -showDetails` durante una ejecucion distribuida. Muestra contenedores activos en `slave1`, `slave-base` y `master`, junto con recursos asignados. Es una evidencia directa del uso simultaneo de los tres nodos. |
| `ImgMultiNodo/Nodes_02_yarn_nodos_cluster_activos.png` | Vista web de YARN en la seccion de nodos del cluster. Muestra `Active Nodes = 3` y lista `slave1`, `slave-base` y `master`, con memoria y vcores disponibles. Sirve para demostrar que el cluster multimodo esta activo. |
| `ImgMultiNodo/Query1_Resultado_Head.png` | Captura del resultado JSON de la consulta 1 en HDFS. Muestra los totales y promedios agrupados por `PERIODO`. Sirve como evidencia de la consulta Spark SQL con `groupBy` y funciones de agregacion. |
| `ImgMultiNodo/Query2_resultado_Head.png` | Captura del resultado JSON de la consulta 2 en HDFS. Muestra el top de predios exclusivos 2025 ordenados por `monto_total_neto`. Sirve como evidencia de la consulta con vista temporal, filtros y ordenamiento. |
| `ImgMultiNodo/SPARKUISQL.png` | Captura de Spark UI durante la ejecucion de consultas SQL. Muestra informacion de jobs, stages y ejecucion Spark asociada a la aplicacion. Sirve como evidencia de procesamiento Spark en el cluster. |
| `ImgMultiNodo/SparkUIETL.png` | Captura de Spark UI durante la etapa ETL. Muestra jobs completados para lectura del CSV, transformaciones y escritura de JSON. Sirve como evidencia de la ejecucion del ETL en Spark. |
| `ImgMultiNodo/VerificarCluster_Los3Activos.png` | Captura por terminal de `yarn node -list -showDetails`. Muestra `Total Nodes: 3` y los nodos `slave1`, `slave-base` y `master` en estado `RUNNING`. Sirve para comprobar que el cluster reconoce los tres nodos. |
| `ImgMultiNodo/cluster_01_yarn_aplicacion_spark_succeeded.png` | Captura de YARN UI con la aplicacion `ArbitriosMDJM` en estado `FINISHED` y `SUCCEEDED`. Sirve como evidencia de que el proyecto se ejecuto correctamente en el cluster. |
| `ImgMultiNodo/jsonContribuyenteHead.png` | Captura de algunas filas del JSON `contribuyentes.json`. Muestra la estructura generada para la tabla de contribuyentes. Sirve como muestra de la tabla JSON de salida. |
| `ImgMultiNodo/json_Contribuyente_emisiones.png` | Captura de HDFS listando las carpetas y archivos generados para `contribuyentes.json` y `emisiones.json`, incluyendo `_SUCCESS` y partes JSON. Sirve como evidencia de las dos tablas JSON pedidas. |
| `ImgMultiNodo/json_Emisiones_Head.png` | Captura de algunas filas del JSON `emisiones.json`. Muestra campos de predio, contribuyente, periodo, montos y descuentos. Sirve como muestra de la tabla de hechos generada por el ETL. |
| `ImgMultiNodo/json_query_1_2.png` | Captura de HDFS listando los directorios de salida de las consultas `query1_groupby_periodo.json` y `query2_top_predios_2025.json`. Sirve como evidencia de que ambas consultas generaron salidas JSON. |
| `ImgMultiNodo/yarn_node_showdetails_live.png` | Copia con nombre limpio de la captura `Imagen pegada.png`. Muestra el monitoreo en vivo de YARN con contenedores distribuidos entre los tres nodos. Es la version recomendada para usar en LaTeX por no tener espacios en el nombre. |

## ImgNod1

| Imagen | Descripcion |
|---|---|
| `ImgNod1/Capturas_Rercursos_3nodo_en_vivo.png` | Captura de `htop` en tres terminales simultaneas: master, `slave1` y `slave-base`. Muestra uso de CPU, memoria y procesos durante la ejecucion. Sirve como evidencia del monitoreo de recursos por nodo. |
| `ImgNod1/Curva_Perdida_MLP_Regre.png` | Captura de la curva de perdida del modelo `MLPRegressor`. Muestra la perdida por epoca y permite verificar que el entrenamiento del modelo de regresion se ejecuta y converge. |
| `ImgNod1/Curva_Perdida_svr_regresor.png` | Captura de la curva de perdida del modelo `SVRegressor`. Muestra la evolucion de la perdida epsilon-insensitiva durante el entrenamiento. Sirve como evidencia del segundo modelo de regresion solicitado. |
| `ImgNod1/ETL_SQL.png` | Captura de la ejecucion local/mononodo del ETL y las consultas SQL. Muestra conteos de filas y resultados de las consultas en terminal. Sirve como evidencia de la prueba en un solo nodo. |
| `ImgNod1/Imagen pegada.png` | Captura de monitoreo de recursos similar a `Capturas_Rercursos_3nodo_en_vivo.png`. Muestra `htop` en master y workers durante la ejecucion, util para evidenciar paralelismo y uso de CPU/memoria. |
| `ImgNod1/MetriRegres.png` | Captura de metricas de regresion. Muestra valores de RMSE, MSE, MAE y R2 para los modelos de regresion. Sirve como evidencia visual de la salida de metricas. |
| `ImgNod1/MetricasClasifi.png` | Captura de metricas de clasificacion. Muestra accuracy, precision, recall y F1-score para los modelos de clasificacion. Sirve como evidencia visual de la evaluacion de modelos. |
| `ImgNod1/Metricas_Regresion_Clasificacion.png` | Captura de metricas de regresion y clasificacion obtenidas desde HDFS en la ejecucion multinodo. Sirve como evidencia compacta de los resultados ML del cluster. |
| `ImgNod1/Regre_Clas.png` | Captura de la ejecucion local de regresion y clasificacion. Muestra metricas de modelos y tiempo total de ejecucion local. Sirve para comparar contra el cluster. |
| `ImgNod1/SvrRegresLoss.png` | Captura/imagen de la curva o archivo de perdida del modelo `SVRegressor`. Sirve como respaldo grafico de la perdida de entrenamiento para SVR. |
| `ImgNod1/eje_vivo_recurs.png` | Captura de recursos durante ejecucion, enfocada en el host/master. Muestra procesos y consumo de CPU/memoria mientras corre Spark o VirtualBox. Sirve como evidencia secundaria de monitoreo. |
| `ImgNod1/mlpRegressorLoss.png` | Captura/imagen de la curva o archivo de perdida del modelo `MLPRegressor`. Sirve como respaldo grafico de la perdida de entrenamiento para MLP de regresion. |
| `ImgNod1/query1json.png` | Captura del archivo JSON generado por la consulta 1. Sirve como muestra directa de la salida `query1_groupby_periodo.json`. |
| `ImgNod1/query2json.png` | Captura del archivo JSON generado por la consulta 2. Sirve como muestra directa de la salida `query2_top_predios_2025.json`. |
| `ImgNod1/sql1terminal.png` | Captura de la consulta SQL 1 mostrada en terminal. Presenta el resumen por periodo con totales y promedios. Sirve como evidencia clara del `groupBy` con funciones de agregacion. |
| `ImgNod1/sql2terminal.png` | Captura de la consulta SQL 2 mostrada en terminal. Presenta el top de predios exclusivos 2025 ordenados por monto neto. Sirve como evidencia clara de la consulta con vista temporal y `ORDER BY`. |
| `ImgNod1/table_contribuyente_parte0json.png` | Captura de una parte del JSON `contribuyentes.json`. Muestra registros de la tabla de contribuyentes generada por el ETL. |
| `ImgNod1/table_contribuyente_parte1json.png` | Captura adicional de otra parte del JSON `contribuyentes.json`. Sirve como muestra complementaria de la tabla de contribuyentes. |
| `ImgNod1/table_contribuyente_parte2json.png` | Captura adicional de otra parte del JSON `contribuyentes.json`. Sirve como evidencia de continuidad de los datos generados. |
| `ImgNod1/table_contribuyente_parte3json.png` | Captura adicional de otra parte del JSON `contribuyentes.json`. Sirve como respaldo visual de la tabla generada. |
| `ImgNod1/table_emisiones_parte0json.png` | Captura de una parte del JSON `emisiones.json`. Muestra registros de la tabla de hechos con predios, periodos, montos y descuentos. |
| `ImgNod1/table_emisiones_parte1json.png` | Captura adicional de otra parte del JSON `emisiones.json`. Sirve como muestra complementaria de la salida ETL. |
| `ImgNod1/table_emisiones_parte2json.png` | Captura adicional de otra parte del JSON `emisiones.json`. Sirve como evidencia de la tabla de emisiones generada. |
| `ImgNod1/table_emisiones_parte3json.png` | Captura adicional de otra parte del JSON `emisiones.json`. Sirve como respaldo visual de la tabla de hechos. |

## Imagenes mas recomendadas para el informe

Si se desea reducir el numero de capturas en el PDF, las mas importantes son:

1. `ImgConfi/etc_hosts_master_workers.png`
2. `ImgConfi/hadoop_workers_config.png`
3. `ImgConfi/spark_env_defaults_yarn_config.png`
4. `ImgMultiNodo/Nodes_02_yarn_nodos_cluster_activos.png`
5. `ImgMultiNodo/yarn_node_showdetails_live.png`
6. `ImgMultiNodo/cluster_01_yarn_aplicacion_spark_succeeded.png`
7. `ImgMultiNodo/json_Contribuyente_emisiones.png`
8. `ImgMultiNodo/json_query_1_2.png`
9. `ImgNod1/ETL_SQL.png`
10. `ImgNod1/Regre_Clas.png`
11. `ImgNod1/Capturas_Rercursos_3nodo_en_vivo.png`
12. `ImgNod1/Curva_Perdida_MLP_Regre.png`
13. `ImgNod1/Curva_Perdida_svr_regresor.png`
