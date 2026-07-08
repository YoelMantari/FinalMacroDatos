SPARK_HOME ?= /opt/spark
SPARK_SUBMIT := $(SPARK_HOME)/bin/spark-submit
SCALA_CP := $(SPARK_HOME)/jars/*
SOURCES := $(shell find src/main/scala -name '*.scala')

SPARK_MASTER ?= local[*]
INPUT ?= file://$(CURDIR)/data/dataset_vf_3.csv
OUTPUT ?= file://$(CURDIR)/output
JAR := target/arbitrios-mdjm.jar

.PHONY: build hdfs-start run-etl run-sql run-regression run-classification run-all clean

# Compila con el scalac que ya trae Spark (SPARK_HOME/jars) y empaqueta con
# el `jar` del JDK. No usa Maven/sbt: nada que instalar ni Internet que
# necesitar en la VM del cluster, mas alla de tener Spark instalado.
build:
	mkdir -p target/classes
	java -cp "$(SCALA_CP)" scala.tools.nsc.Main -classpath "$(SCALA_CP)" -d target/classes $(SOURCES)
	jar cfe $(JAR) mdjm.Main -C target/classes .

# Copia el dataset a HDFS pseudo-distribuido local (namenode/datanode deben
# estar corriendo: start-dfs.sh). No usado si SPARK_MASTER/INPUT apuntan a file://
hdfs-start:
	hdfs dfs -mkdir -p /mdjm/input
	hdfs dfs -put -f data/dataset_vf_3.csv /mdjm/input/dataset_vf_3.csv

run-etl:
	$(SPARK_SUBMIT) --class mdjm.Main --master $(SPARK_MASTER) $(JAR) $(INPUT) $(OUTPUT) etl

run-sql:
	$(SPARK_SUBMIT) --class mdjm.Main --master $(SPARK_MASTER) $(JAR) $(INPUT) $(OUTPUT) sql

run-regression:
	$(SPARK_SUBMIT) --class mdjm.Main --master $(SPARK_MASTER) $(JAR) $(INPUT) $(OUTPUT) regression

run-classification:
	$(SPARK_SUBMIT) --class mdjm.Main --master $(SPARK_MASTER) $(JAR) $(INPUT) $(OUTPUT) classification

run-all:
	$(SPARK_SUBMIT) --class mdjm.Main --master $(SPARK_MASTER) $(JAR) $(INPUT) $(OUTPUT) all

clean:
	rm -rf target output/tables output/queries output/metrics output/loss_curves
	mkdir -p output/tables output/queries output/metrics output/loss_curves
