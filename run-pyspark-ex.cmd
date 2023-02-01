@echo off

set SPARK_HOME=E:\Dtonic\study\spark\spark-3.0.2-bin-hadoop2.7
set HADOOP_CONF_DIR=E:\Dtonic\study\spark\hadoop-2.7.4\etc\hadoop
set PYTHONIOENCODING=utf8
set PYTHONPATH=%SPARK_HOME%\python
set PATH=%PATH%;%SPARK_HOME%\bin
set SPARK_SUBMIT_OPTIONS=^
    --master local ^
    --driver-memory 1G ^
    --executor-memory 2G ^
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    
    
    @REM --master yarn \
    @REM  --conf spark.ui.port=9040 \
    @REM  --conf spark.kryo.registrator=org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

set PYSPARK_PYTHON=C:\Users\damon\AppData\Local\Programs\Python\Python38-32\python

set CMD=spark-submit %SPARK_SUBMIT_OPTIONS% ^
        ex-03_8.py ^
        %*

echo %CMD%

@REM $CMD 2>&1 | tee Analysis-contact-tracing.out

%CMD%