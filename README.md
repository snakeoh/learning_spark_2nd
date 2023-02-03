# learning_spark_2nd

> 현재 윈도우 실행파일만 있음. 리눅스는 추후..

####실행법
실행할 py 파일이 ex.py라고 한다면
cmd에서

```
.\run-pyspark-ex.cmd ex.py <args>
```

#### 필요사항

1. spark 가 PC에 있어야 하며 run-pyspark-ex.cmd 의 SPARK_HOME을 설정
2. hadoop이 PC에 있거나, 클러스터의 hadoop conf의 hdfs-site.xml, yarn-site.xml, 등등의 파일 경로를 run-pyspark-ex.cmd 파일의 HADOOP_CONF_DIR 에 설정
3. run-pyspark-ex.cmd 의 PYSPARK_PYTHON 경로를 설정
