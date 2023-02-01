# learning_spark_2nd

> 현재 윈도우 실행파일만 있음. 리눅스는 추후.. ####실행법
> run-pyspark-ex.cmd 파일에서
> 실행할 python 파일의 이름을 수정한다.
> ex) ex-03_8.py

그리고 cmd에서

```
.\run-pyspark-ex.cmd <args>
```

#### 필요사항

1. spark 가 PC에 있어야 하며 run-pyspark-ex.cmd 의 SPARK_HOME을 설정
2. hadoop이 PC에 있거나, 클러스터의 hadoop conf의 hdfs-site.xml, yarn-site.xml, 등등의 파일 경로를 run-pyspark-ex.cmd 파일의 HADOOP_CONF_DIR 에 설정
3. run-pyspark-ex.cmd 의 PYSPARK_PYTHON 경로를 설정
