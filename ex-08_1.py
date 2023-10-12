#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *


def main(spark):
    print("111111111111111111111111111")

    sc = spark

    lines = (
        spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", 23)
        .load()
    )

    words = lines.select(split(col("value"), "\\s").alias("word"))
    counts = words.groupby("word").count()
    checkpointDir = "./checkPoint"
    streamingQuery = (
        counts.writeStream.format("console")
        .outputMode("complete")
        .trigger(processingTime="1 second")
        .option("checkpointLocation", checkpointDir)
        .start()
    )
    streamingQuery.awaitTermination()

    # kafka. topic=events
    inputDF = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
        .option("subscribe", "events")
        .load()
    )

    # kafka produce
    counts = "..."  # DataFrame[word: string, count: long]
    streamingQuery = (
        counts.selectExpr(
            "cast(word as string) as key", "cast(count as string) as value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
        .option("topic", "wordCounts")
        .outputMode("update")
        .option("checkpointLocation", checkpointDir)
        .start()
    )

    # 스트리밍 데이터를 foreachBatch로 카산드라 2.4.2 이전 버전에 쓰기
    hostAddr = "ip"
    keyspaceName = "<keyspace>"
    tableName = "<tableName>"

    spark.conf.set("spark.cassandra.connection.host", hostAddr)

    def writeCountsToCassandra(updatedCountsDF, batchId):
        # 카산드라 배치 데이터 소스를 써서 업데이트된 숫자를 쓴다.
        (
            updatedCountsDF.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(table=tableName, keyspace=keyspaceName)
            .save()
        )

    streamingQuery = (
        counts.writeStream.foreachBatch(writeCountsToCassandra)
        .outputMode("update")
        .optioin("checkpointLocation", checkpointDir)
        .start()
    )

    # 여러곳에 쓰기. 캐싱-쓰기-캐시풀기
    def writeCountsToMultipleLocations(updatedCountsDF, batchId):
        updatedCountsDF.persist()
        updatedCountsDF.write.format(...).save()  # 위치 1
        updatedCountsDF.write.format(...).save()  # 위치 2
        updatedCountsDF.unpersist()

    # foreach
    # 변형 1: 함수 사용
    def process_row(row):
        # 이제 저장장치에 쓴다
        pass

    query = streamingDF.writeStream.foreach(process_row).start()

    # 변형 2: ForeachWriter 클래스 사용
    class ForeachWriter:
        def open(self, partitionId, epochId):
            # 데이터 저장소에 대한 접속을 열어놓는다.
            # 쓰기가 계속되어야 하면 true를 리턴한다.
            # 파이썬에서는 이 함수는 선택 사항이다.
            # 지정되어 있지 않다면 자동적으로 쓰기는 계속될 것이다.
            return True

        def process(self, row):
            # 열린 접속을 사용해서 저장소에 문자열을 쓴다.
            # 이 함수는 필수이다.
            pass

        def close(self, error):
            # 접속을 닫는다. 이 함수는 선택 사항이다.
            pass

    resultDF.writeStream.foreach(ForeachWriter()).start()


if __name__ == "__main__":
    argument = sys.argv
    print("Argument : {}".format(argument))

    spark = (
        SparkSession.builder.appName("learning-spark-2 ex")
        .enableHiveSupport()
        .getOrCreate()
    )

    # conf = SparkConf().setMaster("local").setAppName("learning-spark ex")

    # spark = SparkContext(conf=conf)

    main(spark)
