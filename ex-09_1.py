#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
import stat
import shutil
from delta import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *


def main(spark, localDir):
    print("111111111111111111111111111")

    sc = spark

    # 소스 데이터 경로 설정
    sourcePath = "./data/loans/loan-risks.snappy.parquet"

    # 델타 레이크 경로 설정
    deltaPath = localDir

    # 같은 대출 데이터를 사용하여 델타 테이블 생성
    (
        spark.read.format("parquet")
        .load(sourcePath)
        .write.format("delta")
        .save(deltaPath)
    )

    # loans_delta라고 하는  데이터의 뷰를 생성
    (spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta"))

    spark.sql("select * from loans_delta limit 1").show()


if __name__ == "__main__":
    argument = sys.argv
    print("Argument : {}".format(argument))

    localDir = "./tmp/loans_delta/"

    # os.chmod("./tmp", 0o777)
    # os.chmod(localDir, 0o777)

    spark = (
        SparkSession.builder.appName("learning-spark-2 ex")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.local.dir", localDir)
        .enableHiveSupport()
        .getOrCreate()
    )
    # .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")

    # conf = SparkConf().setMaster("local").setAppName("learning-spark ex")

    # spark = SparkContext(conf=conf)

    #####################################################
    # my_packages = ["io.delta:delta-core_2.12:2.1.0"]

    # builder = (
    #     SparkSession.builder.appName("learning-spark-2 ex")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    # )
    # spark = configure_spark_with_delta_pip(builder).getOrCreate()
    #######################################################

    try:
        main(spark, localDir)
    finally:
        os.chmod(localDir, 0o777)
        shutil.rmtree(localDir + "*")
