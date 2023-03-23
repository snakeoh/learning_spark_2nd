#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.ml import image


def main(spark):
    print("111111111111111111111111111")

    sc = spark

    file = "../LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet/"
    # df = spark.read.format("parquet").load(file)

    # df.show()

    # hive 테이블로 만들고 읽기
    # spark.sql(
    #     """CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
    #                 USING parquet
    #                 OPTIONS (
    #                     path "E:/Dtonic/study/spark/LearningSparkV2-master/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"
    #                 )"""
    # )

    # spark.sql("SELECT * FROM us_delay_flights_tbl").show()

    

    image_dir = "../LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
    images_df = spark.read.format("image").load(image_dir)
    images_df.printSchema()

    path = "../LearningSparkV2-master/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
    binary_files_df = (spark.read.format("binaryFile")
                        .option("pathGlobFilter", "*.jpg")
                        .load(path)
                        )
    binary_files_df.show()

    binary_files_df = (spark.read.format("binaryFile")
                        .option("pathGlobFilter", "*.jpg")
                        .option("recursiveFileLookup", "true")
                        .load(path)
                        )
    binary_files_df.show()


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
