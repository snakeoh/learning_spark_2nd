#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main(spark):
    print("111111111111111111111111111")

    fire_schema = StructType(
        [
            StructField("CallNumber", IntegerType(), True),
            StructField("UnitID", StringType(), True),
            StructField("IncidentNumber", IntegerType(), True),
            StructField("CallType", StringType(), True),
            StructField("CallDate", StringType(), True),
            StructField("WatchDate", StringType(), True),
            StructField("CallFinalDisposition", StringType(), True),
            StructField("AvailableDtTm", StringType(), True),
            StructField("Address", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Zipcode", IntegerType(), True),
            StructField("Battalion", StringType(), True),
            StructField("StationArea", StringType(), True),
            StructField("Box", StringType(), True),
            StructField("OriginalPriority", StringType(), True),
            StructField("Priority", StringType(), True),
            StructField("FinalPriority", IntegerType(), True),
            StructField("ALSUnit", BooleanType(), True),
            StructField("CallTypeGroup", StringType(), True),
            StructField("NumAlarms", IntegerType(), True),
            StructField("UnitType", StringType(), True),
            StructField("UnitSequenceInCallDispatch", IntegerType(), True),
            StructField("FirePreventionDistrict", StringType(), True),
            StructField("SupervisorDistrict", StringType(), True),
            StructField("Neighborhood", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("Delay", FloatType(), True),
        ]
    )

    sf_fire_file = "data/sf-fire-calls.txt"
    fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

    # fire_df.show()

    few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
        col("CallType") != "Medical Incident"
    )
    few_fire_df.show(5, truncate=False)

    # 신고타입이 몇 종인지
    (
        fire_df.select("CallType")
        .where(col("CallType").isNotNull())
        .agg(countDistinct("CallType").alias("DistinctCallTypes"))
        .show()
    )

    # CallType 이 not null 인거중에 목록으로 보기
    (
        fire_df.select("CallType")
        .where(col("CallType").isNotNull())
        .distinct()
        .show(10, False)
    )

    # 컬럼 이름바꾸기 
    (fire_df.select("Delay").where(col("Delay") > 5).show(5, False))

    new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
    (
        new_fire_df.select("ResponseDelayedinMins")
        .where(col("ResponseDelayedinMins") > 5)
        .show(5, False)
    )

    fi


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
