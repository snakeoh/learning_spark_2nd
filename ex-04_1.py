#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import substring


def main(spark):
    print("111111111111111111111111111")

    sc = spark

    csv_file = "data/departuredelays.txt"

    df = (
        spark.read.format("csv")
        .option("ingerSchema", "true")
        .option("header", "true")
        .load(csv_file)
    )

    schema = "'data' STRING, \
            'delay' INT, \
            'distance' INT, \
            'origin' STRING,\
            'destination' STRING"

    df.createOrReplaceTempView("us_delay_flight_tbl")

    spark.sql(
        """SELECT distance, origin, destination
    FROM us_delay_flight_tbl WHERE distance > 1000
    ORDER BY distance DESC"""
    ).show(10)

    spark.sql(
        """SELECT date, delay, origin, destination
    FROM us_delay_flight_tbl
    WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
    ORDER BY delay DESC"""
    ).show(10)

    spark.sql(
        """SELECT substring(date, 0, 2) as month, count(*), max(delay), mean(delay)
    FROM us_delay_flight_tbl
    GROUP BY month
    ORDER BY count(*) DESC"""
    ).show(10)

    spark.sql(
        """SELECT delay, origin, destination,
        CASE
            WHEN delay > 360 THEN 'Very Long Delays'
            WHEN delay >= 120 AND delay <= 360 THEN 'Long Delays'
            WHEN delay >= 60 AND delay < 120 THEN 'Short Delays'
            WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
            WHEN delay = 0 THEN 'No Delays'
            ELSE 'Early'
        END AS Flight_Delays
        FROM us_delay_flight_tbl
        ORDER BY origin, delay DESC"""
    ).show(10)

    spark.catalog.listDatabases()
    spark.catalog.listTables()
    # spark.catalog.listColumns("us_delay_flight_tbl")


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
