#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import expr


def main(spark):
    print("111111111111111111111111111")

    sc = spark

    tripdelaysFilePath = "data/flight/departuredelays.txt"
    airportsnaFilePath = "data/flight/airport-codes-na.txt"

    airportsna = (
        spark.read.format("csv")
        .options(header="true", inferSchema="true", sep="\t")
        .load(airportsnaFilePath)
    )

    airportsna.createOrReplaceTempView("airports_na")

    departureDelays = (
        spark.read.format("csv").options(header="true").load(tripdelaysFilePath)
    )

    departureDelays = departureDelays.withColumn(
        "delay", expr("CAST(delay as INT) as delay")
    ).withColumn("distance", expr("CAST(distance as INT) as distance"))

    departureDelays.createOrReplaceTempView("departureDelays")

    foo = departureDelays.filter(
        expr(
            """origin == 'SEA' AND destination == 'SFO' and
        date like '01010%' and delay > 0"""
        )
    )
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * from airports_na LIMIT 10").show()

    spark.sql("SELECT * from departureDelays LIMIT 10").show()

    spark.sql("SELECT * from foo").show()

    # Uniion
    bar = departureDelays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(
        expr(
            """origin == 'SEA' AND destination == 'SFO'
        AND date LIKE '01010%' AND delay > 0 """
        )
    ).show()

    # Join
    foo.join(airportsna, airportsna.IATA == foo.origin).select(
        "City", "State", "date", "delay", "distance", "destination"
    ).show()

    # 윈도우
    spark.sql(
        """
        DROP TABLE IF EXISTS departureDelaysWindow;
        """
    )

    spark.sql(
        """
        CREATE TABLE departureDelaysWindow AS
        SELECT origin, destination, SUM(delay) AS TotalDelays
            FROM departureDelays
        WHERE origin IN ('SEA', 'SFO', 'JFK')
            AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
        GROUP BY origin, destination;
        """
    )
    spark.sql(
        """
        SELECT * FROM departureDelaysWindow;
    """
    ).show()

    spark.sql(
        """
        SELECT origin, destination, SUM(TotalDelays) AS TotalDelays
        FROM departureDelaysWindow
        WHERE origin = 'SEA'
        GROUP BY origin, destination
        ORDER BY SUM(TotalDelays) DESC
        LIMIT 3
    """
    ).show()

    spark.sql(
        """
        SELECT origin, destination, TotalDelays, rank
            FROM(
                SELECT origin, destination, TotalDelays,
                dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank
                FROM departureDelaysWindow
            ) t
        WHERE rank <= 3
    """
    ).show()

    # 수정
    print("22222222222222222222222222222222222222222")

    foo.show()

    foo2 = foo.withColumn(
        "status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )
    foo2.show()

    foo3 = foo2.drop("delay")
    foo3.show()

    foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    # 피벗
    spark.sql(
        """
        SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
        FROM departureDelays
        WHERE origin = 'SEA'
    """
    ).show()


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
