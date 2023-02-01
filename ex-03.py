#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext, Row
from pyspark.sql.functions import *


def main(spark):
    print("111111111111111111111111111")

    schema = "Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"

    data = [
        [
            1,
            "Jules",
            "Damji",
            "https://tinyurl.1",
            "1/4/2016",
            4535,
            ["twitter", "LinkedIn"],
        ],
        [
            2,
            "Brooke",
            "Wenig",
            "https://tinyurl.2",
            "5/5/2018",
            8908,
            ["twitter", "LinkedIn"],
        ],
        [
            3,
            "Denny",
            "Lee",
            "https://tinyurl.3",
            "6/7/2019",
            7659,
            ["web", "twitter", "FB", "LinkedIn"],
        ],
        [
            4,
            "Tathagata",
            "Das",
            "https://tinyurl.4",
            "5/12/2018",
            10568,
            ["twitter", "FB"],
        ],
        [
            5,
            "Matei",
            "Zaharia",
            "https://tinyurl.5",
            "5/14/2014",
            40578,
            ["web", "twitter", "FB", "LinkedIn"],
        ],
        [
            6,
            "Reynold",
            "Xin",
            "https://tinyurl.6",
            "3/2/2015",
            25568,
            ["twitter", "LinkedIn"],
        ],
    ]

    blogs_df = spark.createDataFrame(data, schema)
    # blogs_df = spark.createDataFrame(data)

    blogs_df.show()

    print(blogs_df.printSchema())

    blogs_df.columns

    # blogs_df.col("Id")

    blogs_df.select(expr("Hits * 2")).show(2)
    blogs_df.select(col("Hits") * 2).show(2)

    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

    blogs_df.withColumn(
        "AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))
    ).select(col("AuthorsId")).show(4)

    blogs_df.show()

    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(col("Hits")).show(2)
    blogs_df.select("Hits").show(2)

    # blogs_df.sort(col("Id").desc).show()
    # blogs_df.sort("Id".desc).show()

    blog_row = Row(
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    )
    print(blog_row[1])


if __name__ == "__main__":
    argument = sys.argv
    print("Argument : {}".format(argument))

    spark = (
        SparkSession.builder.appName("learning-spark ex")
        .enableHiveSupport()
        .getOrCreate()
    )

    # conf = SparkConf().setMaster("local").setAppName("learning-spark ex")

    # spark = SparkContext(conf=conf)

    main(spark)
