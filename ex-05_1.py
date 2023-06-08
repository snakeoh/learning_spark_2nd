#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.types import LongType
from pyspark.sql.types import *

import pandas as pd
from pyspark.sql.functions import col, pandas_udf


def main(spark):
    print("111111111111111111111111111")

    sc = spark

    def cubed(s):
        return s * s * s

    spark.udf.register("cubed", cubed, LongType())

    spark.range(1, 9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

    #####
    def cubed(a: pd.Series) -> pd.Series:
        return a * a * a

    cubed_udf = pandas_udf(cubed, returnType=LongType())

    x = pd.Series([1, 2, 3])
    print(cubed(x))

    df = spark.range(1, 4)
    df.select("id", cubed_udf(col("id"))).show()

    #####################
    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)
    t_c.createOrReplaceTempView("tc")

    t_c.show()

    spark.sql(
        """
    SELECT 
        celsius
        , transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrendeit
    From tC
    """
    ).show()

    spark.sql(
        """
    SELECT celsius,
        filter(celsius, t -> t > 38) AS high
        From tC
    """
    ).show()

    spark.sql(
        """
    SELECT celsius,
        exists(celsius, t -> t = 38) AS threshold
        FROM Tc
    """
    ).show()

    spark.sql(
        """
    SELECT celsius, 
        reduce(
            celsius,
            0,
            (t, acc) -> t + acc,
            acc -> (acc div size(celsius) * 9 div 5) + 32
        ) AS avgFahrenheit
    From tC
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
