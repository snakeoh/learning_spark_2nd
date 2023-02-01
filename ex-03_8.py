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

    # 컬럼 타입 변환
    fire_ts_df = (
        new_fire_df.withColumn(
            "IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")
        )
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn(
            "AvailableDtTS",
            to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"),
        )  # 24시간 표시로 변경됨
        # .withColumn( # 변환된 데이터도 am pm을 달고 싶은데 잘 안되네, 24시간 표시로 되었으니 변경은 조금만 찾으면 될듯
        #     "AvailableDtTSA",
        #     to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
        #     # .cast(
        #     #     TimestampType()), "MM-dd-yyyy hh:mm:ss a"
        #     )
    )

    (
        fire_ts_df.select(
            "IncidentDate",
            "OnWatchDate",
            "AvailableDtTm",
            "AvailableDtTS"  # ,
            # "AvailableDtTSA",
        ).show(10, False)
    )

    # 이제 date/time 연산이 가능함
    (
        fire_ts_df.select(year("IncidentDate"))
        .distinct()
        .orderBy(year("IncidentDate"))
        .show()
    )

    # 집계연산 aggregation

    # 가장 흔한 형태의 신고는?
    (
        fire_ts_df.select("CallType")
        .where(col("CallType").isNotNull())
        .groupBy("CallType")
        .count()
        .orderBy("count", ascending=False)
        .show(n=10, truncate=False)
    )

    # 통계함수
    import pyspark.sql.functions as F

    (
        fire_ts_df.select(
            F.sum("NumAlarms"),
            F.avg("ResponseDelayedinMins"),
            F.min("ResponseDelayedinMins"),
            F.max("ResponseDelayedinMins"),
        ).show()
    )

    # 예제 : 2018년에 왔던 신고 전화들의 모든 유형은 어떤 것이었는가?
    (
        fire_ts_df.select("CallType")
        .where(year("IncidentDate") == "2018")
        .distinct()
        .show()
    )

    # 예제 : 2018년에 신고 전화가 가장 많았던 달은 언제인가?
    (
        fire_ts_df.select(month("IncidentDate"))
        .where(year("IncidentDate") == "2018")
        .groupBy(
            "month(IncidentDate)"
        )  # month(IncidentDate) 이걸 컬럼이름으로 만들게됨. month("IncidentDate") 아님
        .count()
        .orderBy("count", ascending=False)
        .show()
    )

    # 예제 : 2018년에 가장 많은 신고가 들어온 샌프란시스코 지역은 어디인가?
    (
        fire_ts_df.select("Zipcode")
        # .where(year("IncidentDate") == "2002")  # 2018년에는 SF 지역이 없음...
        .where(year("IncidentDate") == "2018")  # 2018년에는 San Francisco 지역임...
        # .where(col("City") == "SF")
        .where(col("City") == "San Francisco")
        .groupBy("Zipcode")
        .count()
        .orderBy("count", ascending=False)
        .show()
    )

    # 예제 : 2018년에 가장 응답 시간이 늦었던 지역은 어디인가?
    (
        fire_ts_df.select("City", "ResponseDelayedinMins")
        .where(year("IncidentDate") == "2018")
        .groupBy("City")
        .max("ResponseDelayedinMins")
        .orderBy("max(ResponseDelayedinMins)", ascending=False)
        .show()
    )


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
