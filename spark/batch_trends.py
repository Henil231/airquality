#!/usr/bin/env python3
"""Batch lane: roll the readings history into monthly trends per sensor."""
import argparse

from pyspark.sql import SparkSession, functions as F


def monthly_trends(readings):
    return (
        readings.withColumn("month", F.date_trunc("month", F.col("ts")))
        .groupBy("sensor_id", "pollutant", "month")
        .agg(
            F.avg("value").alias("avg_value"),
            F.max("value").alias("max_value"),
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("air-trends").getOrCreate()
    properties = {"driver": "org.postgresql.Driver"}

    readings = spark.read.jdbc(args.pg, "readings", properties=properties)
    trends = monthly_trends(readings)
    trends.write.jdbc(args.pg, "trends", mode="overwrite", properties=properties)
    print(f"wrote {trends.count()} trend rows")


if __name__ == "__main__":
    main()
