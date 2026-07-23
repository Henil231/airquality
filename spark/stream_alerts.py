#!/usr/bin/env python3
"""Real time lane: read readings from Kafka, flag EPA AQI breaches, store alerts."""
import argparse
import os

# EPA PM2.5 AQI breakpoints: (C_lo, C_hi, AQI_lo, AQI_hi, category).
PM25_BREAKPOINTS = [
    (0.0, 12.0, 0, 50, "Good"),
    (12.1, 35.4, 51, 100, "Moderate"),
    (35.5, 55.4, 101, 150, "Unhealthy for Sensitive Groups"),
    (55.5, 150.4, 151, 200, "Unhealthy"),
    (150.5, 250.4, 201, 300, "Very Unhealthy"),
    (250.5, 350.4, 301, 400, "Hazardous"),
    (350.5, 500.4, 401, 500, "Hazardous"),
]

# Alert at Unhealthy for Sensitive Groups and above.
ALERT_AQI_DEFAULT = 101

# A sensor repeating the same AQI within this window does not alert again.
DEDUP_WINDOW_DEFAULT = "1 hour"


def pm25_aqi(value):
    """Return (AQI, category) for a PM2.5 concentration in ug/m3, or None if invalid."""
    if value is None or value < 0:
        return None
    c = float(value)
    for c_lo, c_hi, aqi_lo, aqi_hi, category in PM25_BREAKPOINTS:
        if c <= c_hi:
            aqi = (aqi_hi - aqi_lo) / (c_hi - c_lo) * (c - c_lo) + aqi_lo
            return round(aqi), category
    # Above the top breakpoint: cap at the most hazardous level.
    _, _, _, aqi_hi, category = PM25_BREAKPOINTS[-1]
    return aqi_hi, category


def parse(stream):
    from pyspark.sql import functions as F, types as T
    schema = T.StructType([
        T.StructField("source", T.StringType()),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("pollutant", T.StringType()),
        T.StructField("value", T.DoubleType()),
        T.StructField("unit", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("ts", T.DoubleType()),
    ])
    return stream.select(
        F.from_json(F.col("value").cast("string"), schema).alias("d")
    ).select("d.*")


def flag_breaches(readings, alert_aqi=ALERT_AQI_DEFAULT, dedup_window=DEDUP_WINDOW_DEFAULT):
    # Single home for the alert decision. AQI math lives in pm25_aqi.
    # A sensor repeating its last reading within the window is not a new
    # breach, so it is dropped rather than re-alerted.
    from pyspark.sql import functions as F, types as T
    aqi_type = T.StructType([
        T.StructField("aqi", T.IntegerType()),
        T.StructField("category", T.StringType()),
    ])
    aqi_udf = F.udf(pm25_aqi, aqi_type)
    return (
        readings
        .filter(F.col("pollutant") == "pm25")
        .withColumn("aqi", aqi_udf(F.col("value")))
        .filter(F.col("aqi.aqi") >= alert_aqi)
        .select(
            "source", "sensor_id", "pollutant",
            F.col("aqi.aqi").cast("double").alias("value"),
            F.col("aqi.category").alias("level"),
            "lat", "lon",
            F.to_timestamp(F.col("ts")).alias("ts"),
        )
        .withWatermark("ts", dedup_window)
        .dropDuplicatesWithinWatermark(["sensor_id", "value"])
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="readings")
    parser.add_argument("--pg", required=True)
    parser.add_argument("--alert-aqi", type=int,
                        default=int(os.getenv("ALERT_AQI", ALERT_AQI_DEFAULT)))
    parser.add_argument("--dedup-window", default=os.getenv("DEDUP_WINDOW", DEDUP_WINDOW_DEFAULT),
                        help="skip re-alerting a sensor's unchanged AQI within this window")
    parser.add_argument("--once", action="store_true",
                        help="drain the topic once and exit, for tests and evidence runs")
    parser.add_argument("--from-start", action="store_true",
                        help="read from the earliest offset, for a live demo on a fresh topic")
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("air-alerts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest" if (args.once or args.from_start) else "latest")
        .load()
    )
    alerts = flag_breaches(parse(stream), args.alert_aqi, args.dedup_window)

    def to_postgres(batch, _epoch_id):
        (
            batch.write.format("jdbc")
            .option("url", args.pg)
            .option("dbtable", "alerts")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )

    writer = alerts.writeStream.foreachBatch(to_postgres).outputMode("append")
    if args.once:
        writer = writer.trigger(availableNow=True)
    writer.start().awaitTermination()


def demo():
    """Assert known EPA PM2.5 AQI values. Runs without pyspark."""
    cases = [
        (0.0, (0, "Good")),
        (6.0, (25, "Good")),
        (12.0, (50, "Good")),
        (12.1, (51, "Moderate")),
        (35.4, (100, "Moderate")),
        (35.5, (101, "Unhealthy for Sensitive Groups")),
        (55.5, (151, "Unhealthy")),
        (150.5, (201, "Very Unhealthy")),
        (250.5, (301, "Hazardous")),
        (600.0, (500, "Hazardous")),
    ]
    for value, expected in cases:
        got = pm25_aqi(value)
        assert got == expected, f"pm25_aqi({value}) = {got}, expected {expected}"
    assert pm25_aqi(None) is None
    assert pm25_aqi(-1.0) is None
    assert pm25_aqi(35.4)[0] < ALERT_AQI_DEFAULT <= pm25_aqi(35.5)[0]
    print("self check passed:", len(cases), "AQI cases, alert cutoff", ALERT_AQI_DEFAULT)


if __name__ == "__main__":
    import sys
    if "--selfcheck" in sys.argv:
        demo()
    else:
        main()
