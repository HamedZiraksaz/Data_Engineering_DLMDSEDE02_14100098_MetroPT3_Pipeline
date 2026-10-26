"""
Calculates hourly sensor summaries for the MetroPT-3 batch pipeline.

This script reads the raw sensor data from PostgreSQL through JDBC. It uses
PySpark to calculate hourly averages for TP2, TP3, oil temperature and motor
current. It also marks hours that fall within the configured failure ranges
and saves the result in `agg.fact_sensor_hourly`.

The script recalculates the full hourly table each time it runs. It does not
update only the new records.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://db:5432/metropt")
DB_USER = os.environ.get("DB_USER", "metropt")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "metropt")

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# These periods come from the four failure events in the MetroPT-3 dataset.
# The start and end times below already include a six-hour margin.
FAILURE_WINDOWS = [
    ("2020-04-17 18:00:00", "2020-04-19 05:59:00"),
    ("2020-05-29 17:30:00", "2020-05-30 12:00:00"),
    ("2020-06-05 04:00:00", "2020-06-07 20:30:00"),
    ("2020-07-15 08:30:00", "2020-07-16 01:00:00"),
]


JDBC_JAR_PATH = os.environ.get("JDBC_JAR_PATH", "/opt/jars/postgresql-42.7.3.jar")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")


def get_spark():
    builder = (
        SparkSession.builder
        .appName("MetroPT3-Aggregation")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    )
    if os.path.exists(JDBC_JAR_PATH):
        builder = builder.config("spark.jars", JDBC_JAR_PATH)
    else:
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    return builder.getOrCreate()


def near_failure_condition():
    """Return a Spark condition for hours within the failure ranges.
    The six-hour margin is already included in FAILURE_WINDOWS."""
    cond = F.lit(False)
    for start, end in FAILURE_WINDOWS:
        cond = cond | ((F.col("hour_ts") >= F.lit(start)) & (F.col("hour_ts") <= F.lit(end)))
    return cond


def run_aggregation(spark):
    print("[aggregate] Reading sensor records from PostgreSQL...")
    df = spark.read.jdbc(JDBC_URL, "raw.sensor_readings", properties=JDBC_PROPS)

    if df.rdd.isEmpty():
        print("[aggregate] No raw sensor data found. Run ingestion first.")
        return

    df = df.withColumn("hour_ts", F.date_trunc("hour", F.col("ts")))

    agg = (
        df.groupBy("hour_ts")
        .agg(
            F.avg("tp2").alias("avg_tp2"),
            F.avg("tp3").alias("avg_tp3"),
            F.avg("oil_temperature").alias("avg_oil_temperature"),
            F.avg("motor_current").alias("avg_motor_current"),
            F.count("tp2").alias("reading_count"),
        )
        .withColumn("near_failure", near_failure_condition())
        .orderBy("hour_ts")
    )

    row_count = agg.count()
    print(f"[aggregate] Calculated {row_count} hourly rows. Saving the results...")

    # The hourly table is recalculated on each run, so replace its previous
    # contents with the new results.
    (
        agg.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(JDBC_URL, "agg.fact_sensor_hourly", properties=JDBC_PROPS)
    )
    print("[aggregate] Done.")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_aggregation(spark)
    spark.stop()


if __name__ == "__main__":
    main()
