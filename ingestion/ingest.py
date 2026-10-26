"""
Loads MetroPT-3 sensor data for one selected month into PostgreSQL.

I use PySpark to read the CSV, select the month and write the records to
`raw.sensor_readings`. Before loading, the script checks `raw.ingestion_log`
and skips a month if it is already listed there.

The data and the log are written separately. If a run stops between these
two steps, I need to check the database before trying that month again.

Example:
    python ingest.py --month 2020-02
"""
import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

JDBC_URL = os.environ.get(
    "JDBC_URL", "jdbc:postgresql://db:5432/metropt"
)
DB_USER = os.environ.get("DB_USER", "metropt")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "metropt")
DATA_DIR = os.environ.get("DATA_DIR", "/data/raw")

JDBC_PROPS = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

COLUMN_MAP = {
    "timestamp": "ts", "TP2": "tp2", "TP3": "tp3", "H1": "h1",
    "DV_pressure": "dv_pressure", "Reservoirs": "reservoirs",
    "Oil_temperature": "oil_temperature", "Motor_current": "motor_current",
    "COMP": "comp", "DV_eletric": "dv_eletric", "Towers": "towers",
    "MPG": "mpg", "LPS": "lps", "Pressure_switch": "pressure_switch",
    "Oil_level": "oil_level", "Caudal_impulses": "caudal_impulses",
}


JDBC_JAR_PATH = os.environ.get("JDBC_JAR_PATH", "/opt/jars/postgresql-42.7.3.jar")
# Sized for a 16GB-RAM laptop; tested locally on this hardware alongside
# Postgres and the dashboard. local[2] + 2g driver memory leaves headroom so
# the container stays well under its 3g mem_limit (see docker-compose.yml).
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")


def get_spark():
    builder = (
        SparkSession.builder
        .appName("MetroPT3-Ingestion")
        .master(SPARK_MASTER)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
    )
    if os.path.exists(JDBC_JAR_PATH):
        # If the PostgreSQL JDBC JAR is available in the image, use that
        # local copy instead of downloading it again when the job starts.
        builder = builder.config("spark.jars", JDBC_JAR_PATH)
    else:
        builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    return builder.getOrCreate()


def already_ingested(spark, month: str) -> bool:
    try:
        log_df = spark.read.jdbc(
            JDBC_URL, "raw.ingestion_log", properties=JDBC_PROPS
        )
    except Exception:
        # If the ingestion log cannot be read, this check returns False.
        # A database error may therefore look like a month that was not loaded.
        return False
    return log_df.filter(F.col("month") == month).count() > 0


def load_month(spark, month: str):
    csv_path = os.path.join(DATA_DIR, "MetroPT3(AirCompressor).csv")
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[ingest] Reading the CSV with Spark and filtering for month {month}...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    df = df.select(*COLUMN_MAP.keys())
    for old, new in COLUMN_MAP.items():
        df = df.withColumnRenamed(old, new)

    df = df.withColumn("ts", F.to_timestamp("ts"))
    df = df.filter(F.date_format("ts", "yyyy-MM") == month)
    df = df.withColumn("ingestion_batch", F.lit(month))

    row_count = df.count()
    if row_count == 0:
        print(f"[ingest] No records found for {month}.")
        return 0

    (
        df.write
        .mode("append")
        .jdbc(JDBC_URL, "raw.sensor_readings", properties=JDBC_PROPS)
    )

    log_row = spark.createDataFrame([(month, row_count)], ["month", "rows_loaded"])
    (
        log_row.write
        .mode("append")
        .jdbc(JDBC_URL, "raw.ingestion_log", properties=JDBC_PROPS)
    )

    print(f"[ingest] Saved {row_count} records for {month}.")
    return row_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="Month to load, for example 2020-02")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    if already_ingested(spark, args.month):
        print(f"[ingest] {args.month} is already in the ingestion log. Skipping this month.")
        spark.stop()
        return

    load_month(spark, args.month)
    spark.stop()


if __name__ == "__main__":
    main()
