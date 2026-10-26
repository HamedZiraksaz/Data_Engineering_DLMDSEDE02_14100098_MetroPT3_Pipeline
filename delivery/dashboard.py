"""
Shows hourly MetroPT-3 sensor averages in a Streamlit dashboard.

The dashboard reads `agg.fact_sensor_hourly` from PostgreSQL and plots the
selected sensor over time. It also lists the hours marked by the processing
step as close to the documented failure events.

I kept pandas and psycopg2 in this service because it reads the smaller,
already-aggregated table. The larger data processing steps use PySpark.
"""
import os

import pandas as pd
import psycopg2
import streamlit as st

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "metropt")
DB_USER = os.environ.get("DB_USER", "metropt")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "metropt")


@st.cache_data(ttl=60)
def load_hourly():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        df = pd.read_sql(
            "SELECT * FROM agg.fact_sensor_hourly ORDER BY hour_ts;", conn
        )
    finally:
        conn.close()
    return df


@st.cache_data(ttl=60)
def load_failure_windows():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    try:
        df = pd.read_sql("SELECT * FROM agg.dim_failure_window ORDER BY start_ts;", conn)
    finally:
        conn.close()
    return df


st.set_page_config(page_title="MetroPT-3 Compressor Monitoring", layout="wide")
st.title("MetroPT-3: Hourly Sensor Averages")
st.caption(
    "Data path: CSV -> PySpark ingestion -> PostgreSQL (raw) -> "
    "PySpark aggregation -> PostgreSQL (agg) -> dashboard."
)

try:
    hourly = load_hourly()
    windows = load_failure_windows()
except Exception as e:
    st.error(f"Could not load data from PostgreSQL: {e}")
    st.stop()

if hourly.empty:
    st.warning("The hourly results table is empty. Run ingestion and aggregation first.")
    st.stop()

hourly["hour_ts"] = pd.to_datetime(hourly["hour_ts"])

st.subheader("Documented failure events")
st.dataframe(windows, use_container_width=True)

sensor = st.selectbox(
    "Sensor",
    ["avg_tp2", "avg_tp3", "avg_oil_temperature", "avg_motor_current"],
    format_func=lambda c: c.replace("avg_", "").replace("_", " ").title(),
)

st.subheader(f"{sensor.replace('avg_', '').replace('_', ' ').title()} by hour")
st.line_chart(hourly.set_index("hour_ts")[[sensor]])

near_failure = hourly[hourly["near_failure"]]
st.subheader(f"Hours within the marked failure ranges ({len(near_failure)} of {len(hourly)})")
st.dataframe(
    near_failure[["hour_ts", sensor, "reading_count"]].reset_index(drop=True),
    use_container_width=True,
)
