# MetroPT-3 Batch Processing Pipeline

DLMDSEDE02 – Data Engineering portfolio, Hamed Ziraksaz (matr. 14100098),
IU International University of Applied Sciences.

For Task 1, I developed a local batch pipeline with Docker Compose.
The project uses the MetroPT-3 compressor dataset,which contains 1,516,948
time-stamped sensor readings from February to September 2020 and four
recorded failure periods (Davari et al., 2021; see "Dataset source" below).
The pipeline loads monthly data into PostgreSQL, uses PySpark to calculate
hourly averages, and displays the results in a Streamlit dashboard.

## Changes after the conception phase

In my first design,I planned to use pandas for ingestion and
aggregation. The feedback on the conception phase asked for Spark to be
used in ingestion as well, so I changed both steps to PySpark.

The ingestion service now reads the CSV with Spark and writes the chosen
month to PostgreSQL through JDBC. The processing service uses Spark again
to read the raw table and calculate hourly averages. I kept pandas and
psycopg2 in the Streamlit dashboard because it only reads the much smaller
hourly result table.

## Architecture

```
MetroPT-3 CSV
     │
     ▼
Ingestion (Docker: Python + PySpark)  ──▶  raw.sensor_readings  (Postgres)
                                             raw.ingestion_log
     │
     ▼
Processing (Docker: Python + PySpark) ──▶  agg.fact_sensor_hourly (Postgres)
                                             agg.dim_failure_window
     │
     ▼
Delivery (Docker: Streamlit dashboard) ──▶  hourly sensor trend charts,
                                             hours flagged near a failure
```

Docker Compose defines separate containers for PostgreSQL, ingestion,
processing and the dashboard. The two Spark services run as one-off jobs.
The database tables are created by `db-init/init.sql` when PostgreSQL is
initialized for the first time.

## How to run the project

1. Download `MetroPT3(AirCompressor).csv` from the UCI Machine Learning
   Repository (https://archive.ics.uci.edu/dataset/791/metropt+3+dataset,
   DOI: 10.24432/C5VW3R) and put it in `data/raw/`. The CSV is not included
   in this repository because of its size.
2. Start PostgreSQL and the dashboard:
   ```
   docker compose up -d db delivery
   ```
3. Load the months you need. The script checks `raw.ingestion_log` and
   skips a month if it has already been recorded:
   ```
   docker compose run --rm ingestion --month 2020-02
   docker compose run --rm ingestion --month 2020-03
   ...
   ```
   If a run fails after saving data but before writing the log, check the
   database before retrying that month.
4. Run the PySpark job to calculate hourly averages:
   ```
   docker compose run --rm processing
   ```
5. Open the dashboard at http://localhost:8501

The Spark services currently use `local[2]` and 2 GB of driver memory.
Docker Compose sets a 3 GB memory limit for each Spark container. These
settings were tested locally on a 16GB-RAM laptop running Docker Desktop
alongside Postgres and the dashboard.

## Dataset source

MetroPT-3 is publicly available from the UCI Machine Learning Repository:
https://archive.ics.uci.edu/dataset/791/metropt+3+dataset
(DOI: 10.24432/C5VW3R).

Reference: Davari, N., Veloso, B., Ribeiro, R. P., Pereira, P. M. and
Gama, J. (2021). Predictive maintenance based on anomaly detection using
deep learning for air production unit in the railway industry. 2021 IEEE
8th International Conference on Data Science and Advanced Analytics (DSAA).
DOI: 10.1109/DSAA53316.2021.9564181

## Dataset check

One section of the dataset description gives the number of instances
as "15,169,480". The CSV provided for this project contains 1,516,948
sensor readings, so the larger figure appears to be a documentation error.
The data is sampled at about 0.1 Hz, or one record every ten seconds.

## Repository

https://github.com/HamedZiraksaz/Data_Engineering_DLMDSEDE02_14100098_MetroPT3_Pipeline
