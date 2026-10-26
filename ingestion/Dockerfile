FROM python:3.11-slim

# PySpark needs Java to run, so this image includes a JVM.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download the PostgreSQL JDBC driver when building the image.
# Spark can then use the local JAR at runtime.
RUN mkdir -p /opt/jars && \
    curl -sSfL -o /opt/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY ingest.py .

ENTRYPOINT ["python", "ingest.py"]
