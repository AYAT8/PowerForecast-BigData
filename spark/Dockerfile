# E3 - Image Spark 3.5.3 (Python 3.10) avec Hudi, S3A (MinIO), Kafka et JDBC PostgreSQL
FROM apache/spark:3.5.3-python3
USER root
COPY spark/download_jars.sh /tmp/download_jars.sh
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && bash /tmp/download_jars.sh /opt/spark/jars \
    && pip3 install --no-cache-dir numpy
USER spark
