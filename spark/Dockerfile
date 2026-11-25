# Image Spark 3.5.3 avec Hudi, S3A (MinIO), Kafka et JDBC PostgreSQL

# Variante java17 : elle embarque Python 3.10, comme l'image Airflow.
# Avec la variante python3 (Python 3.8), les jobs lancés par Airflow plantent.
FROM apache/spark:3.5.3-java17-python3
USER root
COPY spark/download_jars.sh /tmp/download_jars.sh
# curl pour récupérer les JAR, numpy pour MLlib
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && bash /tmp/download_jars.sh /opt/spark/jars \
    && pip3 install --no-cache-dir numpy
USER spark
