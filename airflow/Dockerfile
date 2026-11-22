# E4 - Airflow + Java + pyspark (même version de Python que l'image Spark : 3.10)
FROM apache/airflow:2.9.3-python3.10
USER root
COPY spark/download_jars.sh /tmp/download_jars.sh
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.3 numpy
USER root
RUN bash /tmp/download_jars.sh "$(python -c 'import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),"jars"))')"
USER airflow
