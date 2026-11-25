#!/bin/bash
# Télécharge les JAR dans le dossier passé en argument (utilisé aussi par l'image Airflow).
# Versions alignées : Spark 3.5.3, Scala 2.12, Hudi 0.15.0, Hadoop 3.3.4.
# Ne pas en changer une seule, sinon ClassNotFoundException au lancement des jobs.
set -euo pipefail

DEST="$1"
M=https://repo1.maven.org/maven2

JARS=(
  # Hudi
  org/apache/hudi/hudi-spark3.5-bundle_2.12/0.15.0/hudi-spark3.5-bundle_2.12-0.15.0.jar

  # S3A pour lire/écrire dans MinIO
  org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
  com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

  # Connecteur Kafka et ses dépendances
  org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar
  org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar
  org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar
  org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

  # Pilote JDBC PostgreSQL
  org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
)

for p in "${JARS[@]}"; do
  fichier="$DEST/$(basename "$p")"
  echo "Téléchargement de $(basename "$p")"
  curl -fsSL -o "$fichier" "$M/$p"
  if [ ! -s "$fichier" ]; then
    echo "ERREUR : $fichier est vide (téléchargement échoué depuis $M/$p)" >&2
    exit 1
  fi
done

echo "Les ${#JARS[@]} JAR sont installés dans $DEST"
