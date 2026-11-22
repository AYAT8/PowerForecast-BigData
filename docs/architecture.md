# Architecture
Sources (CSV rejoué) → **Kafka** (`releves_compteurs`) → **Bronze** (MinIO, Parquet brut) → **Silver** (Hudi, nettoyé, upsert) → **Gold** (agrégats) → **Superset**.
Spark traite entre les zones ; **Airflow** orchestre le batch quotidien ; **Docker Compose** lance l'ensemble.

| Étape | Service Docker | Responsable |
|---|---|---|
| Sources / Kafka | kafka, kafka-init, producer | E1 |
| Data Lake / Hudi | minio, minio-init, bronze-stream, jobs bronze/silver/quality | E2 |
| Spark / ML | spark-master, spark-worker, jobs gold/kmeans/gbt | E3 |
| Orchestration / Dashboard | postgres, airflow, superset | E4 |

TODO (tous) : ajouter ici le schéma dessiné de l'architecture (image) pour le cahier des charges.
