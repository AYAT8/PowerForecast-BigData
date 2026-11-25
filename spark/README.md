# spark/

Image Spark du projet (utilisée par `spark-master`, `spark-worker` et `bronze-stream`) et jobs de traitement.

- `Dockerfile` : Spark 3.5.3, Python 3.10, avec les JAR Hudi, S3A, Kafka et PostgreSQL
- `download_jars.sh` : téléchargement des JAR (réutilisé par l'image Airflow)
- `jobs/common.py` : session Spark, chemins du Data Lake et options Hudi partagés par tous les jobs

## Lancer un job

PowerShell :

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/silver_to_gold.py
```

Git Bash (sinon le chemin `/opt/...` est converti en chemin Windows) :

```bash
MSYS_NO_PATHCONV=1 docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/silver_to_gold.py
```

Pour tester sans toucher aux vraies données, ajouter par exemple `-e SILVER_PATH=s3a://silver/releves_test` après `exec`.

## Variables d'environnement

| Variable | Rôle | Défaut |
|---|---|---|
| `SILVER_PATH` | table Hudi Silver | `s3a://silver/releves` |
| `GOLD_PATH` | Parquet Gold | `s3a://gold/conso_foyer_jour` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | accès MinIO (S3A) | `minioadmin` / `minioadmin123` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | accès PostgreSQL (JDBC) | `postgres` / `postgres` |
| `KAFKA_TOPIC` | topic des relevés | `releves_compteurs` |

Les identifiants viennent du `.env`. `SILVER_PATH` et `GOLD_PATH` ne servent que pour les tests.

## Erreurs fréquentes

- `ClassNotFoundException` : un JAR manque ou n'a pas la bonne version, voir `download_jars.sh`.
- `Initial job has not accepted any resources` : worker absent ou déjà occupé, vérifier `docker compose logs spark-master`.
- Erreur S3A (403, `NoSuchBucket`) : vérifier le `.env` et que `minio-init` a fini en `Exited (0)`.
- Erreur JDBC : PostgreSQL pas encore *healthy* ou table absente (`sql/init.sql`).
- `Python in worker has different version` : le driver et les workers doivent avoir la même version de Python (3.10).
