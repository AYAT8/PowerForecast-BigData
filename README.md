# Prévision de la consommation électrique

Projet Big Data (module Big Data, filière ADIA) : **Sources → Kafka → Data Lake Bronze → Lakehouse Hudi Silver/Gold → Spark → Dashboard Superset**, orchestré par **Airflow**, déployé par **Docker Compose**.
Objectifs : prévoir la consommation (GBTRegressor) et regrouper les clients par profil (KMeans).

## Lancer toute la plateforme
```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```
Prévoir au moins **8 Go de RAM** alloués à Docker. Première construction : plusieurs minutes (téléchargement des JAR et images).

| Service | URL | Identifiants |
|---|---|---|
| MinIO (Data Lake) | http://localhost:9001 | `.env` |
| Spark Master | http://localhost:8080 (ou `SPARK_UI_PORT`) | — |
| Airflow | http://localhost:8081 | `.env` |
| Superset | http://localhost:8088 | `.env` |
| PostgreSQL | localhost:5432 (base `conso`) | `.env` |
| Kafka (depuis le PC) | localhost:29092 | — |

Port déjà utilisé sur votre PC (erreur « port is already allocated ») : pour Spark, mettez par exemple `SPARK_UI_PORT=8082` dans `.env`.

## Parcours d'une donnée
1. `producer` lit `data/echantillon.csv` et envoie les relevés dans le topic `releves_compteurs`.
2. `bronze-stream` (Spark Streaming) copie les messages bruts dans `s3a://bronze/releves/`.
3. Airflow (`conso_quotidien`) lance : `bronze_to_silver` (table Hudi `s3a://silver/releves`) → `quality_check` → `silver_to_gold` → `kmeans_profils` + `gbt_prevision`.
4. Les tables Gold, profils et prévisions sont copiées dans PostgreSQL (`conso`) ; Superset les affiche.

Lancer le DAG à la main : Airflow → `conso_quotidien` → ▶. Une démo complète : attendre que `producer` ait fini (`docker compose logs producer`), puis déclencher le DAG.

## Équipe
| Rôle | Responsabilité | Dossiers | Membre (GitHub) |
|---|---|---|---|
| E1 | Sources et ingestion Kafka | `producer/`, `data/`, `scripts/` | @… |
| E2 | Data Lake et Lakehouse Hudi | `spark/jobs/kafka_to_bronze.py`, `bronze_to_silver.py`, `quality_check.py` | @… |
| E3 | Traitements Spark (SQL, DataFrames, MLlib) | `spark/`, `silver_to_gold.py`, `kmeans_profils.py`, `gbt_prevision.py` | @… |
| E4 | Airflow, dashboard, intégration Docker | `airflow/`, `superset/`, `sql/`, `docker-compose.yml` | @… |

## Interfaces entre couches (à garder à jour)
Voir `docs/contrat_de_donnees.md`. Topic : `releves_compteurs` · Buckets : `bronze`, `silver`, `gold`, `checkpoints` · Silver : `s3a://silver/releves` · Gold : `s3a://gold/conso_foyer_jour` + tables PostgreSQL.

## Structure du dépôt
```
docker-compose.yml   plateforme complète (une seule commande)
producer/  data/  scripts/            E1
spark/ (Dockerfile, jobs/)            E2 + E3
airflow/  superset/  sql/             E4
docs/ (cahier des charges, rapport, présentation, architecture)
```

## Commandes utiles
```bash
docker compose up -d kafka minio        # démarrer seulement certains services
docker compose logs -f bronze-stream    # suivre un service
docker compose down                     # arrêter (les données restent)
docker compose down -v                  # tout effacer (repart de zéro)
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /jobs/silver_to_gold.py
```

## Problèmes fréquents
- Conteneur qui s'arrête (code 137) : manque de RAM → augmenter la mémoire Docker.
- `Connection refused` Kafka : dans Docker utiliser `kafka:9092`, depuis le PC `localhost:29092`.
- `ClassNotFoundException` : versions des JAR (voir `spark/download_jars.sh`).
- Les jobs Spark attendent des ressources : un seul job batch à la fois pendant le développement.
