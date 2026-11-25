"""Configuration Spark commune à tous les jobs (chemins, session, options Hudi)."""
import os
from pyspark.sql import SparkSession

# SILVER_PATH et GOLD_PATH peuvent être surchargés (docker compose exec -e ...)
# pour tester sur d'autres dossiers sans toucher aux vraies données.
BRONZE_PATH = "s3a://bronze/releves/"
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://silver/releves")
GOLD_PATH = os.getenv("GOLD_PATH", "s3a://gold/conso_foyer_jour")
CHECKPOINT_BRONZE = "s3a://checkpoints/bronze/"
JDBC_URL = "jdbc:postgresql://postgres:5432/conso"
JDBC_PROPS = {"user": os.getenv("POSTGRES_USER", "postgres"),
              "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
              "driver": "org.postgresql.Driver"}
TOPIC = os.getenv("KAFKA_TOPIC", "releves_compteurs")


def get_spark(app_name: str, cores: int = 2, memory: str = "1g") -> SparkSession:
    """Session Spark configurée pour Hudi et MinIO."""
    # Le worker n'a que 4 cœurs : on limite chaque job pour que le streaming
    # et un batch puissent tourner en même temps.
    return (SparkSession.builder.appName(app_name)
            .config("spark.cores.max", cores)
            .config("spark.executor.memory", memory)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # recommandé par Hudi
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
            # MinIO n'a pas de DNS par bucket : il faut des URL du type minio:9000/bucket/...
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())


# Un relevé = un foyer à un instant. En cas de doublon, l'upsert garde le plus récent (ingested_at).
HUDI_SILVER_OPTIONS = {
    "hoodie.table.name": "silver_releves",
    "hoodie.datasource.write.recordkey.field": "household_id,ts",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.precombine.field": "ingested_at",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",  # clé sur 2 colonnes
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}
