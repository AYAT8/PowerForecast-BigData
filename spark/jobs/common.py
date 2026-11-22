"""Configuration Spark partagée par tous les jobs (E2 + E3). Une seule source de vérité pour les chemins."""
import os
from pyspark.sql import SparkSession

# --- Chemins du Data Lake (voir README) ---
BRONZE_PATH = "s3a://bronze/releves/"
SILVER_PATH = "s3a://silver/releves"
GOLD_PATH = "s3a://gold/conso_foyer_jour"
CHECKPOINT_BRONZE = "s3a://checkpoints/bronze/"
JDBC_URL = "jdbc:postgresql://postgres:5432/conso"
JDBC_PROPS = {"user": os.getenv("POSTGRES_USER", "postgres"),
              "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
              "driver": "org.postgresql.Driver"}
TOPIC = os.getenv("KAFKA_TOPIC", "releves_compteurs")


def get_spark(app_name: str, cores: int = 2, memory: str = "1g") -> SparkSession:
    """cores/memory limités : le streaming et les jobs batch doivent se partager le worker."""
    return (SparkSession.builder.appName(app_name)
            .config("spark.cores.max", cores)
            .config("spark.executor.memory", memory)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate())


HUDI_SILVER_OPTIONS = {
    "hoodie.table.name": "silver_releves",
    "hoodie.datasource.write.recordkey.field": "household_id,ts",
    "hoodie.datasource.write.partitionpath.field": "date",
    "hoodie.datasource.write.precombine.field": "ingested_at",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}
