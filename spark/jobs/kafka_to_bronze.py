"""E2 - Streaming : topic Kafka -> zone Bronze (données brutes, jamais modifiées)."""
from common import get_spark, BRONZE_PATH, CHECKPOINT_BRONZE, TOPIC

spark = get_spark("kafka_to_bronze", cores=1, memory="512m")
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest").load())
bronze = raw.selectExpr("CAST(key AS STRING) AS k", "CAST(value AS STRING) AS json",
                        "timestamp AS ingested_at")
(bronze.writeStream.format("parquet")
 .option("path", BRONZE_PATH)
 .option("checkpointLocation", CHECKPOINT_BRONZE)     # le « marque-page » : ne pas le changer
 .trigger(processingTime="30 seconds")
 .start().awaitTermination())
