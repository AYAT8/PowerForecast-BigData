"""E2 - Bronze -> Silver : parse JSON, types, nettoyage, dédoublonnage, UPSERT dans une table Hudi."""
from pyspark.sql.functions import from_json, to_timestamp, to_date
from common import get_spark, BRONZE_PATH, SILVER_PATH, HUDI_SILVER_OPTIONS

spark = get_spark("bronze_to_silver", cores=2, memory="1g")
schema = "household_id STRING, ts STRING, kwh DOUBLE, quartier STRING"

silver = (spark.read.parquet(BRONZE_PATH)
          .select(from_json("json", schema).alias("d"), "ingested_at").select("d.*", "ingested_at")
          .withColumn("ts", to_timestamp("ts"))
          .withColumn("date", to_date("ts"))
          .filter("household_id IS NOT NULL AND ts IS NOT NULL AND kwh IS NOT NULL AND kwh >= 0")
          .dropDuplicates(["household_id", "ts"]))

(silver.write.format("hudi").options(**HUDI_SILVER_OPTIONS).mode("append").save(SILVER_PATH))
print("Silver écrit :", silver.count(), "lignes")
