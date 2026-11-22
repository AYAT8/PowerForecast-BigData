"""E3 - Silver -> Gold (Spark SQL) : consommation par foyer, quartier et jour. Copie vers PostgreSQL pour Superset."""
from common import get_spark, SILVER_PATH, GOLD_PATH, JDBC_URL, JDBC_PROPS

spark = get_spark("silver_to_gold", cores=2, memory="1g")
spark.read.format("hudi").load(SILVER_PATH).createOrReplaceTempView("silver")
gold = spark.sql("""
    SELECT household_id, quartier, date,
           ROUND(SUM(kwh), 3) AS conso_kwh,
           ROUND(MAX(kwh), 3) AS pic_demi_heure,
           CAST(COUNT(*) AS INT) AS nb_releves
    FROM silver
    GROUP BY household_id, quartier, date""")
gold.write.mode("overwrite").parquet(GOLD_PATH)
(gold.write.mode("overwrite").option("truncate", "true")
 .jdbc(JDBC_URL, "gold_conso_foyer_jour", properties=JDBC_PROPS))
print("Gold écrit :", gold.count(), "lignes")
