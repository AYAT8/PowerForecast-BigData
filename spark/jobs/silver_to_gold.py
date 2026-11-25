"""E3 - Silver -> Gold (Spark SQL) : consommation par foyer, quartier et jour. Copie vers PostgreSQL pour Superset."""
import sys
from common import get_spark, SILVER_PATH, GOLD_PATH, JDBC_URL, JDBC_PROPS

spark = get_spark("silver_to_gold", cores=2, memory="1g")


def arreter(message):
    # Code de sortie 1 pour que la tâche Airflow passe en échec
    print(f"ERREUR silver_to_gold : {message}", file=sys.stderr)
    spark.stop()
    sys.exit(1)


# Une table Hudi se reconnaît à son dossier .hoodie
hoodie = spark._jvm.org.apache.hadoop.fs.Path(SILVER_PATH.rstrip("/") + "/.hoodie")
if not hoodie.getFileSystem(spark._jsc.hadoopConfiguration()).exists(hoodie):
    arreter(f"table Silver introuvable à {SILVER_PATH} (lancer bronze_to_silver.py d'abord)")

silver = spark.read.format("hudi").load(SILVER_PATH)
if silver.isEmpty():
    arreter(f"table Silver vide à {SILVER_PATH}")
silver.createOrReplaceTempView("silver")

# Quartier manquant -> « inconnu » : le foyer reste dans Gold sans être mélangé à un vrai quartier
gold = spark.sql("""
    SELECT household_id, COALESCE(quartier, 'inconnu') AS quartier, date,
           ROUND(SUM(kwh), 3) AS conso_kwh,
           ROUND(MAX(kwh), 3) AS pic_demi_heure,
           CAST(COUNT(*) AS INT) AS nb_releves
    FROM silver
    GROUP BY household_id, COALESCE(quartier, 'inconnu'), date""")
gold.write.mode("overwrite").parquet(GOLD_PATH)

# On relit le Parquet au lieu de refaire l'agrégation (un cache() éclaterait la sortie en 200 petits fichiers)
gold = spark.read.parquet(GOLD_PATH)
(gold.write.mode("overwrite").option("truncate", "true")
 .jdbc(JDBC_URL, "gold_conso_foyer_jour", properties=JDBC_PROPS))
print("Gold écrit :", gold.count(), "lignes")
