"""E2 - Contrôle qualité de la zone Silver. Fait échouer la tâche Airflow si un contrôle échoue."""
import sys
from common import get_spark, SILVER_PATH

spark = get_spark("quality_check", cores=1, memory="512m")
df = spark.read.format("hudi").load(SILVER_PATH)
total = df.count()
erreurs = []
if total == 0:
    erreurs.append("Silver est vide")
if df.filter("kwh < 0 OR kwh IS NULL").count() > 0:
    erreurs.append("valeurs kwh négatives ou nulles")
if total - df.select("household_id", "ts").distinct().count() > 0:
    erreurs.append("doublons (household_id, ts)")
print(f"Lignes Silver : {total}")
if erreurs:
    print("ECHEC QUALITE :", "; ".join(erreurs)); sys.exit(1)
print("Contrôle qualité OK")
