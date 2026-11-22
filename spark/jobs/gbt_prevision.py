"""E3 - GBTRegressor : prévoit la consommation journalière d'un foyer. Comparée à une référence simple (J-7)."""
from pyspark.sql import Window
from pyspark.sql.functions import lag, dayofweek, month, col, current_timestamp, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from common import get_spark, GOLD_PATH, JDBC_URL, JDBC_PROPS

spark = get_spark("gbt_prevision", cores=2, memory="1g")
gold = spark.read.parquet(GOLD_PATH)

w = Window.partitionBy("household_id").orderBy("date")
d = (gold.withColumn("conso_j1", lag("conso_kwh", 1).over(w))
         .withColumn("conso_j7", lag("conso_kwh", 7).over(w))
         .withColumn("jour_sem", dayofweek("date"))
         .withColumn("mois", month("date"))
         .dropna(subset=["conso_j1", "conso_j7"]))

# Découpage PAR DATE (jamais au hasard : on n'entraîne pas avec le futur) : 80 % des jours pour l'entraînement
dates = sorted(r["date"] for r in d.select("date").distinct().collect())
if len(dates) < 5:
    raise SystemExit("Pas assez de jours pour entraîner le modèle")
coupure = dates[int(len(dates) * 0.8)]
feats = VectorAssembler(inputCols=["conso_j1", "conso_j7", "jour_sem", "mois"], outputCol="features")
train = feats.transform(d.filter(col("date") < lit(coupure)))
test = feats.transform(d.filter(col("date") >= lit(coupure)))

model = GBTRegressor(featuresCol="features", labelCol="conso_kwh", maxIter=50, seed=42).fit(train)
pred = model.transform(test).withColumnRenamed("prediction", "conso_prevue")

res = []
for nom, colonne in [("GBT", "conso_prevue"), ("reference_J-7", "conso_j7")]:
    for m in ("rmse", "mae"):
        v = RegressionEvaluator(labelCol="conso_kwh", predictionCol=colonne, metricName=m).evaluate(pred)
        print(f"{nom} {m} = {v:.4f}"); res.append((nom, m, float(v)))

(pred.select("household_id", "date", col("conso_kwh").alias("conso_reelle"), "conso_prevue",
             col("conso_j7").alias("conso_reference"))
     .write.mode("overwrite").option("truncate", "true").jdbc(JDBC_URL, "previsions_conso", properties=JDBC_PROPS))
(spark.createDataFrame(res, "modele STRING, metrique STRING, valeur DOUBLE")
      .withColumn("date_calcul", current_timestamp())
      .select("date_calcul", "modele", "metrique", "valeur")
      .write.mode("append").jdbc(JDBC_URL, "metriques_modeles", properties=JDBC_PROPS))
