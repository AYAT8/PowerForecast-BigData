"""E3 - KMeans : regroupe les foyers par profil horaire de consommation (24 valeurs par foyer)."""
from pyspark.sql.functions import hour, avg
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from common import get_spark, SILVER_PATH, JDBC_URL, JDBC_PROPS

K = 4        # nombre de profils : à choisir avec le score silhouette (voir boucle ci-dessous)
spark = get_spark("kmeans_profils", cores=2, memory="1g")
silver = spark.read.format("hudi").load(SILVER_PATH)

heures = [str(h) for h in range(24)]
profil = (silver.withColumn("h", hour("ts")).groupBy("household_id")
          .pivot("h", list(range(24))).agg(avg("kwh")).na.fill(0))
vec = VectorAssembler(inputCols=heures, outputCol="raw").transform(profil)
data = StandardScaler(inputCol="raw", outputCol="features", withMean=True).fit(vec).transform(vec)

for k in range(2, 8):                                   # TODO E3 : noter les scores dans le rapport
    sc = ClusteringEvaluator(featuresCol="features").evaluate(
        KMeans(k=k, seed=42, featuresCol="features").fit(data).transform(data))
    print(f"k={k} silhouette={sc:.3f}")

pred = KMeans(k=K, seed=42, featuresCol="features", predictionCol="profil").fit(data).transform(data)
pred.select("household_id", "profil").write.mode("overwrite").option("truncate", "true") \
    .jdbc(JDBC_URL, "profils_foyers", properties=JDBC_PROPS)

# courbe moyenne de chaque profil (pour nommer les groupes : « pic du soir », etc.)
courbes = pred.select("profil", *[f"`{h}`" for h in heures])
rows = []
for h in heures:
    rows.append(courbes.groupBy("profil").agg(avg(f"`{h}`").alias("kwh_moyen"))
                .selectExpr("profil", f"{int(h)} AS heure", "kwh_moyen"))
from functools import reduce
reduce(lambda a, b: a.unionByName(b), rows).write.mode("overwrite").option("truncate", "true") \
    .jdbc(JDBC_URL, "profils_courbes", properties=JDBC_PROPS)
