"""E4 - DAG quotidien : Bronze -> Silver -> contrôle qualité -> Gold -> (KMeans, prévision).
Le flux Kafka -> Bronze tourne en continu dans le service `bronze-stream` (hors DAG)."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SUBMIT = "spark-submit --master spark://spark-master:7077 /jobs/"

with DAG(
    dag_id="conso_quotidien",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["conso-electrique"],
) as dag:
    silver = BashOperator(task_id="bronze_vers_silver", bash_command=SUBMIT + "bronze_to_silver.py")
    controle = BashOperator(task_id="controle_qualite", bash_command=SUBMIT + "quality_check.py")
    gold = BashOperator(task_id="agregats_gold", bash_command=SUBMIT + "silver_to_gold.py")
    kmeans = BashOperator(task_id="profils_kmeans", bash_command=SUBMIT + "kmeans_profils.py")
    prevision = BashOperator(task_id="prevision_gbt", bash_command=SUBMIT + "gbt_prevision.py")

    silver >> controle >> gold >> [kmeans, prevision]
