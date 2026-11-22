"""E1 - Rejoue data/echantillon.csv dans Kafka comme un flux de relevés (ordre chronologique)."""
import json, os, time
import pandas as pd
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "releves_compteurs")
RATE = int(os.getenv("PRODUCER_RATE", "500"))          # messages par seconde
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode(),
    value_serializer=lambda v: json.dumps(v).encode())

df = pd.read_csv("/data/echantillon.csv").sort_values("ts")
df["ts"] = df["ts"].astype(str)
print(f"{len(df)} relevés à envoyer vers {TOPIC} ({RATE}/s)", flush=True)

for i, rec in enumerate(df.to_dict("records"), start=1):
    producer.send(TOPIC, key=str(rec["household_id"]), value=rec)   # clé = foyer -> ordre conservé
    if i % RATE == 0:
        producer.flush()
        time.sleep(1)
        print(f"{i} messages envoyés", flush=True)
producer.flush()
print("Terminé", flush=True)
