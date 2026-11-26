
"""E1 - Envoie les relevés de consommation vers Kafka par blocs."""

import json
import os

import pandas as pd
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC", "releves_compteurs")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CSV_PATH = os.getenv("CSV_PATH", "/data/echantillon.csv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))

REQUIRED_COLUMNS = ["household_id", "ts", "kwh"]

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

total_sent = 0

try:
    print(f"Lecture du fichier : {CSV_PATH}", flush=True)
    print(f"Topic Kafka : {TOPIC}", flush=True)

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):

        # Vérification des colonnes
        missing_columns = [
            col for col in REQUIRED_COLUMNS
            if col not in chunk.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Colonnes manquantes dans le CSV : {missing_columns}"
            )

        # Nettoyage des données
        chunk["kwh"] = pd.to_numeric(chunk["kwh"], errors="coerce")
        chunk = chunk.dropna(subset=REQUIRED_COLUMNS)

        if chunk.empty:
            continue

        # Tri du bloc par date
        chunk = chunk.sort_values("ts")

        # Préparation des messages
        records = chunk.to_dict("records")
        futures = []

        # Envoi vers Kafka
        for record in records:
            future = producer.send(
                TOPIC,
                key=str(record["household_id"]),
                value=record
            )
            futures.append(future)

        # Confirmation des envois du bloc
        producer.flush()

        for future in futures:
            future.get(timeout=30)

        total_sent += len(futures)

        print(
            f"{total_sent} messages envoyés",
            flush=True
        )

    print(
        f"Ingestion terminée : {total_sent} messages envoyés avec succès.",
        flush=True
    )

finally:
    producer.close()
