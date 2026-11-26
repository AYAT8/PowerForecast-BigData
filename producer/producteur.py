"""E1 - Envoie les relevés de consommation vers Kafka par blocs."""

import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


# =========================
# Configuration
# =========================

TOPIC = os.getenv("KAFKA_TOPIC", "releves_compteurs")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CSV_PATH = os.getenv("CSV_PATH", "/data/echantillon.csv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
PRODUCER_RATE = float(os.getenv("PRODUCER_RATE", "0"))

REQUIRED_COLUMNS = ["household_id", "ts", "kwh"]


# =========================
# Validation configuration
# =========================

if CHUNK_SIZE <= 0:
    raise ValueError("CHUNK_SIZE doit être un nombre positif.")

if PRODUCER_RATE < 0:
    raise ValueError("PRODUCER_RATE ne peut pas être négatif.")

if not os.path.isfile(CSV_PATH):
    raise FileNotFoundError(f"Fichier CSV introuvable : {CSV_PATH}")


# =========================
# Initialisation Kafka
# =========================

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5
)


# =========================
# Ingestion
# =========================

total_sent = 0
total_invalid = 0
start_time = time.perf_counter()
last_send_time = 0

try:
    print(f"Lecture du fichier : {CSV_PATH}", flush=True)
    print(f"Topic Kafka : {TOPIC}", flush=True)
    print(f"Taille des blocs : {CHUNK_SIZE}", flush=True)

    if PRODUCER_RATE > 0:
        print(
            f"Limitation de vitesse : {PRODUCER_RATE} messages/seconde",
            flush=True
        )
    else:
        print("Limitation de vitesse désactivée.", flush=True)

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):

        # -------------------------
        # Vérification des colonnes
        # -------------------------

        missing_columns = [
            col for col in REQUIRED_COLUMNS
            if col not in chunk.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Colonnes manquantes dans le CSV : {missing_columns}"
            )

        # -------------------------
        # Nettoyage des données
        # -------------------------

        initial_rows = len(chunk)

        chunk["kwh"] = pd.to_numeric(
            chunk["kwh"],
            errors="coerce"
        )

        chunk["ts"] = pd.to_datetime(
            chunk["ts"],
            errors="coerce"
        )

        chunk = chunk.dropna(subset=REQUIRED_COLUMNS)

        total_invalid += initial_rows - len(chunk)

        if chunk.empty:
            continue

        # -------------------------
        # Tri chronologique
        # -------------------------

        chunk = chunk.sort_values("ts")

        # Conversion des dates pour JSON
        chunk["ts"] = chunk["ts"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # -------------------------
        # Préparation des messages
        # -------------------------

        records = chunk.to_dict("records")
        futures = []

        # -------------------------
        # Envoi vers Kafka
        # -------------------------

        for record in records:

            # Contrôle du débit si activé
            if PRODUCER_RATE > 0:

                interval = 1.0 / PRODUCER_RATE
                elapsed = time.perf_counter() - last_send_time

                if elapsed < interval:
                    time.sleep(interval - elapsed)

            future = producer.send(
                TOPIC,
                key=str(record["household_id"]),
                value=record
            )

            futures.append(future)
            last_send_time = time.perf_counter()

        # -------------------------
        # Confirmation des envois
        # -------------------------

        producer.flush()

        for future in futures:
            future.get(timeout=30)

        total_sent += len(futures)

        print(
            f"{total_sent} messages envoyés | "
            f"{total_invalid} lignes invalides ignorées",
            flush=True
        )

    # -------------------------
    # Résultat final
    # -------------------------

    elapsed_time = time.perf_counter() - start_time

    print("\n========== INGESTION TERMINÉE ==========", flush=True)
    print(f"Messages envoyés : {total_sent}", flush=True)
    print(f"Lignes invalides ignorées : {total_invalid}", flush=True)
    print(f"Durée : {elapsed_time:.2f} secondes", flush=True)

    if elapsed_time > 0:
        throughput = total_sent / elapsed_time
        print(f"Débit moyen : {throughput:.2f} messages/seconde", flush=True)

finally:
    producer.close()
    print("Kafka Producer fermé.", flush=True)