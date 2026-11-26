"""E1 - Préparation des données Smart Meters in London."""

import argparse
import glob
import random
import os
import pandas as pd

# 1. Paramètres
p = argparse.ArgumentParser()

p.add_argument("--halfhourly-dir", required=True)
p.add_argument(
    "--n",
    type=int,
    default=None,
    help="Nombre de lignes pour un échantillon (facultatif)"
)
p.add_argument("--start", default="2012-10-01")
p.add_argument("--end", default="2012-12-31")
p.add_argument("--out", default="data/echantillon.csv")

a = p.parse_args()

random.seed(42)
CHUNK_SIZE = 50000
reservoir = []
total_valid = 0

# 2. Recherche des fichiers
files = sorted(glob.glob(f"{a.halfhourly_dir}/*.csv"))

if not files:
    raise FileNotFoundError("Aucun fichier CSV trouvé.")

print(f"{len(files)} fichiers CSV trouvés.")

# 3. Mapping des colonnes
columns_mapping = {
    "LCLid": "household_id",
    "DateTime": "ts",
    "KWH/hh (per half hour)": "kwh"
}

start_date = pd.to_datetime(a.start)
end_date = pd.to_datetime(a.end) + pd.Timedelta(days=1)

# Préparer le fichier de sortie
os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)

if a.n is None:
    if os.path.exists(a.out):
        os.remove(a.out)

# 4. Lecture par chunks
for file in files:

    print(f"Lecture : {file}")

    for chunk in pd.read_csv(file, chunksize=CHUNK_SIZE):

        chunk.columns = chunk.columns.str.strip()
        chunk = chunk.rename(columns=columns_mapping)

        required = ["household_id", "ts", "kwh"]

        if not all(col in chunk.columns for col in required):
            raise ValueError(f"Colonnes manquantes dans {file}")

        # Conversion des données
        chunk["kwh"] = pd.to_numeric(chunk["kwh"], errors="coerce")
        chunk["ts"] = pd.to_datetime(chunk["ts"], errors="coerce")

        # Filtrage de la période
        chunk = chunk[
            (chunk["ts"] >= start_date)
            & (chunk["ts"] < end_date)
        ]

        # Nettoyage
        chunk = chunk.dropna(
            subset=["household_id", "ts", "kwh"]
        )

        # Conservation des 3 colonnes utiles
        chunk = chunk[
            ["household_id", "ts", "kwh"]
        ]

        total_valid += len(chunk)

        # MODE COMPLET : conserver toutes les lignes
        if a.n is None:

            chunk.to_csv(
                a.out,
                mode="a",
                index=False,
                header=not os.path.exists(a.out)
            )

        # MODE ÉCHANTILLON : Reservoir Sampling
        else:

            for record in chunk.to_dict("records"):

                if len(reservoir) < a.n:
                    reservoir.append(record)

                else:
                    j = random.randint(1, total_valid)

                    if j <= a.n:
                        reservoir[j - 1] = record

    print("Fichier traité.")

# 5. Export de l'échantillon
if a.n is not None:

    if not reservoir:
        raise ValueError("Aucune donnée trouvée.")

    result = pd.DataFrame(reservoir)
    result = result.sort_values("ts")
    result.to_csv(a.out, index=False)

# 6. Résultat
print("----------------------------------")
print(f"Total de lignes valides : {total_valid}")

if a.n is None:
    print("Mode : données complètes")
    print(f"Fichier généré : {a.out}")
else:
    print("Mode : échantillon")
    print(f"Lignes échantillonnées : {len(reservoir)}")
    print(f"Fichier généré : {a.out}")