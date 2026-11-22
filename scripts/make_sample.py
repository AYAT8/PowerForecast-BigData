"""E1 - Construit data/echantillon.csv à partir du jeu 'Smart Meters in London' (Kaggle).
Exemple :
  python scripts/make_sample.py --halfhourly-dir /chemin/halfhourly_dataset \
      --info /chemin/informations_households.csv --n 200 --start 2012-10-01 --end 2012-12-31
Vérifiez les noms de colonnes de VOS fichiers (LCLid, tstp, energy(kWh/hh), Acorn_grouped)."""
import argparse, glob, pandas as pd

p = argparse.ArgumentParser()
p.add_argument("--halfhourly-dir", required=True)
p.add_argument("--info", required=True)
p.add_argument("--n", type=int, default=200)
p.add_argument("--start", default="2012-10-01")
p.add_argument("--end", default="2012-12-31")
p.add_argument("--out", default="data/echantillon.csv")
a = p.parse_args()

info = pd.read_csv(a.info)[["LCLid", "Acorn_grouped"]].rename(
    columns={"LCLid": "household_id", "Acorn_grouped": "quartier"})
ids = set(info["household_id"].sample(a.n, random_state=42))
parts = []
for f in sorted(glob.glob(f"{a.halfhourly_dir}/*.csv")):
    d = pd.read_csv(f)
    d = d[d["LCLid"].isin(ids)].rename(columns={"LCLid": "household_id", "tstp": "ts", "energy(kWh/hh)": "kwh"})
    d["kwh"] = pd.to_numeric(d["kwh"], errors="coerce")          # 'Null' -> NaN
    d["ts"] = pd.to_datetime(d["ts"])
    parts.append(d[(d["ts"] >= a.start) & (d["ts"] <= a.end)])
df = pd.concat(parts).dropna(subset=["kwh"]).merge(info, on="household_id")
df.sort_values("ts")[["household_id", "ts", "kwh", "quartier"]].to_csv(a.out, index=False)
print(len(df), "lignes ->", a.out)
