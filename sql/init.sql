-- Exécuté automatiquement au premier démarrage de PostgreSQL (E4)
CREATE DATABASE airflow;
CREATE DATABASE superset;
CREATE DATABASE conso;
\connect conso

-- Tables lues par Superset (alimentées par les jobs Spark, E3)
CREATE TABLE IF NOT EXISTS gold_conso_foyer_jour (
  household_id TEXT, quartier TEXT, date DATE,
  conso_kwh DOUBLE PRECISION, pic_demi_heure DOUBLE PRECISION, nb_releves INTEGER);
CREATE TABLE IF NOT EXISTS profils_foyers (household_id TEXT, profil INTEGER);
CREATE TABLE IF NOT EXISTS profils_courbes (profil INTEGER, heure INTEGER, kwh_moyen DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS previsions_conso (
  household_id TEXT, date DATE,
  conso_reelle DOUBLE PRECISION, conso_prevue DOUBLE PRECISION, conso_reference DOUBLE PRECISION);
CREATE TABLE IF NOT EXISTS metriques_modeles (
  date_calcul TIMESTAMP DEFAULT now(), modele TEXT, metrique TEXT, valeur DOUBLE PRECISION);
