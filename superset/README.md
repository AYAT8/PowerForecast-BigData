# Dashboard Superset (E4)
1. Ouvrir http://localhost:8088 (identifiants dans `.env`).
2. Settings → Database Connections → + Database → PostgreSQL, URI SQLAlchemy :
   `postgresql+psycopg2://postgres:postgres@postgres:5432/conso`
3. Créer un dataset par table : `gold_conso_foyer_jour`, `profils_foyers`, `profils_courbes`, `previsions_conso`, `metriques_modeles`.
4. Construire 3 pages : **Vue globale** (conso par jour/quartier), **Profils** (foyers par profil, courbe type sur 24 h), **Prévisions** (réel vs prévu, erreur).
5. Exporter le dashboard (menu ··· → Export) et déposer le fichier `.zip` dans ce dossier.
