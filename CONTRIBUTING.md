# Règles de travail

- **Branches** : une par fonctionnalité, préfixée par le rôle (`e1/producteur-kafka`, `e3/kmeans`). Pas de commit direct sur `main`.
- **Pull requests** : chaque fusion est relue par un autre membre.
- **Comptes** : chacun commite avec **son propre compte** (la contribution individuelle est notée).
- **Secrets** : jamais de mot de passe dans Git, utiliser `.env`.
- **Interfaces** : toute modification d'un topic, bucket, table, colonne ou port est annoncée à l'équipe et reportée dans le README.
- **Commits** : `[E2] ajoute l'upsert Hudi sur la table Silver`.
- **docker-compose.yml** : modifié en binôme ou à tour de rôle pour éviter les conflits.
