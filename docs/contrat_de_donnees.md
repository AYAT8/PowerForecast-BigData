# Contrat de données (à valider ensemble — NE PLUS MODIFIER sans prévenir)
Message Kafka (JSON) sur le topic `releves_compteurs`, clé = `household_id` :

| Champ | Type | Exemple |
|---|---|---|
| household_id | texte | MAC000002 |
| ts | horodatage ISO | 2012-10-12 00:30:00 |
| kwh | décimal ≥ 0 | 0.136 |
| quartier | texte (groupe Acorn) | Affluent |

Silver (Hudi, `s3a://silver/releves`) : clé = `household_id, ts` · precombine = `ingested_at` · partition = `date`.
Gold (`s3a://gold/conso_foyer_jour` + table PostgreSQL) : household_id, quartier, date, conso_kwh, pic_demi_heure, nb_releves.
