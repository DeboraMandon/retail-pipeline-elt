\# Architecture — retail-pipeline-elt



\## Vue d'ensemble



Pipeline ELT local orchestré par Airflow, transformé par dbt, stocké dans PostgreSQL et visualisé dans Metabase. Tout tourne dans Docker Compose.



\## Flux de données

Kaggle API
    ↓
download_data.py → data/raw/ (CSV)
    ↓
load_raw.py → PostgreSQL schéma raw (9 tables, ~1.5M lignes)
    ↓
dbt staging → vues nettoyées + cast de types (9 modèles)
    ↓
dbt marts → schéma étoile : fact_orders + 3 dimensions (4 tables)
    ↓
ML → ml.forecast_revenue (Prophet) + ml.satisfaction_predictions (régression logistique)
    ↓
Metabase → 2 dashboards (Analyse ELT + Prédictions ML)

Orchestration : DAG Airflow 6 tâches (hebdomadaire, lundi 6h UTC)



\## Schéma étoile (marts)

&#x20;     dim\_customers

&#x20;          │

&#x20;          │ customer\_id

&#x20;          │

dim\_sellers ───┤              ├─── dim\_products

seller\_id    │  fact\_orders │    product\_id

│              │

└──────────────┘

order\_id (PK)

customer\_id (FK)

item\_count

total\_price

total\_freight

total\_payment

delivery\_days

delay\_vs\_estimate\_days

review\_score



\## Choix techniques



\### PostgreSQL 16

\- Schémas séparés par couche (raw / staging / marts / ml)

\- `DISTINCT ON` pour dédupliquer les avis (syntaxe native PostgreSQL)

\- `init\_db.sql` exécuté automatiquement au premier démarrage



\### dbt 1.11

\- Macro `generate\_schema\_name` pour contrôler les noms de schémas

\- Staging : vues (pas de stockage physique, toujours à jour)

\- Marts : tables physiques (performance pour Metabase)

\- 12 tests de qualité (unique, not\_null, accepted\_values)



\### Airflow 2.9 (LocalExecutor)

\- DAG hebdomadaire (lundi 6h UTC)

\- 6 tâches BashOperator enchaînées

\- 2 retries avec 5 min de délai

\- `catchup=False` pour éviter les runs historiques



\### Docker Compose

\- PostgreSQL exposé sur 5433 (évite le conflit avec un PostgreSQL local)

\- Volume `airflow\_home` partagé entre webserver et scheduler

\- `requirements.txt` monté en volume pour la reproductibilité des packages


\### Bloc ML

\- Prophet 1.3.0 : prévision du CA mensuel (`growth='flat'`, saisonnalité multiplicative)

\- scikit-learn 1.8.0 : régression logistique sur 4 features (delivery_days, total_payment, item_count, total_freight)

\- Résultats stockés dans le schéma `ml` de PostgreSQL

\- Performance satisfaction : Accuracy 81%, AUC-ROC 0.680


\### Metabase

\- Connexion via réseau Docker interne (host: postgres, port: 5432)

\- Dashboard 1 — Analyse ELT : CA mensuel, Top catégories, Satisfaction, Délais livraison

\- Dashboard 2 — Prédictions ML : Prévision Prophet, Distribution probabilités satisfaction


\## Variables d'environnement



| Variable | Usage |

|---|---|

| POSTGRES\_USER / PASSWORD | Connexion PostgreSQL |

| PIPELINE\_DB / AIRFLOW\_DB / METABASE\_DB | Bases de données |

| AIRFLOW\_FERNET\_KEY | Chiffrement des connexions Airflow |

| AIRFLOW\_SECRET\_KEY | Sessions web Airflow |

| KAGGLE\_USERNAME / KEY | API Kaggle |

| DB\_HOST / DB\_PORT | Connexion depuis les conteneurs |



\## Ports exposés



| Service | Port machine | Port conteneur |

|---|---|---|

| PostgreSQL | 5433 | 5432 |

| Airflow | 8080 | 8080 |

| Metabase | 3000 | 3000 |

