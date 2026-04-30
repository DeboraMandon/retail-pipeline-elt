

\# retail-pipeline-elt



Pipeline ELT moderne de bout en bout sur le dataset \*\*Olist Brazil\*\* (e-commerce brésilien, Kaggle).



Construit pour démontrer la maîtrise d'une stack data moderne : ingestion, transformation, orchestration, qualité et visualisation.



\---



\## Stack technique



| Couche | Outil | Version |

|---|---|---|

| Infra | Docker Compose | v2.26 |

| Stockage | PostgreSQL | 16 |

| Ingestion | Python + API Kaggle | 3.13 |

| Transformation | dbt | 1.11 |

| ML | Prophet + scikit-learn | 1.3.0 / 1.8.0 |

| Orchestration | Apache Airflow | 2.9.3 |

| Dashboard | Metabase | latest |



\---



\## Architecture

Kaggle API

↓

download\_data.py → data/raw/ (CSV)

↓

load\_raw.py → PostgreSQL schéma raw (9 tables, \~1.5M lignes)

↓

dbt staging → vues nettoyées + cast de types (9 modèles)

↓

dbt marts → schéma étoile : fact\_orders + 3 dimensions (4 tables)

↓

Metabase → Dashboard analytique (CA, catégories, satisfaction, délais)

Orchestration : DAG Airflow (hebdomadaire, lundi 6h UTC)



\---



\## Dataset



\*\*Brazilian E-Commerce Public Dataset by Olist\*\* — \[Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)



9 tables CSV, \~120 MB, 2016-2018 :



| Table raw | Description | Lignes |

|---|---|---|

| orders | Commandes clients | 99 441 |

| customers | Clients | 99 441 |

| order\_items | Lignes de commande | 112 650 |

| order\_payments | Paiements | 103 886 |

| order\_reviews | Avis clients | 99 224 |

| products | Produits | 32 951 |

| sellers | Vendeurs | 3 095 |

| geolocation | Coordonnées GPS | 1 000 163 |

| product\_category\_name\_translation | Traduction catégories | 71 |



\---



\## Lancer le projet



\### Prérequis



\- Docker Desktop >= 4.x (4 Go RAM minimum)

\- Python 3.10+

\- Compte Kaggle + clé API



\### Installation



```bash

\# 1. Cloner le repo

git clone https://github.com/DeboraMandon/retail-pipeline-elt.git

cd retail-pipeline-elt



\# 2. Configurer les variables d'environnement

cp env.example .env

\# Remplir .env avec les vraies valeurs



\# 3. Démarrer l'infrastructure

docker compose up -d



\# 4. Télécharger les données

pip install kaggle python-dotenv

python ingestion/download\_data.py



\# 5. Charger en base

pip install pandas sqlalchemy psycopg2-binary

python ingestion/load\_raw.py



\# 6. Lancer les transformations dbt

pip install dbt-postgres

cd dbt \&\& dbt run \&\& dbt test

```



\### Interfaces



| Service | URL | Credentials |

|---|---|---|

| Airflow | http://localhost:8080 | admin / voir .env |

| Metabase | http://localhost:3000 | créer un compte |

| PostgreSQL | localhost:5433 | pipeline\_user / voir .env |



\---



\## Structure du repo

retail-pipeline-elt/

├── ingestion/          # Scripts Python (download + load)

├── dbt/

│   ├── models/

│   │   ├── staging/    # 9 vues de nettoyage

│   │   └── marts/      # 4 tables analytiques

│   └── macros/         # generate\_schema\_name

├── airflow/

│   └── dags/           # DAG Airflow

├── infra/              # init\_db.sql

├── ml/                 # Scripts ML (à venir)

├── docs/               # Documentation

└── docker-compose.yml



\---



\## Modèles dbt



\### Staging (vues)

Nettoyage et cast de types depuis le schéma `raw`.



\### Marts (tables)



\*\*fact\_orders\*\* — une ligne par commande

\- Métriques : item\_count, total\_price, total\_freight, total\_payment, delivery\_days, review\_score



\*\*dim\_customers\*\* — une ligne par client

\- Métriques : order\_count, total\_spent, first/last\_order\_at



\*\*dim\_products\*\* — une ligne par produit

\- Jointure avec la table de traduction (catégories en anglais)



\*\*dim\_sellers\*\* — une ligne par vendeur

\- Métriques : order\_count, total\_revenue, avg\_item\_price



\---



\## Dashboard Metabase



4 visualisations disponibles sur http://localhost:3000 :



\- \*\*CA mensuel\*\* — évolution du chiffre d'affaires (2016-2018)

\- \*\*Top 10 catégories\*\* — par revenus (bed\_bath\_table, health\_beauty...)

\- \*\*Satisfaction client\*\* — score moyen mensuel (1-5)

\- \*\*Délai de livraison\*\* — par état brésilien



\---


## Application interactive

🌐 **Demo en ligne** : [retail-pipeline-elt.streamlit.app](https://retail-pipeline-elt-9ffyrd8egbzgyevsyqqdfg.streamlit.app)

Lancer en local (données temps réel) :
\```bash
streamlit run streamlit_app/streamlit_app.py
\```


\## Auteur



\*\*Débora Mandon\*\* — \[GitHub](https://github.com/DeboraMandon)

