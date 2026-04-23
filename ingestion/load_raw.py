"""
load_raw.py
Charge les CSV Olist depuis data/raw/ vers le schéma raw de PostgreSQL.
Idempotent : DROP TABLE IF EXISTS + CREATE + COPY à chaque exécution.
Utilisation : python ingestion/load_raw.py
"""

import os
import pathlib
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

# ── Chargement des variables d'environnement ──────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST     = "localhost"
DB_PORT     = "5433"          # port exposé par Docker sur la machine
DB_NAME     = os.getenv("PIPELINE_DB")

# ── Chemins ───────────────────────────────────────────────────────────────────
ROOT_DIR     = pathlib.Path(__file__).parent.parent
DATA_RAW_DIR = ROOT_DIR / "data" / "raw"

# ── Mapping CSV → nom de table raw ───────────────────────────────────────────
CSV_TABLE_MAP = {
    "olist_customers_dataset.csv"            : "customers",
    "olist_geolocation_dataset.csv"          : "geolocation",
    "olist_order_items_dataset.csv"          : "order_items",
    "olist_order_payments_dataset.csv"       : "order_payments",
    "olist_order_reviews_dataset.csv"        : "order_reviews",
    "olist_orders_dataset.csv"               : "orders",
    "olist_products_dataset.csv"             : "products",
    "olist_sellers_dataset.csv"              : "sellers",
    "product_category_name_translation.csv"  : "product_category_name_translation",
}

# ── Connexion PostgreSQL ───────────────────────────────────────────────────────
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return sqlalchemy.create_engine(url)

# ── Chargement ────────────────────────────────────────────────────────────────
def load_csv(engine, csv_file: pathlib.Path, table_name: str):
    print(f"[INFO] Chargement de {csv_file.name} -> raw.{table_name}")
    df = pd.read_csv(csv_file, dtype=str)  # tout en string : on ne cast pas dans le raw
    df.to_sql(
        name=table_name,
        con=engine,
        schema="raw",
        if_exists="replace",   # DROP + CREATE + INSERT (idempotent)
        index=False,
        chunksize=10_000,
        method="multi",
    )
    print(f"[OK]   {len(df):>7} lignes chargées dans raw.{table_name}")

def load_all():
    if not DATA_RAW_DIR.exists():
        raise FileNotFoundError(
            f"Dossier {DATA_RAW_DIR} introuvable. Lance d'abord download_data.py"
        )

    engine = get_engine()
    print(f"[INFO] Connexion à {DB_HOST}:{DB_PORT}/{DB_NAME} en tant que {DB_USER}")

    total = 0
    for csv_name, table_name in CSV_TABLE_MAP.items():
        csv_path = DATA_RAW_DIR / csv_name
        if not csv_path.exists():
            print(f"[WARN] {csv_name} introuvable, ignoré")
            continue
        load_csv(engine, csv_path, table_name)
        total += 1

    print(f"\n[OK] {total}/{len(CSV_TABLE_MAP)} tables chargées dans le schéma raw.")

if __name__ == "__main__":
    load_all()