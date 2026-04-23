"""
download_data.py
Télécharge le dataset Olist Brazil depuis Kaggle et le place dans data/raw/.
Prérequis : KAGGLE_USERNAME et KAGGLE_KEY dans le .env
Utilisation : python ingestion/download_data.py
"""

import os
import zipfile
import pathlib
from dotenv import load_dotenv

# ── Chargement des variables d'environnement ──────────────────────────────────
load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_KEY      = os.getenv("KAGGLE_KEY")

if not KAGGLE_USERNAME or not KAGGLE_KEY:
    raise EnvironmentError(
        "KAGGLE_USERNAME ou KAGGLE_KEY manquant dans le .env"
    )

# Kaggle lit ses credentials depuis les variables d'environnement
os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
os.environ["KAGGLE_KEY"]      = KAGGLE_KEY

# ── Import Kaggle API (après injection des credentials) ───────────────────────
from kaggle.api.kaggle_api_extended import KaggleApi as KaggleApiExtended

# ── Chemins ───────────────────────────────────────────────────────────────────
ROOT_DIR     = pathlib.Path(__file__).parent.parent
DATA_RAW_DIR = ROOT_DIR / "data" / "raw"
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

DATASET = "olistbr/brazilian-ecommerce"

# ── Téléchargement ────────────────────────────────────────────────────────────
def download():
    print(f"[INFO] Connexion à l'API Kaggle en tant que {KAGGLE_USERNAME}")
    api = KaggleApiExtended()
    api.authenticate()

    print(f"[INFO] Téléchargement du dataset {DATASET}...")
    api.dataset_download_files(
        dataset=DATASET,
        path=str(DATA_RAW_DIR),
        unzip=True,
        quiet=False,
        force=False   # ne re-télécharge pas si déjà présent
    )
    print(f"[OK] Fichiers disponibles dans {DATA_RAW_DIR}")
    for f in sorted(DATA_RAW_DIR.glob("*.csv")):
        size_mb = f.stat().st_size / 1_048_576
        print(f"     {f.name:<60} {size_mb:.1f} MB")

if __name__ == "__main__":
    download()