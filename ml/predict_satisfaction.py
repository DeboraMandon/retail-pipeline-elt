"""
predict_satisfaction.py
Prediction de la satisfaction client (score >= 4) via regression logistique.
Features : delivery_days, total_payment, item_count, total_freight
Source : marts.fact_orders
Sortie : table ml.satisfaction_predictions dans PostgreSQL
Utilisation : python ml/predict_satisfaction.py
"""

import os
import pandas as pd
import sqlalchemy
import numpy as np
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler

# ── Chargement des variables d environnement ─────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5433")
DB_NAME     = os.getenv("PIPELINE_DB")

FEATURES = ['delivery_days', 'total_payment', 'item_count', 'total_freight']
TARGET   = 'satisfied'

# ── Connexion PostgreSQL ──────────────────────────────────────────────────────
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return sqlalchemy.create_engine(url)

# ── Extraction des donnees ────────────────────────────────────────────────────
def extract_data(engine):
    query = """
        SELECT
            order_id,
            delivery_days,
            total_payment,
            item_count,
            total_freight,
            review_score,
            CASE WHEN review_score >= 4 THEN 1 ELSE 0 END AS satisfied
        FROM marts.fact_orders
        WHERE review_score IS NOT NULL
          AND delivery_days IS NOT NULL
          AND delivery_days > 0
          AND total_payment > 0
    """
    df = pd.read_sql(query, engine)
    print(f"[INFO] {len(df)} commandes extraites")
    print(f"[INFO] Taux de satisfaction : {df['satisfied'].mean():.1%}")
    return df

# ── Entrainement du modele ────────────────────────────────────────────────────
def train_model(df):
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled  = scaler.transform(X_test)

    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train_scaled, y_train)

    y_pred = model.predict(X_test_scaled)
    y_prob = model.predict_proba(X_test_scaled)[:, 1]

    print("\n[INFO] Performance du modele :")
    print(classification_report(y_test, y_pred, target_names=['insatisfait', 'satisfait']))
    print(f"[INFO] AUC-ROC : {roc_auc_score(y_test, y_prob):.3f}")

    return model, scaler

# ── Prediction sur toutes les commandes ──────────────────────────────────────
def predict_all(engine, df, model, scaler):
    X = df[FEATURES]
    X_scaled = scaler.transform(X)

    df['proba_satisfait']  = model.predict_proba(X_scaled)[:, 1]
    df['pred_satisfait']   = model.predict(X_scaled)

    result = df[['order_id', 'review_score', 'satisfied',
                 'proba_satisfait', 'pred_satisfait'] + FEATURES]

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "DROP TABLE IF EXISTS ml.satisfaction_predictions CASCADE"
        ))

    result.to_sql(
        name="satisfaction_predictions",
        con=engine,
        schema="ml",
        if_exists="replace",
        index=False
    )
    print(f"\n[OK] {len(result)} predictions sauvegardees dans ml.satisfaction_predictions")

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    engine = get_engine()
    print(f"[INFO] Connexion a {DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = extract_data(engine)
    model, scaler = train_model(df)
    predict_all(engine, df, model, scaler)
    print("[OK] predict_satisfaction.py termine")

if __name__ == "__main__":
    run()