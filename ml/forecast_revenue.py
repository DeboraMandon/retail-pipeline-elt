"""
forecast_revenue.py
Prevision du chiffre d affaires mensuel avec Prophet.
Source : marts.fact_orders
Sortie : table ml.forecast_revenue dans PostgreSQL
Utilisation : python ml/forecast_revenue.py
"""

import os
import pathlib
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from prophet import Prophet

# ── Chargement des variables d environnement ─────────────────────────────────
load_dotenv()

DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = os.getenv("DB_PORT", "5433")
DB_NAME     = os.getenv("PIPELINE_DB")

# ── Connexion PostgreSQL ──────────────────────────────────────────────────────
def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return sqlalchemy.create_engine(url)

# ── Extraction des donnees ────────────────────────────────────────────────────
def extract_monthly_revenue(engine):
    query = """
        SELECT
            date_trunc('month', order_purchased_at) AS ds,
            SUM(total_payment)                       AS y
        FROM marts.fact_orders
        WHERE order_purchased_at IS NOT NULL
          AND order_status = 'delivered'
          AND order_purchased_at < '2018-09-01'
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(query, engine)
    df['ds'] = pd.to_datetime(df['ds'])
    print(f"[INFO] {len(df)} mois de donnees extraits")
    return df

# ── Entrainement et prevision ─────────────────────────────────────────────────
def forecast(df, periods=3):
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode='multiplicative',
        growth='flat'
    )
    model.fit(df)
    future = model.make_future_dataframe(periods=periods, freq='MS')
    forecast_df = model.predict(future)
    print(f"[INFO] Prevision sur {periods} mois generee")
    return forecast_df[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]

# ── Sauvegarde en base ────────────────────────────────────────────────────────
def save_forecast(engine, forecast_df):
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(
            "DROP TABLE IF EXISTS ml.forecast_revenue CASCADE"
        ))
    forecast_df.to_sql(
        name="forecast_revenue",
        con=engine,
        schema="ml",
        if_exists="replace",
        index=False
    )
    print(f"[OK] {len(forecast_df)} lignes sauvegardees dans ml.forecast_revenue")

# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    engine = get_engine()
    print(f"[INFO] Connexion a {DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = extract_monthly_revenue(engine)
    forecast_df = forecast(df, periods=3)
    save_forecast(engine, forecast_df)
    print("[OK] forecast_revenue.py termine")

if __name__ == "__main__":
    run()