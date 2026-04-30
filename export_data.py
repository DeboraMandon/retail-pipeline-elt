import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv

load_dotenv()

url = (
    f"postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:5433/{os.getenv('PIPELINE_DB')}"
)
engine = sqlalchemy.create_engine(url)

queries = {
    "ca_mensuel": """
        SELECT date_trunc('month', order_purchased_at) AS mois,
               SUM(total_payment) AS ca_total,
               COUNT(*) AS nb_commandes
        FROM marts.fact_orders
        WHERE order_status = 'delivered'
        GROUP BY 1 ORDER BY 1
    """,
    "top_categories": """
        SELECT p.product_category_name_english AS categorie,
               SUM(f.total_payment) AS ca_total
        FROM marts.fact_orders f
        JOIN staging.stg_order_items oi ON f.order_id = oi.order_id
        JOIN marts.dim_products p ON oi.product_id = p.product_id
        WHERE p.product_category_name_english IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """,
    "satisfaction": """
        SELECT date_trunc('month', order_purchased_at) AS mois,
               ROUND(AVG(review_score)::numeric, 2) AS score_moyen
        FROM marts.fact_orders
        WHERE review_score IS NOT NULL
        GROUP BY 1 ORDER BY 1
    """,
    "delai_livraison": """
        SELECT c.customer_state AS etat,
               ROUND(AVG(f.delivery_days)::numeric, 1) AS delai_moyen,
               COUNT(*) AS nb_commandes
        FROM marts.fact_orders f
        JOIN marts.dim_customers c ON f.customer_id = c.customer_id
        WHERE f.delivery_days > 0 AND f.delivery_days IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """,
    "forecast": """
        SELECT ds, yhat, yhat_lower, yhat_upper
        FROM ml.forecast_revenue ORDER BY ds
    """,
    "satisfaction_proba": """
        SELECT ROUND(proba_satisfait::numeric, 1) AS tranche,
               COUNT(*) AS nb_commandes
        FROM ml.satisfaction_predictions
        GROUP BY 1 ORDER BY 1
    """,
    "raw_counts": """
        SELECT table_name,
               pg_stat_user_tables.n_live_tup AS nb_lignes
        FROM information_schema.tables
        JOIN pg_stat_user_tables
          ON information_schema.tables.table_name = pg_stat_user_tables.relname
        WHERE table_schema = 'raw'
        ORDER BY table_name
    """,
}

for name, sql in queries.items():
    df = pd.read_sql(sql, engine)
    df.to_csv(f"streamlit_data/{name}.csv", index=False)
    print(f"OK {name} : {len(df)} lignes")

print("Export termine !")