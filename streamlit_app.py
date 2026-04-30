"""
streamlit_app.py
Interface graphique du projet retail-pipeline-elt
4 onglets : Contexte | Pipeline | Analytics | ML
Utilisation : streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlalchemy
import os
from dotenv import load_dotenv

# ── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

st.set_page_config(
    page_title="retail-pipeline-elt",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Connexion PostgreSQL ───────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    try:
        url = (
            f"postgresql+psycopg2://"
            f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
            f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5433')}"
            f"/{os.getenv('PIPELINE_DB')}"
        )
        engine = sqlalchemy.create_engine(url)
        # Test de connexion
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))
        return engine
    except Exception:
        return None

@st.cache_data(ttl=300)
def query(_engine, sql):
    return pd.read_sql(sql, _engine)

# ── Onglets ───────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Contexte & Objectif",
    "⚙️ Pipeline",
    "📊 Analytics",
    "🤖 ML"
])

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 1 — CONTEXTE & OBJECTIF
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.title("🔄 retail-pipeline-elt")
    st.markdown("### Pipeline ELT moderne de bout en bout")
    st.markdown("---")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown("""
        #### Objectif
        Construire un pipeline ELT complet sur le dataset **Olist Brazil**
        (e-commerce bresilien, Kaggle) en utilisant une stack open source industrialisable.

        #### Qu'est-ce qu'un pipeline ELT ?
        - **Extract** : telecharger les donnees brutes (Kaggle API)
        - **Load** : charger sans transformation dans PostgreSQL (schema `raw`)
        - **Transform** : transformer en SQL avec dbt (schemas `staging` et `marts`)

        La transformation se fait **apres** le chargement, directement en base.
        C'est plus performant et traçable qu'un ETL classique.
        """)

    with col2:
        st.markdown("#### Chiffres cles")
        st.metric("Tables raw", "9")
        st.metric("Lignes totales", "~1.55M")
        st.metric("Modeles dbt", "13")
        st.metric("Tests qualite", "12 PASS")
        st.metric("Taches Airflow", "6")

    st.markdown("---")
    st.markdown("#### Stack technique")

    cols = st.columns(6)
    stack = [
        ("🐘", "PostgreSQL 16", "Stockage"),
        ("🔧", "dbt 1.11", "Transformation"),
        ("🌊", "Airflow 2.9", "Orchestration"),
        ("🐍", "Python 3.13", "Ingestion + ML"),
        ("📊", "Metabase", "Dashboard"),
        ("🐳", "Docker", "Infrastructure"),
    ]
    for col, (icon, name, role) in zip(cols, stack):
        with col:
            st.markdown(f"**{icon} {name}**")
            st.caption(role)

    st.markdown("---")
    st.markdown("#### Architecture du pipeline")
    st.code("""
Kaggle API
    ↓
download_data.py  →  data/raw/*.csv
    ↓
load_raw.py       →  PostgreSQL : schema raw (9 tables, ~1.55M lignes)
    ↓
dbt staging       →  PostgreSQL : schema staging (9 vues, types castes)
    ↓
dbt marts         →  PostgreSQL : schema marts (4 tables, schema etoile)
    ↓
ML                →  PostgreSQL : schema ml (forecast + predictions)
    ↓
Metabase + Streamlit  →  Dashboards analytiques

Orchestration : DAG Airflow hebdomadaire (lundi 6h UTC)
    """, language="text")

    st.markdown("#### Schema etoile (marts)")
    st.code("""
         dim_customers
               |
dim_sellers — fact_orders — dim_products
               |
          (review_score, delivery_days, total_payment...)
    """, language="text")

    st.markdown("---")
    st.info("📁 Code source : [github.com/DeboraMandon/retail-pipeline-elt](https://github.com/DeboraMandon/retail-pipeline-elt)")

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 2 — PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.title("⚙️ Etat du pipeline")
    st.markdown("---")

    engine = get_engine()
    if engine is None:
        st.warning("⚠️ Base de donnees inaccessible. Verifie que Docker est demarré (docker compose up -d)")
        st.stop()

    # Etat des tables raw
    st.markdown("### 📦 Schema raw — tables ingérees")
    raw_tables = [
        "orders", "customers", "order_items", "order_payments",
        "order_reviews", "products", "sellers", "geolocation",
        "product_category_name_translation"
    ]
    raw_data = []
    for table in raw_tables:
        try:
            count = query(engine, f"SELECT COUNT(*) as n FROM raw.{table}").iloc[0]["n"]
            raw_data.append({"Table": f"raw.{table}", "Lignes": f"{count:,}", "Statut": "✅"})
        except:
            raw_data.append({"Table": f"raw.{table}", "Lignes": "-", "Statut": "❌"})
    st.dataframe(pd.DataFrame(raw_data), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🔧 Schema marts — modeles dbt")
    mart_tables = ["fact_orders", "dim_customers", "dim_products", "dim_sellers"]
    mart_data = []
    for table in mart_tables:
        try:
            count = query(engine, f"SELECT COUNT(*) as n FROM marts.{table}").iloc[0]["n"]
            mart_data.append({"Modele": f"marts.{table}", "Lignes": f"{count:,}", "Statut": "✅"})
        except:
            mart_data.append({"Modele": f"marts.{table}", "Lignes": "-", "Statut": "❌"})
    st.dataframe(pd.DataFrame(mart_data), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### 🤖 Schema ml — predictions")
    ml_tables = ["forecast_revenue", "satisfaction_predictions"]
    ml_data = []
    for table in ml_tables:
        try:
            count = query(engine, f"SELECT COUNT(*) as n FROM ml.{table}").iloc[0]["n"]
            ml_data.append({"Table": f"ml.{table}", "Lignes": f"{count:,}", "Statut": "✅"})
        except:
            ml_data.append({"Table": f"ml.{table}", "Lignes": "-", "Statut": "❌"})
    st.dataframe(pd.DataFrame(ml_data), use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### ✅ Tests dbt")
    tests = [
        {"Test": "unique — stg_orders.order_id", "Resultat": "PASS ✅"},
        {"Test": "unique — stg_customers.customer_id", "Resultat": "PASS ✅"},
        {"Test": "not_null — stg_orders (order_id, customer_id, order_status)", "Resultat": "PASS ✅"},
        {"Test": "not_null — stg_customers (customer_id, unique_id, state)", "Resultat": "PASS ✅"},
        {"Test": "not_null — stg_order_items (order_id, order_item_id, price)", "Resultat": "PASS ✅"},
        {"Test": "accepted_values — stg_orders.order_status (8 valeurs)", "Resultat": "PASS ✅"},
    ]
    st.dataframe(pd.DataFrame(tests), use_container_width=True, hide_index=True)
    st.success("12/12 tests dbt — PASS")

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 3 — ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.title("📊 Analytics — Olist Brazil")
    st.markdown("---")

    engine = get_engine()
    if engine is None:
        st.warning("⚠️ Base de donnees inaccessible. Verifie que Docker est demarré (docker compose up -d)")
        st.stop()

    # CA mensuel
    st.markdown("### 💰 Chiffre d'affaires mensuel")
    df_ca = query(engine, """
        SELECT date_trunc('month', order_purchased_at) AS mois,
               SUM(total_payment) AS ca_total,
               COUNT(*) AS nb_commandes
        FROM marts.fact_orders
        WHERE order_status = 'delivered'
        GROUP BY 1 ORDER BY 1
    """)
    df_ca["mois"] = pd.to_datetime(df_ca["mois"])
    fig_ca = px.line(df_ca, x="mois", y="ca_total",
                     title="CA mensuel (commandes livrees)",
                     labels={"mois": "Mois", "ca_total": "CA (BRL)"},
                     color_discrete_sequence=["#065A82"])
    fig_ca.update_traces(line_width=2.5)
    st.plotly_chart(fig_ca, use_container_width=True)

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        # Top categories
        st.markdown("### 🛍️ Top 10 categories")
        df_cat = query(engine, """
            SELECT p.product_category_name_english AS categorie,
                   SUM(f.total_payment) AS ca_total
            FROM marts.fact_orders f
            JOIN staging.stg_order_items oi ON f.order_id = oi.order_id
            JOIN marts.dim_products p ON oi.product_id = p.product_id
            WHERE p.product_category_name_english IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 10
        """)
        fig_cat = px.bar(df_cat, x="ca_total", y="categorie",
                         orientation="h",
                         labels={"ca_total": "CA (BRL)", "categorie": ""},
                         color_discrete_sequence=["#1C7293"])
        fig_cat.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_cat, use_container_width=True)

    with col2:
        # Satisfaction mensuelle
        st.markdown("### ⭐ Satisfaction client mensuelle")
        df_sat = query(engine, """
            SELECT date_trunc('month', order_purchased_at) AS mois,
                   ROUND(AVG(review_score)::numeric, 2) AS score_moyen
            FROM marts.fact_orders
            WHERE review_score IS NOT NULL
            GROUP BY 1 ORDER BY 1
        """)
        df_sat["mois"] = pd.to_datetime(df_sat["mois"])
        fig_sat = px.line(df_sat, x="mois", y="score_moyen",
                          labels={"mois": "Mois", "score_moyen": "Score moyen (1-5)"},
                          color_discrete_sequence=["#02C39A"])
        fig_sat.update_traces(line_width=2.5)
        fig_sat.update_layout(yaxis=dict(range=[1, 5]))
        st.plotly_chart(fig_sat, use_container_width=True)

    st.markdown("---")
    # Delai par etat
    st.markdown("### 🚚 Delai de livraison moyen par etat bresilien")
    df_delai = query(engine, """
        SELECT c.customer_state AS etat,
               ROUND(AVG(f.delivery_days)::numeric, 1) AS delai_moyen,
               COUNT(*) AS nb_commandes
        FROM marts.fact_orders f
        JOIN marts.dim_customers c ON f.customer_id = c.customer_id
        WHERE f.delivery_days > 0 AND f.delivery_days IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """)
    fig_delai = px.bar(df_delai, x="etat", y="delai_moyen",
                       color="delai_moyen",
                       color_continuous_scale="Blues",
                       labels={"etat": "Etat", "delai_moyen": "Delai moyen (jours)"},
                       hover_data=["nb_commandes"])
    st.plotly_chart(fig_delai, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# ONGLET 4 — ML
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    st.title("🤖 Predictions ML")
    st.markdown("---")

    engine = get_engine()
    if engine is None:
        st.warning("⚠️ Base de donnees inaccessible. Verifie que Docker est demarré (docker compose up -d)")
        st.stop()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📈 Prevision CA — Prophet")
        st.caption("Modele : growth='flat', seasonality_mode='multiplicative'")
        df_fc = query(engine, """
            SELECT ds, yhat, yhat_lower, yhat_upper
            FROM ml.forecast_revenue ORDER BY ds
        """)
        df_fc["ds"] = pd.to_datetime(df_fc["ds"])
        cutoff = df_fc["ds"].max() - pd.DateOffset(months=3)

        fig_fc = go.Figure()
        hist = df_fc[df_fc["ds"] <= cutoff]
        pred = df_fc[df_fc["ds"] > cutoff]

        fig_fc.add_trace(go.Scatter(x=hist["ds"], y=hist["yhat"],
                                     name="Historique", line=dict(color="#065A82", width=2)))
        fig_fc.add_trace(go.Scatter(x=pred["ds"], y=pred["yhat"],
                                     name="Prevision", line=dict(color="#02C39A", width=2, dash="dash")))
        fig_fc.add_trace(go.Scatter(
            x=pd.concat([pred["ds"], pred["ds"][::-1]]),
            y=pd.concat([pred["yhat_upper"], pred["yhat_lower"][::-1]]),
            fill="toself", fillcolor="rgba(2,195,154,0.15)",
            line=dict(color="rgba(255,255,255,0)"),
            name="Intervalle confiance"
        ))
        fig_fc.update_layout(xaxis_title="Mois", yaxis_title="CA (BRL)")
        st.plotly_chart(fig_fc, use_container_width=True)

    with col2:
        st.markdown("### 🎯 Satisfaction — Distribution des probabilites")
        st.caption("Modele : regression logistique, 4 features, AUC-ROC=0.680")
        df_proba = query(engine, """
            SELECT ROUND(proba_satisfait::numeric, 1) AS tranche,
                   COUNT(*) AS nb_commandes
            FROM ml.satisfaction_predictions
            GROUP BY 1 ORDER BY 1
        """)
        fig_proba = px.bar(df_proba, x="tranche", y="nb_commandes",
                           color="tranche",
                           color_continuous_scale="Teal",
                           labels={"tranche": "Probabilite predite", "nb_commandes": "Nb commandes"})
        st.plotly_chart(fig_proba, use_container_width=True)

    st.markdown("---")
    st.markdown("### 📊 Metriques du modele de satisfaction")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Accuracy", "81%")
    col2.metric("AUC-ROC", "0.680")
    col3.metric("Taux satisfaits", "78.9%")
    col4.metric("Commandes predites", "95 829")

    st.markdown("---")
    st.markdown("### 🔍 Explorer les predictions")
    seuil = st.slider("Seuil de probabilite de satisfaction", 0.0, 1.0, 0.5, 0.05)
    df_sample = query(engine, f"""
        SELECT order_id, review_score, satisfied,
               ROUND(proba_satisfait::numeric, 3) AS proba_satisfait,
               pred_satisfait, delivery_days,
               ROUND(total_payment::numeric, 2) AS total_payment
        FROM ml.satisfaction_predictions
        WHERE proba_satisfait >= {seuil - 0.05}
          AND proba_satisfait <= {seuil + 0.05}
        LIMIT 20
    """)
    st.dataframe(df_sample, use_container_width=True, hide_index=True)