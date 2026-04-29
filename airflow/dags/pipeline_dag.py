"""
pipeline_dag.py
DAG principal du pipeline retail-pipeline-elt
Ordre : download_data -> load_raw -> dbt run -> dbt test
Planification : hebdomadaire (lundi 6h00 UTC)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# ── Paramètres par défaut ─────────────────────────────────────────────────────
default_args = {
    "owner"           : "pipeline",
    "depends_on_past" : False,
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Définition du DAG ─────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_pipeline_elt",
    description="Pipeline ELT Olist : ingestion -> raw -> staging -> marts",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * 1",   # tous les lundis a 6h UTC
    catchup=False,
    tags=["retail", "elt", "olist"],
) as dag:

    # ── Tâche 1 : téléchargement des données Kaggle ───────────────────────────
    download_data = BashOperator(
        task_id="download_data",
        bash_command="cd /opt/airflow/ingestion && python download_data.py",
        doc_md="Telecharge le dataset Olist depuis Kaggle via API",
    )

    # ── Tâche 2 : chargement dans le schéma raw ───────────────────────────────
    load_raw = BashOperator(
        task_id="load_raw",
        bash_command="cd /opt/airflow/ingestion && python load_raw.py",
        doc_md="Charge les CSV dans le schema raw de PostgreSQL",
    )

    # ── Tâche 3 : transformations dbt ─────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt/profiles",
        doc_md="Execute les modeles dbt : staging + marts",
    )

    # ── Tâche 4 : tests dbt ───────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt/profiles",
        doc_md="Execute les tests de qualite dbt",
    )

    # ── Ordre d'exécution ─────────────────────────────────────────────────────
    download_data >> load_raw >> dbt_run >> dbt_test