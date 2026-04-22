-- =============================================================================
-- init_db.sql — exécuté automatiquement au premier démarrage de PostgreSQL
-- Crée les bases secondaires et les schémas métier dans pipeline_db
-- Note : pipeline_db est déjà créée par POSTGRES_DB dans docker-compose.yml
-- =============================================================================

-- Créer la base Metabase (pour stocker la config de l'outil)
SELECT 'CREATE DATABASE metabase_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'metabase_db'
)\gexec

-- Connexion à pipeline_db pour créer les schémas métier
\connect pipeline_db

-- Schéma raw : données brutes ingérées telles quelles depuis les sources
CREATE SCHEMA IF NOT EXISTS raw;

-- Schéma staging : premières transformations dbt (nettoyage, cast, renommage)
CREATE SCHEMA IF NOT EXISTS staging;

-- Schéma marts : modèles analytiques finaux (fact_orders, dim_customer, etc.)
CREATE SCHEMA IF NOT EXISTS marts;

-- Schéma ml : tables de prédictions produites par les scripts ML
CREATE SCHEMA IF NOT EXISTS ml;

-- Donner tous les droits sur les schémas à l'utilisateur principal
GRANT ALL PRIVILEGES ON SCHEMA raw      TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA staging  TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA marts    TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA ml       TO pipeline_user;

-- Droits par défaut : toute future table créée dans ces schémas
-- sera automatiquement accessible à pipeline_user
ALTER DEFAULT PRIVILEGES IN SCHEMA raw     GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts   GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA ml      GRANT ALL ON TABLES TO pipeline_user;
