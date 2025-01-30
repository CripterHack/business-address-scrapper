#!/bin/bash
set -e

# Crear extensiones necesarias
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
EOSQL

# Crear usuario de aplicación si es necesario
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
#     CREATE USER app_user WITH PASSWORD 'app_password';
#     GRANT ALL PRIVILEGES ON DATABASE $POSTGRES_DB TO app_user;
# EOSQL

# Configurar permisos
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO PUBLIC;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO PUBLIC;
EOSQL 