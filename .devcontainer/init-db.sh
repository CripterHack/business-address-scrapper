#!/bin/bash
set -e

echo "🔄 Initializing database..."

# Create database if it doesn't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE IF NOT EXISTS businesses (
        id SERIAL PRIMARY KEY,
        business_name VARCHAR(255) NOT NULL,
        address TEXT,
        city VARCHAR(100),
        state VARCHAR(2),
        zip_code VARCHAR(10),
        source_url TEXT,
        verified BOOLEAN DEFAULT false,
        confidence_score FLOAT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        metadata JSONB
    );

    CREATE INDEX IF NOT EXISTS idx_businesses_name ON businesses(business_name);
    CREATE INDEX IF NOT EXISTS idx_businesses_state ON businesses(state);
    CREATE INDEX IF NOT EXISTS idx_businesses_verified ON businesses(verified);
    CREATE INDEX IF NOT EXISTS idx_businesses_created_at ON businesses(created_at);
EOSQL

echo "✅ Database initialization completed!"

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