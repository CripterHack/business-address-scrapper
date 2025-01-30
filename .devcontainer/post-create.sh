#!/bin/bash
set -e

echo "🚀 Iniciando configuración post-creación..."

# Crear directorios necesarios
echo "📁 Creando directorios..."
mkdir -p logs
mkdir -p cache
mkdir -p data
mkdir -p tests/logs
mkdir -p tests/data

# Instalar dependencias con --user flag
echo "📦 Instalando dependencias de desarrollo..."
python -m pip install --user --no-cache-dir -r requirements.txt

# Verificar que pydantic se instaló correctamente
echo "🔍 Verificando instalación de pydantic..."
if ! python -c "import pydantic" 2>/dev/null; then
    echo "❌ Error: pydantic no se instaló correctamente"
    exit 1
fi

# Configurar pre-commit hooks
# echo "🔧 Configurando pre-commit hooks..."
# pre-commit install

# Crear archivo .env si no existe
if [ ! -f .env ]; then
    echo "📝 Creando archivo .env..."
    cat > .env << EOL
# Database Configuration
DB_HOST=db
DB_PORT=5432
DB_NAME=business_scraper
DB_USER=postgres
DB_PASSWORD=devpassword123
DB_POOL_SIZE=5

# Redis Configuration
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

# Cache Configuration
CACHE_TYPE=redis
CACHE_DIR=./cache
CACHE_TTL=3600

# Logging Configuration
LOG_LEVEL=DEBUG
LOG_FILE=./logs/scraper.log

# Metrics Configuration
ENABLE_METRICS=true
METRICS_PORT=8000

# Scraper Configuration
SCRAPER_THREADS=4
REQUEST_TIMEOUT=30
MAX_RETRIES=3
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36
EOL
fi

# Esperar a que la base de datos esté lista
echo "🔄 Esperando a que la base de datos esté lista..."
until PGPASSWORD=devpassword123 psql -h db -U postgres -d business_scraper -c '\q'; do
    echo "🔄 Base de datos no disponible, reintentando en 1 segundo..."
    sleep 1
done

# Ejecutar migraciones iniciales con las variables de entorno explícitas
echo "🔄 Ejecutando migraciones iniciales..."
DB_NAME=business_scraper \
DB_USER=postgres \
DB_PASSWORD=devpassword123 \
python -c "from scraper.database import Database; from scraper.settings import DatabaseSettings; db = Database(DatabaseSettings(name='business_scraper', user='postgres', password='devpassword123')); db.create_tables()"

echo "✅ Configuración post-creación completada!" 