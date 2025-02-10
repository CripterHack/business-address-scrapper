#!/bin/bash
set -e

echo "🚀 Starting post-creation setup..."

# Make sure we are in the correct directory
cd /workspace

# Update system
echo "📦 Updating system..."
sudo apt-get update

# Install system dependencies
echo "📦 Installing system dependencies..."
sudo apt-get install -y \
    build-essential \
    python3-dev \
    python3-pip \
    python3-venv \
    git \
    curl \
    wget \
    libpq-dev \
    postgresql-client \
    redis-tools \
    tesseract-ocr \
    libtesseract-dev \
    python3-pyqt5 \
    python3-pyqt5.qtwebkit \
    python3-pyqt5.sip \
    xvfb \
    libqt5webkit5-dev \
    qttools5-dev-tools \
    qt5-qmake \
    libqt5webkit5 \
    x11-utils \
    && sudo rm -rf /var/lib/apt/lists/*

# Configure Python and pip
echo "🐍 Configuring Python..."
python3 -m pip install --user --upgrade pip setuptools wheel

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p \
    logs/splash \
    cache \
    data/input \
    data/output \
    tests/logs \
    tests/data \
    stats \
    temp/uploads \
    .cache

# Create example .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "📝 Creating example .env file..."
    cat > .env << 'EOL'
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

# Metrics Configuration
METRICS_ENABLED=true
METRICS_PORT=9090

# Logging Configuration
LOG_LEVEL=INFO
LOG_FILE=logs/scraper.log

# Cache Configuration
CACHE_TYPE=redis
CACHE_TTL=3600
CACHE_MAX_SIZE=1000000000

# AI Features
ENABLE_AI_FEATURES=false
LLAMA_MODEL_PATH=models/llama-2-7b.gguf

# Splash Configuration
SPLASH_URL=http://localhost:8051
SPLASH_TIMEOUT=90
SPLASH_WAIT=5

# Scraper Configuration
SCRAPER_THREADS=4
REQUEST_TIMEOUT=30
MAX_RETRIES=3
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36
EOL
fi

# Wait for database to be ready
echo "🔄 Waiting for database to be ready..."
until PGPASSWORD=devpassword123 psql -h db -U postgres -d business_scraper -c '\q' 2>/dev/null; do
    echo "🔄 Database not available, retrying in 1 second..."
    sleep 1
done

# Run initial migrations
echo "🔄 Running initial migrations..."
DB_NAME=business_scraper \
DB_USER=postgres \
DB_PASSWORD=devpassword123 \
python3 -c "from scraper.database import Database; from scraper.settings import DatabaseSettings; db = Database(DatabaseSettings(name='business_scraper', user='postgres', password='devpassword123')); db.create_tables()"

echo "✨ Post-creation setup completed!" 