#!/usr/bin/env python
"""Setup script for initializing the project environment."""

import os
import sys
from pathlib import Path

def create_directories():
    """Create necessary directories."""
    directories = [
        'logs',
        'cache',
        'data',
        'tests/logs',
        'tests/data'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {directory}")

def check_dependencies():
    """Check if all required system dependencies are installed."""
    try:
        import psycopg2
        import redis
        import prometheus_client
        print("All Python dependencies are installed correctly")
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Please run: pip install -r requirements.txt")
        sys.exit(1)

def check_database():
    """Check database connection."""
    try:
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', 5432),
            dbname=os.getenv('DB_NAME', 'business_scraper'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'your_password')
        )
        conn.close()
        print("Database connection successful")
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        print("Please check your database configuration in .env file")

def check_redis():
    """Check Redis connection."""
    try:
        import redis
        from dotenv import load_dotenv
        
        load_dotenv()
        
        client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0))
        )
        client.ping()
        client.close()
        print("Redis connection successful")
    except redis.ConnectionError as e:
        print(f"Redis connection failed: {e}")
        print("Please check your Redis configuration in .env file")
    except ImportError:
        print("Redis is optional and not installed")

def main():
    """Run all setup checks."""
    print("Starting setup checks...")
    print("-" * 50)
    
    create_directories()
    print("-" * 50)
    
    check_dependencies()
    print("-" * 50)
    
    check_database()
    print("-" * 50)
    
    check_redis()
    print("-" * 50)
    
    print("Setup checks completed")

if __name__ == "__main__":
    main() 