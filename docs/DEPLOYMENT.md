# Deployment Guide

## Prerequisites

### System Requirements
- Python 3.8 or higher
- PostgreSQL 12 or higher
- At least 2GB RAM
- At least 10GB free disk space
- Linux-based OS (Ubuntu 20.04 LTS recommended)

### Required Software
- Git
- Python virtual environment
- PostgreSQL
- Tesseract OCR
- Required system libraries:
  ```bash
  sudo apt-get update
  sudo apt-get install -y \
      python3-dev \
      python3-venv \
      postgresql \
      postgresql-contrib \
      tesseract-ocr \
      libtesseract-dev \
      libpq-dev
  ```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/business-address-scraper.git
   cd business-address-scraper
   ```

2. Create and activate virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   ```bash
   cp .env.example .env.prod
   # Edit .env.prod with your production settings
   ```

5. Create required directories:
   ```bash
   sudo mkdir -p /var/log/scraper
   sudo mkdir -p /var/data/scraper/{input,output}
   sudo mkdir -p /var/cache/scraper
   sudo mkdir -p /var/backup/scraper
   sudo chown -R your_user:your_group /var/log/scraper /var/data/scraper /var/cache/scraper /var/backup/scraper
   ```

## Configuration

1. Database Setup:
   ```bash
   sudo -u postgres psql
   CREATE DATABASE scraper_db;
   CREATE USER scraper_user WITH PASSWORD 'your_password';
   GRANT ALL PRIVILEGES ON DATABASE scraper_db TO scraper_user;
   ```

2. Configure `config.yaml`:
   - Set appropriate rate limits
   - Configure database connection
   - Set up monitoring endpoints
   - Configure backup settings

3. Set up logging:
   - Configure log rotation
   - Set appropriate log levels
   - Enable monitoring if needed

## Deployment

1. Run pre-deployment checks:
   ```bash
   python scripts/pre_deploy_check.py
   ```

2. Check production configuration:
   ```bash
   python scripts/check_prod_config.py
   ```

3. Deploy the application:
   ```bash
   python scripts/deploy.py
   ```

## Monitoring

1. Set up Prometheus metrics:
   ```bash
   # Install Prometheus
   sudo apt-get install -y prometheus

   # Configure scraper metrics endpoint
   sudo nano /etc/prometheus/prometheus.yml
   ```

2. Configure alerts:
   ```yaml
   # /etc/prometheus/alerts.yml
   groups:
   - name: scraper_alerts
     rules:
     - alert: HighErrorRate
       expr: scraper_errors_total > 100
       for: 5m
       labels:
         severity: warning
   ```

3. Set up logging monitoring:
   ```bash
   # Install monitoring tools
   sudo apt-get install -y prometheus-node-exporter
   ```

## Backup and Recovery

1. Automated Backups:
   - Daily database backups
   - Configuration backups
   - Log file backups

2. Recovery Procedure:
   ```bash
   # Restore from backup
   python scripts/restore.py --backup-file /path/to/backup.tar.gz
   ```

## Troubleshooting

### Common Issues

1. Database Connection:
   ```bash
   # Check database status
   sudo systemctl status postgresql
   # Check connection
   psql -U scraper_user -d scraper_db -h localhost
   ```

2. Permission Issues:
   ```bash
   # Fix directory permissions
   sudo chown -R your_user:your_group /var/log/scraper
   sudo chmod -R 755 /var/log/scraper
   ```

3. Memory Issues:
   ```bash
   # Check memory usage
   free -h
   # Check scraper process
   ps aux | grep scraper
   ```

### Logging

- Check application logs:
  ```bash
  tail -f /var/log/scraper/scraper.log
  ```

- Check error logs:
  ```bash
  grep ERROR /var/log/scraper/scraper.log
  ```

### Support

For additional support:
1. Check the documentation in the `docs/` directory
2. Submit an issue on GitHub
3. Contact the development team

## CI/CD Pipeline

### GitHub Actions Workflow
```yaml
name: CI/CD Pipeline
on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Run Tests
      - name: Check Coverage
      - name: Lint Code
  
  build:
    needs: test
    steps:
      - name: Build Docker Image
      - name: Push to Registry
  
  deploy:
    needs: build
    steps:
      - name: Deploy to Environment
```

### Deployment Strategies

#### Blue-Green Deployment
- Maintain two identical production environments
- Route traffic between versions using load balancer
- Zero-downtime deployments
- Easy rollback capability

#### Canary Releases
- Gradually route traffic to new version
- Monitor for errors and performance issues
- Automatic rollback on detection of issues

#### Rolling Updates
- Update instances one at a time
- Maintain application availability
- Configurable update batch size

## Scaling

### Horizontal Scaling
- Auto-scaling based on metrics:
  - CPU usage > 70%
  - Memory usage > 80%
  - Request queue length > 1000
- Load balancing configuration
- Database connection pooling

### Vertical Scaling
- Resource allocation guidelines
- Performance monitoring
- Cost optimization

### Database Scaling
- Read replicas
- Connection pooling
- Query optimization
- Partitioning strategy

## Monitoring Stack

### Prometheus + Grafana
- Key metrics:
  - Resource usage
  - Database performance
  - Scraping success rate
  - Processing time
  - Business metrics

### ELK Stack
- Log aggregation
- Log analysis
- Alert configuration
- Dashboard setup

### System Health Monitoring
- Database connectivity
- Cache availability
- External service status
- Resource utilization
- Process status 