# Business Address Scraper

Distributed scraping system with cache for business information extraction.

## Main Features

### Base System
- Scalable distributed architecture
- Configurable multi-threaded processing
- Advanced logging system with customizable levels
- Intelligent error handling and automatic recovery
- Efficient system resource management

### Distributed Cache
- Support for multiple backends (Redis, Memcached)
- Configurable compression and encryption
- Policy-based automatic cleanup system
- Configurable replication and consistency
- Intelligent memory and space management

### Alert System
- Real-time monitoring of critical events
- Configurable severity levels
- Detailed alert history with metadata
- Detection and grouping of duplicate alerts
- Integration with metrics system

### Metrics and Monitoring
- Automatic system metrics collection
- Performance and resource monitoring
- Detailed operation statistics
- Configurable log rotation system
- Standard format metrics export

### Security
- Configurable authentication system
- Protection against brute force attacks
- Token and session management
- Sensitive data encryption
- Configurable access policies

### Advanced Processing
- OCR Integration (Tesseract)
- AI capabilities with LLaMA model
- Parallel data processing
- Configurable extraction pipeline
- Data validation and cleaning

### Resource Management
- Automatic temporary resource cleanup
- Configurable backup management
- CPU and memory usage control
- Disk space monitoring
- Automatic failure recovery

## Project Structure

```
scraper/
├── __init__.py
├── alerts/
│   ├── __init__.py
│   ├── manager.py
│   ├── handlers.py
│   └── metrics.py
├── cache/
│   ├── __init__.py
│   ├── distributed.py
│   ├── cleaner.py
│   ├── compression.py
│   ├── encryption.py
│   └── priority.py
├── core/
│   ├── __init__.py
│   ├── config.py
│   ├── logging.py
│   ├── metrics.py
│   └── utils.py
├── db/
│   ├── __init__.py
│   ├── models.py
│   ├── session.py
│   └── operations.py
├── extractors/
│   ├── __init__.py
│   ├── base.py
│   ├── text.py
│   ├── ocr.py
│   └── ai.py
├── monitor/
│   ├── __init__.py
│   ├── system.py
│   ├── resources.py
│   └── alerts.py
├── security/
│   ├── __init__.py
│   ├── auth.py
│   ├── encryption.py
│   └── tokens.py
└── utils/
    ├── __init__.py
    ├── validation.py
    ├── formatting.py
    └── helpers.py

config/
├── logging.yaml
├── cache.yaml
├── alerts.yaml
├── metrics.yaml
└── security.yaml

tests/
├── unit/
├── integration/
└── performance/

docs/
├── api/
├── setup/
└── examples/
```

### Distributed Cache System

- **Authentication**: Role and token-based access control
- **Compression**: Automatic compression based on data type and size
- **Encryption**: Transparent sensitive data encryption
- **Events**: Pub/sub system for monitoring and reaction
- **Partitioning**: Consistent data distribution
- **Replication**: Redundant copies for high availability
- **Circuit Breakers**: Protection against cascade failures
- **Cleanup**: Automatic data aging management
- **Error Handling**: Unified system with:
  - Detailed logging
  - Error metrics
  - Automatic notifications
  - Intelligent recovery
- **Resource Management**:
  - Automatic connection closure
  - Resource cleanup
  - Context managers
  - Lifecycle management
- **Statistics**:
  - Node performance
  - Resource usage
  - Operations by type
  - Temporal analysis

### Event System

The system uses a centralized event manager to monitor and react to different situations:

#### Event Types

- **Critical** (High Priority):
  - Errors
  - Node failures
  - Recovery/migration failures
  
- **Operational** (Medium Priority):
  - Warnings
  - Migrations
  - Rebalancing
  - Backups/Restorations
  
- **Informational** (Low Priority):
  - GET/SET operations
  - Informational logs
  - Metrics

### Alert System

- **Configuration**:
  - Customizable thresholds by alert type
  - Configurable severity levels
  - Related alert grouping
  - Configurable duplication windows
  
- **Monitoring**:
  - Detailed alert history
  - Severity statistics
  - Filtering and search
  - Alert metrics
  - Automatic history cleanup
  
- **Notifications**:
  - System event integration
  - Similar alert aggregation
  - Alert storm prevention
  - Duplicate detection
  - Silence windows

- **Resource Management**:
  - Automatic periodic cleanup
  - Memory management
  - Context managers
  - Orderly shutdown

- **Statistics**:
  - Period summaries
  - Severity distribution
  - Trend analysis
  - Duplication metrics
  - Cleanup efficiency

### Monitoring System

- **Real-time Metrics**:
  - Operation latency
  - Success/error rates
  - Resource usage
  - Node statistics
  - Access patterns
  
- **Configurable Alerts**:
  - Dynamic thresholds
  - Event correlation
  - Trend analysis
  
- **Reports**:
  - Historical performance
  - Error analysis
  - Resource usage
  - Access patterns
  - Periodic summaries

## Installation

### Prerequisites
- Python 3.8+
- Redis 6.0+ or Memcached 1.6+
- PostgreSQL 12+ (optional)
- Tesseract 4.1+ (optional for OCR)
- CUDA 11.0+ (optional for AI)

### Basic Installation
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Initial setup
python setup.py install
```

### Installation with Optional Features
```bash
# OCR
pip install -r requirements-ocr.txt

# AI
pip install -r requirements-ai.txt

# Database
pip install -r requirements-db.txt
```

## Configuration

### Basic Configuration
1. Copy example files:
```bash
cp config/*.yaml.example config/*.yaml
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your values
```

### Advanced Configuration

#### Cache
1. Choose backend (Redis/Memcached)
2. Configure parameters in `config/cache.yaml`
3. Adjust related environment variables

#### Alert System
1. Define severity levels
2. Configure thresholds in `config/alerts.yaml`
3. Set notification policies

#### Metrics
1. Enable metrics collection
2. Configure intervals in `config/metrics.yaml`
3. Define log rotation policies

#### Security
1. Generate encryption keys
2. Configure policies in `config/security.yaml`
3. Set authentication parameters

## Usage

### Start the System
```bash
# Start the web interface
streamlit run app.py

# Run the scraper only
python run_scraper.py
```

### Monitoring
```bash
# View real-time metrics
python -m scraper.monitor metrics

# View system status
python -m scraper.monitor status

# View active alerts
python -m scraper.monitor alerts
```

### Maintenance
```bash
# Clean cache
python -m scraper.cache clean

# Rotate logs
python -m scraper.utils rotate-logs

# Data backup
python -m scraper.utils backup
```

## Tests

### Run Tests
```bash
# Unit tests
python -m pytest tests/unit

# Integration tests
python -m pytest tests/integration

# Performance tests
python -m pytest tests/performance

# All tests with coverage
python -m pytest --cov=scraper tests/
```

### Specific Tests
```bash
# Cache system tests
python -m pytest tests/unit/test_cache.py

# Alert system tests
python -m pytest tests/unit/test_alerts.py

# Cache performance tests
python -m pytest tests/performance/test_cache_performance.py
```

### Code Analysis
```bash
# Static analysis
flake8 scraper

# Type checking
mypy scraper

# Code formatting
black scraper
```

## Contributing

### Contribution Guide

1. Fork the repository
2. Create a branch for your feature: `git checkout -b feature/feature-name`
3. Implement your changes following style guides
4. Ensure all tests pass
5. Update documentation if necessary
6. Create a pull request

### Code Standards

- Follow PEP 8 for Python code style
- Document all functions and classes with docstrings
- Maintain test coverage > 80%
- Use type hints in all functions
- Maintain cyclomatic complexity < 10

### Development Flow

1. Create issue describing the change
2. Discuss implementation in the issue
3. Implement changes in a branch
4. Run complete test suite
5. Create pull request
6. Code review and approval
7. Merge to main

### Report Bugs

- Use GitHub's issue system
- Include steps to reproduce
- Attach relevant logs
- Specify system version
- Describe expected vs actual behavior

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact and Support

### Communication Channels
- **GitHub Issues**: For bug reports and feature requests
- **Discussions**: For general questions and discussions
- **Wiki**: For extended documentation and guides

### Additional Resources
- [API Documentation](docs/api/README.md)
- [Development Guide](docs/development.md)
- [Usage Examples](docs/examples/README.md)
- [Troubleshooting Guide](docs/troubleshooting.md)

### Maintainers
- Keep code updated
- Review pull requests
- Respond to issues
- Update documentation

---
**Note**: This project is in active development. Contributions are welcome.