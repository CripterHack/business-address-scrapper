[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Type Checking](https://img.shields.io/badge/type%20checking-mypy-blue.svg)](http://mypy-lang.org/)

# Business Address Scraper

## 📦 Dependencies

### Required
- Python 3.8+
- PostgreSQL
- Redis (optional, for caching)

### Python Packages
- streamlit
- pandas
- plotly
- scrapy
- psycopg2-binary
- redis (if using Redis cache)

## 🔧 Features

- 📊 Interactive Dashboard
- 🔍 Business Search Interface
- 💾 Data Import/Export
- ⚙️ System Configuration
- 📈 Performance Metrics
- 🕷️ Web Scraping Capabilities

## 🔒 Security

- Secure environment variable handling through .env files
- SQL injection prevention using parameterized queries
- Input validation and sanitization
- Rate limiting for scraping operations
- Secure database connections

## 📋 Description
A comprehensive web scraping system for business addresses with an integrated graphical interface.

## 🚀 Quick Start with DevContainer

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Visual Studio Code](https://code.visualstudio.com/)
- [Remote - Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

### Initial Setup
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd business-address-scraper
   ```

2. Open in VS Code and start DevContainer:
   - Open VS Code in the project directory
   - Press `F1` or `Ctrl+Shift+P`
   - Select "Remote-Containers: Reopen in Container"
   - Wait for container build to complete

### Available Services
- **Streamlit Application**: http://localhost:8501
  - Main web interface for the application
- **PostgreSQL**: localhost:5432
  - Database: business_scraper
  - User: postgres
  - Password: devpassword123
- **Redis**: localhost:6379
  - Used for caching and performance optimization
- **pgAdmin**: http://localhost:5050
  - Email: admin@admin.com
  - Password: admin
  - Web interface for database management

### Project Structure
```
.
├── .devcontainer/          # Development environment configuration
├── app.py                  # Main Streamlit application
├── scraper/               # Core scraper functionality
│   ├── spiders/          # Scrapy spider definitions
│   ├── database.py       # Database operations
│   └── settings.py       # Configuration settings
├── tests/                # Test suite
├── docs/                 # Documentation
├── scripts/              # Utility scripts
└── requirements.txt      # Project dependencies
```

## 💾 Database Schema

The application uses PostgreSQL with the following main tables:
- businesses: Stores business information
  - id: Primary key
  - name: Business name
  - state: State location
  - verified: Verification status
  - violation_type: Type of violation (if any)
  - created_at: Record creation timestamp
  - updated_at: Last update timestamp

## 🛠️ Development

### Useful Commands
```bash
# Run the application
streamlit run app.py

# Run tests
pytest

# Check test coverage
pytest --cov=scraper tests/

# Format code
black .

# Type checking
mypy .

# Run scraper
python run_scraper.py
```

### Development Workflow
1. Development environment is automatically configured when opening in DevContainer
2. VS Code extensions are preconfigured for:
   - Automatic formatting with Black
   - Linting with Flake8
   - Type checking with MyPy
   - Pre-commit hooks

### Database
- Database is automatically initialized with required tables
- Indexes and extensions are created during initialization
- pgAdmin available for visual administration

### Cache
- Redis is used as the default caching system
- Configuration adjustable in .env file
- Monitoring available through metrics interface

## 📊 Metrics and Monitoring
- Performance metrics available in interface
- Monitoring of:
  - CPU and memory usage
  - Database connections
  - Cache hits/misses
  - Errors and latencies

## 🔧 Configuration

### Environment Variables
Required environment variables:
```env
# Required
QWANT_API_KEY=your_qwant_api_key_here    # API key for Qwant search engine

# Database Configuration
DB_HOST=db                          # Database host
DB_PORT=5432                        # Database port
DB_NAME=business_scraper           # Database name
DB_USER=postgres                    # Database user
DB_PASSWORD=devpassword123         # Database password
DB_POOL_SIZE=5                     # Database pool size

# Redis Configuration
REDIS_HOST=redis                    # Redis host
REDIS_PORT=6379                     # Redis port
REDIS_DB=0                         # Redis database number

# Cache Configuration
CACHE_TYPE=redis                    # Options: redis, filesystem
CACHE_TTL=3600                     # Cache time-to-live in seconds

# Logging Configuration
LOG_LEVEL=INFO                     # Logging level
LOG_FILE=scraper.log               # Logging file

# Scraper Configuration
SCRAPER_MODE=development           # Scraper mode
SCRAPER_THREADS=4                   # Number of concurrent scraping threads
REQUEST_TIMEOUT=30                  # Request timeout in seconds
MAX_RETRIES=3                      # Maximum number of retry attempts

# Metrics Configuration
ENABLE_METRICS=true                 # Enable metrics
METRICS_PORT=8000                   # Metrics port
```

Choose the appropriate example file and copy it to `.env`:
```bash
# For development
cp .env.example .env

# For production
cp .env.prod.example .env

# For testing
cp .env.test.example .env
```

### Customization
- Modify `.devcontainer/devcontainer.json` to adjust environment settings
- Adjust `docker-compose.yml` to modify services
- Configure additional VS Code extensions

## 🔍 Troubleshooting

### Common Issues
1. **Database Connection Error**:
   - Verify PostgreSQL service is running
   - Check credentials in `.env`
   - Review logs with `docker-compose logs db`

2. **Redis Connection Error**:
   - Verify Redis service is running
   - Check configuration in `.env`
   - Review logs with `docker-compose logs redis`

3. **DevContainer Issues**:
   - Rebuild container: "Remote-Containers: Rebuild Container"
   - Check Docker resources allocation
   - Review build logs

### Logs
- Application logs: `./logs/scraper.log`
- Docker logs: `docker-compose logs`
- Specific service logs: `docker-compose logs [service]`

## 📝 Contributing
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Commit your changes: `git commit -am 'Add new feature'`
4. Push to the branch: `git push origin feature/new-feature`
5. Create a Pull Request

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 