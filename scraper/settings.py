"""Configuration settings for the scraper."""

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseModel, Field, validator

from .exceptions import ConfigurationError

class DatabaseSettings(BaseModel):
    """Database connection settings."""
    enabled: bool = Field(default=True)
    type: str = Field(default="postgresql")
    host: str = Field(default="db")
    port: int = Field(default=5432)
    name: str = Field(default="business_scraper")
    user: str = Field(default="postgres")
    password: str = Field(default="devpassword123")
    pool_size: int = Field(default=5)
    max_overflow: int = Field(default=10)
    timeout: int = Field(default=30)

    @validator('type')
    def validate_db_type(cls, v):
        """Validate database type."""
        valid_types = ['postgresql', 'mysql', 'sqlite']
        if v not in valid_types:
            raise ValueError(f"Database type must be one of {valid_types}")
        return v

class ScraperSettings(BaseModel):
    """Scraper behavior settings."""
    mode: str = Field(default="development")
    rate_limit: int = Field(default=1)
    max_retries: int = Field(default=3)
    chunk_size: int = Field(default=1000)
    user_agent: str = Field(default="Business Address Scraper/1.0")
    respect_robots_txt: bool = Field(default=True)
    download_delay: int = Field(default=2)
    concurrent_requests: int = Field(default=1)

    @validator('mode')
    def validate_mode(cls, v):
        """Validate scraper mode."""
        valid_modes = ['development', 'testing', 'sandbox', 'production']
        if v not in valid_modes:
            raise ValueError(f"Mode must be one of {valid_modes}")
        return v

    @validator('rate_limit', 'max_retries', 'chunk_size', 'download_delay', 'concurrent_requests')
    def validate_positive_int(cls, v):
        """Validate positive integers."""
        if v < 1:
            raise ValueError("Value must be positive")
        return v

class OutputSettings(BaseModel):
    """Output configuration settings."""
    format: str = Field(default="csv")
    directory: Path = Field(default=Path("data/output"))
    filename_pattern: str = Field(default="business_data_{timestamp}.csv")
    chunk_enabled: bool = Field(default=True)
    chunk_size: int = Field(default=1000)
    compress_output: bool = Field(default=True)

    @validator('format')
    def validate_format(cls, v):
        """Validate output format."""
        valid_formats = ['csv', 'json']
        if v not in valid_formats:
            raise ValueError(f"Format must be one of {valid_formats}")
        return v

    @validator('directory')
    def validate_directory(cls, v):
        """Validate and create output directory."""
        v.mkdir(parents=True, exist_ok=True)
        return v

class LoggingSettings(BaseModel):
    """Logging configuration settings."""
    level: str = Field(default="INFO")
    file: Path = Field(default=Path("logs/scraper.log"))
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    rotate: bool = Field(default=True)
    max_size: int = Field(default=10485760)  # 10MB
    backup_count: int = Field(default=5)
    compress: bool = Field(default=True)

    @validator('level')
    def validate_level(cls, v):
        """Validate log level."""
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @validator('file')
    def validate_log_file(cls, v):
        """Validate and create log directory."""
        v.parent.mkdir(parents=True, exist_ok=True)
        return v

class Settings:
    """Main settings class."""
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize settings from config file and environment variables."""
        self.config_file = config_file or os.getenv('CONFIG_FILE', 'config.yaml')
        self.config = self._load_config()
        
        # Initialize setting groups
        self.database = DatabaseSettings(**self.config.get('database', {}))
        self.scraper = ScraperSettings(**self.config.get('scraper', {}))
        self.output = OutputSettings(**self.config.get('output', {}))
        self.logging = LoggingSettings(**self.config.get('logging', {}))
        
        # Override with environment variables
        self._apply_env_overrides()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationError(
                f"Configuration file not found: {self.config_file}",
                details={'config_file': self.config_file}
            )
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Error parsing configuration file: {e}",
                details={'config_file': self.config_file, 'error': str(e)}
            )

    def _apply_env_overrides(self):
        """Override settings with environment variables."""
        env_mapping = {
            'SCRAPER_MODE': ('scraper', 'mode'),
            'LOG_LEVEL': ('logging', 'level'),
            'DB_HOST': ('database', 'host'),
            'DB_PORT': ('database', 'port'),
            'DB_NAME': ('database', 'name'),
            'DB_USER': ('database', 'user'),
            'DB_PASSWORD': ('database', 'password'),
            'OUTPUT_DIR': ('output', 'directory'),
        }
        
        for env_var, (section, key) in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                setattr(getattr(self, section), key, value)

    def validate(self):
        """Validate all settings."""
        try:
            self.database.validate()
            self.scraper.validate()
            self.output.validate()
            self.logging.validate()
        except ValueError as e:
            raise ConfigurationError(str(e))

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary."""
        return {
            'database': self.database.dict(),
            'scraper': self.scraper.dict(),
            'output': self.output.dict(),
            'logging': self.logging.dict()
        }

def load_settings(config_file: Optional[str] = None) -> Settings:
    """Load and validate settings."""
    settings = Settings(config_file)
    settings.validate()
    return settings

# Scrapy settings
BOT_NAME = 'business_scraper'
SPIDER_MODULES = ['scraper.spiders']
NEWSPIDER_MODULE = 'scraper.spiders'

# Crawling settings
ROBOTSTXT_OBEY = False  # We need to bypass robots.txt in some cases
COOKIES_ENABLED = False
TELNETCONSOLE_ENABLED = False

# Respect websites and avoid bans
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
CONCURRENT_REQUESTS_PER_IP = 1

# SSL/HTTPS settings
DOWNLOADER_CLIENT_TLS_METHOD = 'TLSv1.2'
DOWNLOADER_CLIENT_TLS_CIPHERS = 'DEFAULT'

# DuckDuckGo settings
DUCKDUCKGO_REGION = "us-en"
DUCKDUCKGO_SAFESEARCH = "moderate"
DUCKDUCKGO_TIMEOUT = 30
DUCKDUCKGO_MAX_RESULTS = 10
DUCKDUCKGO_BACKEND = "api"  # or "html"

# Request headers
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1'
}

# Enable or disable downloader middlewares
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
    'scraper.middlewares.RandomUserAgentMiddleware': 400,
    'scraper.middlewares.ProxyMiddleware': 350,
    'scraper.middlewares.HumanBehaviorMiddleware': 300,
}

# Configure item pipelines
ITEM_PIPELINES = {
    'scraper.pipelines.DuplicateFilterPipeline': 100,
    'scraper.pipelines.BusinessDataPipeline': 200,
}

# Proxy settings
PROXY_LIST = os.getenv('PROXY_LIST', '').split(',')

# Output settings
CSV_OUTPUT_FILE = os.getenv('CSV_OUTPUT_FILE', 'data/output/business_data.csv')
INPUT_FILE = os.getenv('INPUT_FILE', 'data/input/businesses.csv')

# Location validation
NY_STATE_CODE = "NY"
NY_COORDINATES = {
    'lat': 40.7128,
    'lon': -74.0060
}

# LLaMA model settings
LLAMA_MODEL_PATH = os.getenv('LLAMA_MODEL_PATH')

# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 408, 429]
RETRY_PRIORITY_ADJUST = -1

# Cache settings
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400  # 24 hours
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [404, 403, 429, 500, 502, 503, 504]
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Logging settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG')
LOG_FILE = os.getenv('LOG_FILE', 'logs/scraper.log')
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Additional logging settings for debugging
LOG_STDOUT = True
LOG_SHORT_NAMES = False
LOG_FILE_APPEND = True

# Performance monitoring logs
STATS_DUMP = True
STATS_CLASS = 'scrapy.statscollectors.MemoryStatsCollector'
STATS_DUMP_FILE = 'scraper_stats.json'

# Debug settings
DUPEFILTER_DEBUG = True
REDIRECT_DEBUG = True
RETRY_DEBUG = True
PROXY_DEBUG = True
ROBOTSTXT_DEBUG = True

# Error handling
HTTPERROR_ALLOWED_CODES = [404, 403]
HTTPERROR_ALLOW_ALL = False

# Monitoring settings
PROMETHEUS_ENABLED = False
PROMETHEUS_PORT = None

# Additional settings
ENSURE_DIRECTORIES = [
    'data/input',
    'data/output',
    'logs',
    '.cache'
]

# Web Interface settings
WEB_INTERFACE_ENABLED = True
WEB_INTERFACE_PORT = 8080
WEB_INTERFACE_HOST = '0.0.0.0'
MAX_UPLOAD_SIZE = 200 * 1024 * 1024  # 200MB en bytes
ALLOWED_UPLOAD_EXTENSIONS = ['.csv']
TEMP_UPLOAD_DIR = 'temp/uploads'

# Input/Output settings
CSV_OUTPUT_DIR = 'data/output'
CSV_OUTPUT_FILE = os.path.join(CSV_OUTPUT_DIR, 'business_data.csv')
TEMP_INPUT_FILE = os.path.join(TEMP_UPLOAD_DIR, 'current_input.csv')

# Additional directories to ensure
ENSURE_DIRECTORIES = [
    'temp/uploads',
    'data/output',
    'logs',
    'stats',
    '.cache'
] 