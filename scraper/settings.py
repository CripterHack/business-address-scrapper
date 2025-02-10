"""Configuration settings for the scraper."""

import os
from pathlib import Path
from typing import Any, Dict, Optional, List

import yaml
from pydantic import BaseModel, Field, field_validator

from scraper.config.base import BaseSettings, DatabaseSettings
from scraper.config.cache_config import CacheConfig, MetricsConfig
from scraper.exceptions import ConfigurationError
from .constants import SPLASH_LUA_SCRIPT

# Search Engine Settings
DUCKDUCKGO_REGION = "us-en"
DUCKDUCKGO_SAFESEARCH = "moderate"
DUCKDUCKGO_MAX_RESULTS = 10
DUCKDUCKGO_BACKEND = "api"

# File Paths
CSV_OUTPUT_FILE = "data/output/businesses_data.csv"
TEMP_INPUT_FILE = "data/temp/input.csv"

# Location validation
NY_STATE_CODE = "NY"
NY_COORDINATES = {"lat": 40.7128, "lon": -74.0060}

# Cache Settings
CACHE_CLEANUP_THRESHOLD = 800
MAX_CACHE_SIZE = 1000


class ScraperSettings(BaseSettings):
    """Scraper behavior settings."""

    mode: str = Field(default=os.getenv("SCRAPER_MODE", "development"))
    rate_limit: int = Field(default=1)
    max_retries: int = Field(default=int(os.getenv("MAX_RETRIES", "3")))
    chunk_size: int = Field(default=1000)
    user_agent: str = Field(default="Business Address Scraper/1.0")
    respect_robots_txt: bool = Field(default=True)
    download_delay: int = Field(default=2)
    concurrent_requests: int = Field(default=1)
    cookies_enabled: bool = Field(default=False)
    telnet_enabled: bool = Field(default=False)
    randomize_delay: bool = Field(default=True)
    tls_method: str = Field(default="TLSv1.2")
    tls_ciphers: str = Field(default="DEFAULT")
    proxy_list: List[str] = Field(default_factory=list)
    request_timeout: int = Field(default=int(os.getenv("REQUEST_TIMEOUT", "30")))
    retry_http_codes: List[int] = Field(default=[500, 502, 503, 504, 522, 524, 408, 429])
    default_headers: Dict[str, str] = Field(
        default={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
    )
    middlewares: Dict[str, Optional[int]] = Field(
        default={
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": 90,
            "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": None,
            "scraper.middlewares.RandomUserAgentMiddleware": 400,
            "scraper.middlewares.ProxyMiddleware": 350,
            "scraper.middlewares.HumanBehaviorMiddleware": 300,
        }
    )
    pipelines: Dict[str, int] = Field(
        default={
            "scraper.pipelines.DuplicateFilterPipeline": 100,
            "scraper.pipelines.BusinessDataPipeline": 200,
        }
    )
    bot_name: str = Field(default="business_scraper")
    spider_modules: List[str] = Field(default=["scraper.spiders"])
    newspider_module: str = Field(default="scraper.spiders")
    concurrent_requests_per_domain: int = Field(default=1)
    concurrent_requests_per_ip: int = Field(default=1)

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v):
        """Validate scraper mode."""
        valid_modes = ["development", "testing", "sandbox", "production"]
        if v not in valid_modes:
            raise ValueError(f"Mode must be one of {valid_modes}")
        return v

    @field_validator(
        "rate_limit",
        "max_retries",
        "chunk_size",
        "download_delay",
        "concurrent_requests",
        "request_timeout",
        "concurrent_requests_per_domain",
        "concurrent_requests_per_ip",
    )
    @classmethod
    def validate_positive_int(cls, v):
        """Validate positive integers."""
        if v < 1:
            raise ValueError("Value must be positive")
        return v

    @field_validator("retry_http_codes")
    @classmethod
    def validate_http_codes(cls, v):
        """Validate HTTP status codes."""
        if not v:
            raise ValueError("At least one retry HTTP code must be specified")
        for code in v:
            if not isinstance(code, int) or code < 100 or code > 599:
                raise ValueError(f"Invalid HTTP status code: {code}")
        return v

    @field_validator("middlewares")
    @classmethod
    def validate_middlewares(cls, v):
        """Validate middleware configuration."""
        if not v:
            raise ValueError("At least one middleware must be specified")
        for path, priority in v.items():
            if not isinstance(path, str) or not path:
                raise ValueError(f"Invalid middleware path: {path}")
            if priority is not None and (not isinstance(priority, int) or priority < 0):
                raise ValueError(f"Invalid middleware priority for {path}: {priority}")
        return v

    @field_validator("pipelines")
    @classmethod
    def validate_pipelines(cls, v):
        """Validate pipeline configuration."""
        if not v:
            raise ValueError("At least one pipeline must be specified")
        for path, priority in v.items():
            if not isinstance(path, str) or not path:
                raise ValueError(f"Invalid pipeline path: {path}")
            if not isinstance(priority, int) or priority < 0:
                raise ValueError(f"Invalid pipeline priority for {path}: {priority}")
        return v

    @field_validator("default_headers")
    @classmethod
    def validate_headers(cls, v):
        """Validate HTTP headers."""
        if not v:
            raise ValueError("At least one header must be specified")
        for name, value in v.items():
            if not isinstance(name, str) or not name:
                raise ValueError(f"Invalid header name: {name}")
            if not isinstance(value, str):
                raise ValueError(f"Invalid header value for {name}: {value}")
        return v

    @field_validator("spider_modules")
    @classmethod
    def validate_spider_modules(cls, v):
        """Validate spider modules."""
        if not v:
            raise ValueError("At least one spider module must be specified")
        for module in v:
            if not isinstance(module, str) or not module:
                raise ValueError(f"Invalid spider module: {module}")
        return v


class OutputSettings(BaseModel):
    """Output configuration settings."""

    format: str = Field(default="csv")
    directory: Path = Field(default=Path("data/output"))
    filename_pattern: str = Field(default="business_data_{timestamp}.csv")
    chunk_enabled: bool = Field(default=True)
    chunk_size: int = Field(default=1000)
    compress_output: bool = Field(default=True)
    max_file_size: int = Field(default=104857600)  # 100MB
    backup_count: int = Field(default=5)

    @field_validator("format")
    @classmethod
    def validate_format(cls, v):
        """Validate output format."""
        valid_formats = ["csv", "json"]
        if v not in valid_formats:
            raise ValueError(f"Format must be one of {valid_formats}")
        return v

    @field_validator("directory")
    @classmethod
    def validate_directory(cls, v):
        """Validate and create output directory."""
        v.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("max_file_size")
    @classmethod
    def validate_max_file_size(cls, v):
        """Validate max file size."""
        if v < 1024:  # Mínimo 1KB
            raise ValueError("Max file size must be at least 1KB")
        return v

    @field_validator("chunk_size")
    @classmethod
    def validate_chunk_size(cls, v):
        """Validate chunk size."""
        if v < 1:
            raise ValueError("Chunk size must be at least 1")
        return v


class LoggingSettings(BaseModel):
    """Logging configuration settings."""

    level: str = Field(default="INFO")
    file: Path = Field(default=Path("logs/scraper.log"))
    format: str = Field(default="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    date_format: str = Field(default="%Y-%m-%d %H:%M:%S")
    rotate: bool = Field(default=True)
    max_size: int = Field(default=10485760)  # 10MB
    backup_count: int = Field(default=5)
    compress: bool = Field(default=True)
    console_output: bool = Field(default=True)
    json_format: bool = Field(default=True)
    include_extra_fields: bool = Field(default=True)

    @field_validator("level")
    @classmethod
    def validate_level(cls, v):
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()

    @field_validator("file")
    @classmethod
    def validate_log_file(cls, v):
        """Validate and create log directory."""
        v.parent.mkdir(parents=True, exist_ok=True)
        return v

    @field_validator("max_size")
    @classmethod
    def validate_max_size(cls, v):
        """Validate max file size."""
        if v < 1024:  # Mínimo 1KB
            raise ValueError("Max size must be at least 1KB")
        return v

    @field_validator("backup_count")
    @classmethod
    def validate_backup_count(cls, v):
        """Validate backup count."""
        if v < 1:
            raise ValueError("Backup count must be at least 1")
        return v


class DirectorySettings(BaseModel):
    """Directory configuration settings."""

    base_dir: Path = Field(default=Path("."))
    data_dir: Path = Field(default=Path("data"))
    logs_dir: Path = Field(default=Path("logs"))
    cache_dir: Path = Field(default=Path(".cache"))
    temp_dir: Path = Field(default=Path("temp"))
    stats_dir: Path = Field(default=Path("stats"))
    backup_dir: Path = Field(default=Path("backup"))

    @field_validator("*")
    @classmethod
    def validate_directory(cls, v: Path) -> Path:
        """Validate and create directory."""
        v.mkdir(parents=True, exist_ok=True)
        return v


class SearchSettings(BaseModel):
    """Search engine settings."""

    region: str = Field(default="us-en")
    safesearch: str = Field(default="moderate")
    timeout: int = Field(default=30)
    max_results: int = Field(default=10)
    backend: str = Field(default="api")
    model_path: Optional[str] = Field(default=None)

    @field_validator("safesearch")
    @classmethod
    def validate_safesearch(cls, v):
        """Validate safesearch setting."""
        valid_options = ["strict", "moderate", "off"]
        if v not in valid_options:
            raise ValueError(f"Safesearch must be one of {valid_options}")
        return v

    @field_validator("backend")
    @classmethod
    def validate_backend(cls, v):
        """Validate backend setting."""
        valid_backends = ["api", "html"]
        if v not in valid_backends:
            raise ValueError(f"Backend must be one of {valid_backends}")
        return v


class Settings:
    """Main settings class."""

    def __init__(self, config_file: Optional[str] = None):
        """Initialize settings from config file and environment variables."""
        self.config_file = config_file or os.getenv("CONFIG_FILE", "config.yaml")
        self.config = self._load_config()

        # Initialize setting groups
        self.database = DatabaseSettings(**self.config.get("database", {}))
        self.scraper = ScraperSettings(**self.config.get("scraper", {}))
        self.output = OutputSettings(**self.config.get("output", {}))
        self.logging = LoggingSettings(**self.config.get("logging", {}))
        self.directories = DirectorySettings(**self.config.get("directories", {}))
        self.cache = CacheConfig(**self.config.get("cache", {}))
        self.metrics = MetricsConfig(**self.config.get("metrics", {}))
        self.search = SearchSettings(**self.config.get("search", {}))

        # Override with environment variables
        self._apply_env_overrides()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationError(
                f"Configuration file not found: {self.config_file}",
                details={"config_file": self.config_file},
            )
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Error parsing configuration file: {e}",
                details={"config_file": self.config_file, "error": str(e)},
            )

    def _apply_env_overrides(self):
        """Override settings with environment variables."""
        env_mapping = {
            "SCRAPER_MODE": ("scraper", "mode"),
            "LOG_LEVEL": ("logging", "level"),
            "DB_HOST": ("database", "host"),
            "DB_PORT": ("database", "port"),
            "DB_NAME": ("database", "name"),
            "DB_USER": ("database", "user"),
            "DB_PASSWORD": ("database", "password"),
            "OUTPUT_DIR": ("output", "directory"),
            "CACHE_TYPE": ("cache", "type"),
            "METRICS_ENABLED": ("metrics", "enabled"),
            "BASE_DIR": ("directories", "base_dir"),
        }

        for env_var, (section, key) in env_mapping.items():
            value = os.getenv(env_var)
            if value is not None:
                section_obj = getattr(self, section)
                if isinstance(value, str) and hasattr(section_obj, key):
                    field_type = type(getattr(section_obj, key))
                    try:
                        if field_type == Path:
                            value = Path(value)
                        elif field_type == bool:
                            value = value.lower() in ("true", "1", "yes")
                        elif field_type == int:
                            value = int(value)
                        setattr(section_obj, key, value)
                    except (ValueError, TypeError):
                        continue

    def validate(self):
        """Validate all settings."""
        try:
            self.database.validate()
            self.scraper.validate()
            self.output.validate()
            self.logging.validate()
            self.directories.validate()
            self.cache.validate()
            self.metrics.validate()
            self.search.validate()
        except ValueError as e:
            raise ConfigurationError(str(e))

    def to_dict(self) -> Dict[str, Any]:
        """Convert settings to dictionary."""
        return {
            "database": self.database.dict(),
            "scraper": self.scraper.dict(),
            "output": self.output.dict(),
            "logging": self.logging.dict(),
            "directories": self.directories.dict(),
            "cache": self.cache.dict(),
            "metrics": self.metrics.dict(),
            "search": self.search.dict(),
        }


def load_settings(config_file: Optional[str] = None) -> Settings:
    """Load and validate settings."""
    settings = Settings(config_file)
    settings.validate()
    return settings
