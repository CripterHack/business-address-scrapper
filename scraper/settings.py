import os
from dotenv import load_dotenv

load_dotenv()

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

# Qwant API settings
QWANT_API_URL = "https://api.qwant.com/v3/search/web"
QWANT_API_KEY = os.getenv('QWANT_API_KEY')

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
CSV_OUTPUT_FILE = os.getenv('CSV_OUTPUT_FILE', 'business_data.csv')
INPUT_FILE = os.getenv('INPUT_FILE', 'businesses.csv')

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
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'scraper.log')
LOG_ENCODING = 'utf-8'
LOG_FORMAT = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
LOG_DATEFORMAT = '%Y-%m-%d %H:%M:%S'

# Error handling
HTTPERROR_ALLOWED_CODES = [404, 403]  # Handle these errors in spider
HTTPERROR_ALLOW_ALL = False 