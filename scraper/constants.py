"""Constantes y enums para el scraper."""

from enum import Enum, auto
from typing import Dict, List

class SearchSource(Enum):
    """Fuentes de búsqueda."""
    CHAMBER = auto()
    DUCKDUCKGO = auto()
    GOOGLE = auto()
    YELP = auto()
    BBB = auto()

class AddressSource(Enum):
    """Fuentes de direcciones."""
    SEARCH_RESULT = auto()
    WEBPAGE = auto()
    METADATA = auto()
    JSON_LD = auto()
    CHAMBER = auto()

class ValidationLevel(Enum):
    """Niveles de validación."""
    STRICT = auto()
    NORMAL = auto()
    RELAXED = auto()

# Patrones XPath para búsqueda de direcciones
ADDRESS_PATTERNS: List[str] = [
    "//div[contains(@class, 'address')]//text()",
    "//div[contains(@class, 'location')]//text()",
    "//div[contains(@class, 'contact')]//text()",
    "//div[contains(@id, 'address')]//text()",
    "//div[contains(@id, 'location')]//text()",
    "//address//text()",
    "//div[contains(@itemtype, 'PostalAddress')]//text()",
    "//*[@itemprop='address']//text()",
    "//*[@itemprop='streetAddress']//text()",
    "//*[@typeof='PostalAddress']//text()",
    "//div[contains(@class, 'business-info')]//text()",
    "//div[contains(@class, 'store-info')]//text()",
    "//footer//address//text()",
    "//div[contains(@class, 'footer')]//address//text()",
]

# Dominios confiables con pesos
TRUSTED_DOMAINS: Dict[str, float] = {
    'bbb.org': 0.9,
    'yelp.com': 0.8,
    'yellowpages.com': 0.8,
    'chamberofcommerce.com': 0.7,
    'manta.com': 0.6,
    'bizapedia.com': 0.6
}

# Palabras clave por categoría con pesos
KEYWORDS = {
    'business_indicators': {
        'weight': 0.3,
        'words': [
            'business', 'company', 'corporation', 'llc', 'inc', 
            'enterprise', 'firm', 'establishment'
        ]
    },
    'contact_info': {
        'weight': 0.2,
        'words': [
            'contact', 'phone', 'email', 'fax', 'website', 
            'hours', 'schedule', 'location'
        ]
    },
    'address_indicators': {
        'weight': 0.4,
        'words': [
            'address', 'street', 'avenue', 'boulevard', 'road',
            'suite', 'unit', 'floor', 'building'
        ]
    }
}

# User agents para rotación
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

# Patrones regex
PATTERNS = {
    'zip_code': r'^\d{5}(-\d{4})?$',
    'phone': r'^\+?1?\d{9,15}$',
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'url': r'^https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)$'
}

# Estados válidos (códigos de dos letras)
VALID_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI', 'GU', 'MP', 'AS'
}

# Configuración de timeouts y reintentos
REQUEST_TIMEOUT = 30  # segundos
MAX_RETRIES = 3
BASE_DELAY = 5  # segundos

# Límites
MAX_RESULTS_PER_QUERY = 100
MAX_CONCURRENT_REQUESTS = 5
MAX_CACHE_SIZE = 1000  # entradas
CACHE_TTL = 86400  # 24 horas en segundos

# Rutas de archivos
DEFAULT_CACHE_DIR = '.cache'
DEFAULT_LOG_DIR = 'logs'
DEFAULT_OUTPUT_DIR = 'output'
DEFAULT_CONFIG_FILE = 'config.ini'

# Configuración de logging
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
DEFAULT_LOG_LEVEL = 'INFO'

# Headers por defecto
DEFAULT_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0'
}

# Configuración de Splash
SPLASH_LUA_SCRIPT = """
function main(splash)
    splash.private_mode_enabled = true
    splash:set_user_agent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    assert(splash:go(splash.args.url))
    splash:wait(5)
    splash:evaljs("window.scrollTo(0, document.body.scrollHeight/2)")
    splash:wait(2)
    splash:wait_for_selector('address.card-text')
    return {
        html = splash:html(),
        png = splash:png(),
        har = splash:har(),
    }
end
"""

# Códigos de error HTTP para retry
RETRY_HTTP_CODES = {
    408: 'Request Timeout',
    429: 'Too Many Requests',
    500: 'Internal Server Error',
    502: 'Bad Gateway',
    503: 'Service Unavailable',
    504: 'Gateway Timeout'
} 