"""Common utilities for the scraper."""

import re
import logging
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse
import random
import time
from datetime import datetime
from pathlib import Path

from .constants import (
    TRUSTED_DOMAINS,
    KEYWORDS,
    USER_AGENTS,
    PATTERNS,
    VALID_STATES
)
from .exceptions import ValidationError

logger = logging.getLogger(__name__)

def setup_logging(level: str = 'INFO', log_file: Optional[str] = None) -> None:
    """Configure logging."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        logging.getLogger().addHandler(file_handler)

def clean_text(text: str) -> str:
    """Clean and normalize text."""
    if not text:
        return ""
    
    # Remove special characters and extra spaces
    text = re.sub(r'[\n\r\t]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def calculate_score(text: str, url: str, business_terms: List[str]) -> float:
    """Calcula score de relevancia."""
    try:
        text = text.lower()
        url = url.lower()

        # Puntuación base por coincidencia de términos del negocio
        name_matches = sum(1 for term in business_terms if term in text)
        base_score = min(name_matches / len(business_terms), 1.0) * 0.4

        # Puntuación por palabras clave
        keyword_score = 0.0
        for category, config in KEYWORDS.items():
            for word in config['words']:
                if word in text:
                    keyword_score += config['weight']

        # Bonus por dominios confiables
        domain_score = 0.0
        domain = urlparse(url).netloc
        for trusted_domain, weight in TRUSTED_DOMAINS.items():
            if trusted_domain in domain:
                domain_score = weight
                break

        final_score = min(base_score + keyword_score + domain_score, 1.0)
        return round(final_score, 3)

    except Exception as e:
        logger.warning(f"Error calculating score: {str(e)}")
        return 0.1

def validate_url(url: str) -> bool:
    """Valida una URL."""
    try:
        if not url.startswith(('http://', 'https://')):
            return False
            
        parsed = urlparse(url)
        return bool(parsed.netloc and parsed.scheme in ['http', 'https'])
    except Exception:
        return False

def get_random_user_agent() -> str:
    """Retorna un User-Agent aleatorio."""
    return random.choice(USER_AGENTS)

def exponential_backoff(attempt: int, base_delay: float = 30.0) -> float:
    """Calcula delay exponencial con jitter."""
    delay = base_delay * (1.5 ** attempt)
    jitter = random.uniform(-5, 5)
    return delay + jitter

def validate_state(state: str) -> bool:
    """Valida código de estado."""
    return state.upper() in VALID_STATES

def validate_zip_code(zip_code: str) -> bool:
    """Valida código postal."""
    return bool(re.match(PATTERNS['zip_code'], zip_code))

def parse_date(date_string: str) -> Optional[str]:
    """Parsea string de fecha a formato estándar."""
    try:
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None
    except Exception as e:
        logger.warning(f"Error parsing date {date_string}: {str(e)}")
        return None

def ensure_directories(paths: List[str]) -> None:
    """Asegura que los directorios existan."""
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)

def format_business_data(
    business_name: str,
    address_data: Dict[str, Any],
    source_url: str,
    confidence: float
) -> Dict[str, Any]:
    """Formatea datos de negocio."""
    return {
        'name': business_name,
        'address': address_data.get('street', ''),
        'city': address_data.get('city', ''),
        'state': address_data.get('state', ''),
        'zip_code': address_data.get('zip_code', ''),
        'confidence': confidence,
        'source_url': source_url,
        'metadata': {
            'source': 'search_result',
            'extracted_at': datetime.now().isoformat(),
            'domain': urlparse(source_url).netloc
        }
    }

def safe_request_delay() -> None:
    """Implementa delay seguro entre requests."""
    delay = random.uniform(2, 5)
    time.sleep(delay) 