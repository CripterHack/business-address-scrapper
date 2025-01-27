import logging
from typing import Any, Dict, List, Optional
import json
import os
from datetime import datetime
import hashlib
from urllib.parse import urlparse
import tldextract
from scraper.exceptions import ConfigurationError, StorageError

def setup_logging(log_file: str = None, log_level: str = 'INFO') -> None:
    """Configure logging settings"""
    log_format = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=handlers
    )

def load_config(config_file: str = '.env') -> Dict[str, str]:
    """Load configuration from environment file"""
    if not os.path.exists(config_file):
        raise ConfigurationError(f"Configuration file not found: {config_file}")
    
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip().strip('"\'')
        return config
    except Exception as e:
        raise ConfigurationError(f"Error loading configuration: {str(e)}")

def generate_cache_key(url: str, params: Dict[str, Any] = None) -> str:
    """Generate a unique cache key for a URL and parameters"""
    key_parts = [url]
    if params:
        key_parts.append(json.dumps(params, sort_keys=True))
    
    key_string = '|'.join(key_parts)
    return hashlib.md5(key_string.encode()).hexdigest()

def is_valid_domain(url: str, allowed_domains: List[str]) -> bool:
    """Check if URL domain is in allowed list"""
    try:
        ext = tldextract.extract(url)
        domain = f"{ext.domain}.{ext.suffix}"
        return any(allowed in domain for allowed in allowed_domains)
    except Exception:
        return False

def save_to_json(data: Any, filename: str) -> None:
    """Save data to JSON file with error handling"""
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        raise StorageError(f"Error saving to JSON: {str(e)}")

def load_from_json(filename: str) -> Any:
    """Load data from JSON file with error handling"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        raise StorageError(f"Error loading from JSON: {str(e)}")

def format_date(date_str: str, input_format: str = '%Y-%m-%d', output_format: str = '%Y-%m-%d') -> Optional[str]:
    """Format date string with error handling"""
    try:
        if not date_str:
            return None
        date_obj = datetime.strptime(date_str, input_format)
        return date_obj.strftime(output_format)
    except ValueError:
        return None

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ''
    return ' '.join(text.split()).strip()

def extract_domain(url: str) -> str:
    """Extract domain from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc
    except Exception:
        return ''

def create_directory(path: str) -> None:
    """Create directory if it doesn't exist"""
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        raise StorageError(f"Error creating directory: {str(e)}")

def get_file_size(file_path: str) -> int:
    """Get file size in bytes"""
    try:
        return os.path.getsize(file_path)
    except Exception:
        return 0

def is_file_empty(file_path: str) -> bool:
    """Check if file is empty"""
    return get_file_size(file_path) == 0

def backup_file(file_path: str) -> str:
    """Create a backup of a file"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"{file_path}.{timestamp}.bak"
        if os.path.exists(file_path):
            with open(file_path, 'rb') as src, open(backup_path, 'wb') as dst:
                dst.write(src.read())
        return backup_path
    except Exception as e:
        raise StorageError(f"Error creating backup: {str(e)}")

def restore_backup(backup_path: str, original_path: str) -> None:
    """Restore file from backup"""
    try:
        with open(backup_path, 'rb') as src, open(original_path, 'wb') as dst:
            dst.write(src.read())
    except Exception as e:
        raise StorageError(f"Error restoring backup: {str(e)}") 