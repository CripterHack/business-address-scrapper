"""Validation system for the scraper."""

import re
from typing import Dict, Any, Optional, List, Tuple, Union
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse
import logging

from .exceptions import ValidationError
from .constants import VALID_STATES, PATTERNS

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Validation result structure."""
    is_valid: bool
    error_message: Optional[str] = None
    warnings: List[str] = None
    details: Dict[str, Any] = None
    confidence_score: float = 0.0

    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.details is None:
            self.details = {}

@dataclass
class Address:
    """Address data structure."""
    street: str
    city: str
    state: str
    zip_code: str
    is_valid: bool = False
    validation_message: str = ""
    confidence_score: float = 0.0
    source: str = ""
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

class BaseValidator:
    """Base validator class."""
    
    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        logger.error(f"Validation error: {message}")
        
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")
        
    def get_result(self) -> ValidationResult:
        """Get validation result."""
        return ValidationResult(
            is_valid=len(self.errors) == 0,
            error_message='; '.join(self.errors) if self.errors else None,
            warnings=self.warnings,
            confidence_score=1.0 if len(self.errors) == 0 else 0.0
        )
        
    def clear(self) -> None:
        """Clear errors and warnings."""
        self.errors.clear()
        self.warnings.clear()

class AddressValidator(BaseValidator):
    """Address validator with improved validation."""
    
    # Invalid address patterns
    INVALID_PATTERNS = {
        r'^\s*$',  # Empty or whitespace only
        r'^[0-9\s]*$',  # Numbers only
        r'^unknown$',  # Unknown
        r'^test',  # Test addresses
        r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$',  # Emails
        r'^https?://',  # URLs
        r'^[0-9]{3}-[0-9]{3}-[0-9]{4}$',  # Phone numbers
        r'^P\.?O\.?\s*Box',  # P.O. Box
        r'^Private\s+Mailbox',  # Private Mailbox
        r'^General\s+Delivery',  # General Delivery
    }
    
    # Valid street types
    VALID_STREET_TYPES = {
        'street', 'avenue', 'road', 'boulevard', 'lane', 'drive',
        'way', 'court', 'circle', 'plaza', 'square', 'parkway',
        'st', 'ave', 'rd', 'blvd', 'ln', 'dr', 'ct', 'cir', 'plz', 'sq'
    }
    
    # Valid cardinal directions
    VALID_DIRECTIONS = {
        'n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw',
        'north', 'south', 'east', 'west',
        'northeast', 'northwest', 'southeast', 'southwest'
    }

    # Common abbreviations mapping
    STREET_TYPE_MAPPING = {
        'st': 'street',
        'ave': 'avenue',
        'rd': 'road',
        'blvd': 'boulevard',
        'ln': 'lane',
        'dr': 'drive',
        'ct': 'court',
        'cir': 'circle',
        'plz': 'plaza',
        'sq': 'square',
        'pkwy': 'parkway'
    }

    DIRECTION_MAPPING = {
        'n': 'north',
        's': 'south',
        'e': 'east',
        'w': 'west',
        'ne': 'northeast',
        'nw': 'northwest',
        'se': 'southeast',
        'sw': 'southwest'
    }
    
    def validate(self, address: Union[str, Dict[str, Any]]) -> ValidationResult:
        """Validate an address.
        
        Args:
            address: Address string or dictionary with address components
            
        Returns:
            ValidationResult: Validation result
        """
        self.clear()
        
        if isinstance(address, str):
            return self._validate_address_string(address)
        elif isinstance(address, dict):
            return self._validate_address_dict(address)
        else:
            self.add_error("Invalid address format")
            return self.get_result()
    
    def _validate_address_string(self, address: str) -> ValidationResult:
        """Validate a string address.
        
        Args:
            address: Address string to validate
            
        Returns:
            ValidationResult: Validation result
        """
        # Validate invalid patterns
        for pattern in self.INVALID_PATTERNS:
            if re.match(pattern, address, re.IGNORECASE):
                self.add_error("Invalid address format")
                return self.get_result()
        
        # Validate length
        if len(address) < 5:
            self.add_error("Address too short")
            return self.get_result()
        
        # Validate numbers
        if not any(c.isdigit() for c in address):
            self.add_error("Address must contain at least one number")
            return self.get_result()
        
        # Parse and validate components
        components = self._parse_address(address)
        return self._validate_components(components)
    
    def _validate_address_dict(self, address: Dict[str, Any]) -> ValidationResult:
        """Validate an address dictionary.
        
        Args:
            address: Dictionary with address components
            
        Returns:
            ValidationResult: Validation result
        """
        required_fields = ['street', 'city', 'state', 'zip_code']
        
        # Check required fields
        for field in required_fields:
            if field not in address:
                self.add_error(f"Missing required field: {field}")
                return self.get_result()
        
        # Validate street
        if not self._validate_street(address['street']):
            self.add_error("Invalid street format")
        
        # Validate city
        if len(address['city']) < 2:
            self.add_error("City name too short")
        elif not all(c.isalpha() or c.isspace() or c in "'-." for c in address['city']):
            self.add_error("City contains invalid characters")
        
        # Validate state
        if address['state'] not in VALID_STATES:
            self.add_error("Invalid state code")
        
        # Validate ZIP code
        if not re.match(PATTERNS['zip_code'], address['zip_code']):
            self.add_error("Invalid ZIP code format")
        
        return self.get_result()
    
    def _parse_address(self, address: str) -> Dict[str, str]:
        """Parse an address into its components.
        
        Args:
            address: Address string to parse
            
        Returns:
            Dict[str, str]: Dictionary with address components
        """
        components = {}
        parts = [p.strip() for p in address.split(',')]
        
        if len(parts) >= 1:
            components['street'] = parts[0]
        
        if len(parts) >= 2:
            location_parts = parts[-1].strip().split()
            
            if len(location_parts) >= 2:
                # Extract ZIP code
                zip_match = re.search(PATTERNS['zip_code'], location_parts[-1])
                if zip_match:
                    components['zip_code'] = zip_match.group()
                    location_parts.pop()
                
                # Extract state
                if location_parts and location_parts[-1].upper() in VALID_STATES:
                    components['state'] = location_parts[-1].upper()
                    location_parts.pop()
                
                # Remaining parts form the city
                if location_parts:
                    components['city'] = ' '.join(location_parts)
            
            if len(parts) >= 3 and not components.get('city'):
                components['city'] = parts[1]
        
        return components

    def _validate_components(self, components: Dict[str, str]) -> ValidationResult:
        """Validate address components.
        
        Args:
            components: Dictionary with address components
            
        Returns:
            ValidationResult: Validation result
        """
        # Validate street
        if not components.get('street'):
            self.add_error("Missing street")
        elif not self._validate_street(components['street']):
            self.add_error("Invalid street format")
        
        # Validate city
        if not components.get('city'):
            self.add_error("Missing city")
        elif len(components['city']) < 2:
            self.add_error("City name too short")
        elif not all(c.isalpha() or c.isspace() or c in "'-." for c in components['city']):
            self.add_error("City contains invalid characters")
        
        # Validate state
        if not components.get('state'):
            self.add_error("Missing state")
        elif components['state'] not in VALID_STATES:
            self.add_error("Invalid state code")
        
        # Validate ZIP code
        if not components.get('zip_code'):
            self.add_error("Missing ZIP code")
        elif not re.match(PATTERNS['zip_code'], components['zip_code']):
            self.add_error("Invalid ZIP code format")
        
        return self.get_result()
    
    def _validate_street(self, street: str) -> bool:
        """Validate street format.
        
        Args:
            street: Street string to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        if not street:
            return False

        words = street.lower().split()
        
        # Check minimum length
        if len(words) < 2:
            return False

        # Check for number at start
        if not re.match(r'^\d+', words[0]):
            return False

        # Check for valid street type
        has_valid_type = False
        for word in words:
            # Check original word and its mapping
            if word in self.VALID_STREET_TYPES or self.STREET_TYPE_MAPPING.get(word) in self.VALID_STREET_TYPES:
                has_valid_type = True
                break
        
        if not has_valid_type:
            return False

        # Check for cardinal direction if present
        if len(words) > 2:
            for word in words[1:-1]:  # Exclude initial number and final street type
                if word in self.VALID_DIRECTIONS or self.DIRECTION_MAPPING.get(word) in self.VALID_DIRECTIONS:
                    return True

        return True

    def validate_state(self, state: str, target_state: str) -> ValidationResult:
        """Validate state against target state.
        
        Args:
            state: State to validate
            target_state: Target state to compare against
            
        Returns:
            ValidationResult: Validation result
        """
        if not state:
            return ValidationResult(
                is_valid=False,
                error_message="State is required",
                confidence_score=0.0
            )
            
        state = state.upper()
        target_state = target_state.upper()
        
        if state not in VALID_STATES:
            return ValidationResult(
                is_valid=False,
                error_message=f"Invalid state code: {state}",
                confidence_score=0.0
            )
            
        if state != target_state:
            return ValidationResult(
                is_valid=False,
                error_message=f"State {state} does not match target state {target_state}",
                confidence_score=0.0
            )
            
        return ValidationResult(
            is_valid=True,
            confidence_score=1.0
        )

class BusinessValidator(BaseValidator):
    """Business data validator."""

    # Common business name patterns to avoid
    BUSINESS_NAME_BLACKLIST = {
        'test', 'example', 'demo', 'sample', 'dummy', 'fake',
        'unknown', 'undefined', 'null', 'none', 'n/a', 'na'
    }

    # Business name keywords requiring additional validation
    BUSINESS_NAME_KEYWORDS = {
        'inc', 'llc', 'ltd', 'corp', 'corporation', 'company', 'co',
        'incorporated', 'limited', 'partnership', 'associates', 'consulting'
    }

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Valida datos de negocio."""
        self.clear()
        
        # Validar campos requeridos
        required_fields = ['business_name', 'address']
        for field in required_fields:
            if field not in data:
                self.add_error(f"Missing required field: {field}")
                return self.get_result()
        
        # Validar nombre del negocio
        if not self._validate_business_name(data['business_name']):
            return self.get_result()
        
        # Validar dirección
        address_validator = AddressValidator()
        address_result = address_validator.validate(data['address'])
        
        if not address_result.is_valid:
            self.errors.extend(address_result.warnings)
            self.warnings.extend(address_result.warnings)
        
        # Validar URL si existe
        if 'url' in data and data['url']:
            if not self._validate_url(data['url']):
                self.add_error("Invalid URL format")
        
        # Validar fechas si existen
        if 'created_at' in data and data['created_at']:
            if not self._validate_date(data['created_at']):
                self.add_error("Invalid created_at date format")
        
        return self.get_result()
    
    def _validate_business_name(self, name: str) -> bool:
        """Valida el nombre del negocio."""
        # Validar longitud
        if len(name) < 2:
            self.add_error("Business name too short")
            return False
        
        # Validar palabras en lista negra
        name_lower = name.lower()
        for blacklisted in self.BUSINESS_NAME_BLACKLIST:
            if blacklisted in name_lower:
                self.add_error(f"Business name contains invalid term: {blacklisted}")
                return False
        
        # Validar caracteres especiales excesivos
        special_chars = sum(1 for c in name if not c.isalnum() and not c.isspace())
        if special_chars > len(name) * 0.3:
            self.add_error("Business name contains too many special characters")
            return False
        
        # Verificar palabras clave
        words = set(name_lower.split())
        business_keywords = words.intersection(self.BUSINESS_NAME_KEYWORDS)
        
        if business_keywords:
            if 'inc' in words and not name.endswith(('Inc.', 'Inc', 'Incorporated')):
                self.add_warning('Inconsistent Inc. formatting')
            if 'llc' in words and not name.endswith(('LLC', 'L.L.C.')):
                self.add_warning('Inconsistent LLC formatting')
        
        return True
    
    def _validate_url(self, url: str) -> bool:
        """Valida una URL."""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
    
    def _validate_date(self, date_str: str) -> bool:
        """Valida una fecha."""
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False

class URLValidator(BaseValidator):
    """URL validator with improved validation."""
    
    def validate(self, url: str) -> ValidationResult:
        """Validate a URL."""
        self.clear()
        
        if not url:
            self.add_error("URL cannot be empty")
            return self.get_result()
        
        try:
            result = urlparse(url)
            
            if not all([result.scheme, result.netloc]):
                self.add_error("Invalid URL format")
                return self.get_result()
            
            if result.scheme not in ['http', 'https']:
                self.add_error("URL must use HTTP or HTTPS protocol")
                return self.get_result()
            
            if len(url) > 2048:
                self.add_warning("URL is unusually long")
            
            return self.get_result()
            
        except Exception as e:
            self.add_error(f"Error parsing URL: {str(e)}")
            return self.get_result()

class SearchResultValidator(BaseValidator):
    """Search result validator with improved validation."""
    
    def validate(self, result: Dict[str, Any]) -> ValidationResult:
        """Validate a search result."""
        self.clear()
        
        # Validate required fields
        required_fields = ['url', 'title']
        for field in required_fields:
            if field not in result:
                self.add_error(f"Missing required field: {field}")
                return self.get_result()
        
        # Validate URL
        url_validator = URLValidator()
        url_result = url_validator.validate(result['url'])
        
        if not url_result.is_valid:
            self.errors.extend(url_result.errors)
            self.warnings.extend(url_result.warnings)
        
        # Validate title
        if len(result['title']) < 3:
            self.add_error("Title too short")
        
        # Validate score if exists
        if 'relevance_score' in result:
            score = result['relevance_score']
            if not isinstance(score, (int, float)):
                self.add_error("Invalid relevance score type")
            elif not 0 <= score <= 1:
                self.add_error("Relevance score must be between 0 and 1")
        
        return self.get_result() 