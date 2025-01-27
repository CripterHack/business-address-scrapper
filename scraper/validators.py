import re
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from scraper.exceptions import AddressValidationError

@dataclass
class Address:
    street: str
    city: str
    state: str
    zip_code: str
    is_valid: bool = False
    validation_message: str = ""

class AddressValidator:
    # NY ZIP code ranges
    NY_ZIP_RANGES = [
        (10001, 10292),  # Manhattan
        (10301, 10314),  # Staten Island
        (10451, 10475),  # Bronx
        (11004, 11109),  # Queens
        (11351, 11697),  # Queens
        (11201, 11256),  # Brooklyn
        (12007, 14925),  # Rest of NY State
    ]

    # NY Cities (partial list of major cities)
    NY_CITIES = {
        'NEW YORK', 'BROOKLYN', 'BRONX', 'STATEN ISLAND', 'QUEENS',
        'BUFFALO', 'ROCHESTER', 'YONKERS', 'SYRACUSE', 'ALBANY',
        'NEW ROCHELLE', 'MOUNT VERNON', 'SCHENECTADY', 'UTICA',
        'WHITE PLAINS', 'HEMPSTEAD', 'TROY', 'NIAGARA FALLS',
        'BINGHAMTON', 'FREEPORT', 'VALLEY STREAM'
    }

    @classmethod
    def validate_address(cls, address_text: str) -> Address:
        """Validate and parse a full address string"""
        try:
            # Clean and standardize the address
            clean_address = cls._clean_address(address_text)
            
            # Parse address components
            components = cls._parse_address(clean_address)
            
            # Validate each component
            is_valid, message = cls._validate_components(components)
            
            return Address(
                street=components.get('street', ''),
                city=components.get('city', ''),
                state=components.get('state', ''),
                zip_code=components.get('zip_code', ''),
                is_valid=is_valid,
                validation_message=message
            )
        except Exception as e:
            raise AddressValidationError(f"Error validating address: {str(e)}")

    @staticmethod
    def _clean_address(address: str) -> str:
        """Clean and standardize address string"""
        # Convert to uppercase for consistency
        address = address.upper()
        
        # Remove special characters except comma and dash
        address = re.sub(r'[^\w\s,-]', '', address)
        
        # Standardize whitespace
        address = ' '.join(address.split())
        
        return address

    @classmethod
    def _parse_address(cls, address: str) -> Dict[str, str]:
        """Parse address string into components"""
        components = {}
        
        # Split by comma
        parts = [p.strip() for p in address.split(',')]
        
        if len(parts) >= 1:
            components['street'] = parts[0]
        
        if len(parts) >= 2:
            # Handle city, state zip
            location_parts = parts[-1].strip().split()
            
            if len(location_parts) >= 2:
                # Last part should be ZIP code
                components['zip_code'] = location_parts[-1]
                
                # Second to last part should be state
                components['state'] = location_parts[-2]
                
                # Everything else is city
                components['city'] = ' '.join(location_parts[:-2])
            
            # If there's a middle part, it's probably the city
            if len(parts) >= 3 and not components.get('city'):
                components['city'] = parts[1]
        
        return components

    @classmethod
    def _validate_components(cls, components: Dict[str, str]) -> Tuple[bool, str]:
        """Validate address components"""
        messages = []
        
        # Validate state
        if components.get('state') != 'NY':
            messages.append("Address must be in New York state")
        
        # Validate ZIP code
        zip_code = components.get('zip_code', '')
        if not cls._is_valid_ny_zip(zip_code):
            messages.append(f"Invalid NY ZIP code: {zip_code}")
        
        # Validate city
        city = components.get('city', '').upper()
        if city and city not in cls.NY_CITIES:
            messages.append(f"City not recognized as major NY city: {city}")
        
        # Validate street address
        street = components.get('street', '')
        if not cls._is_valid_street(street):
            messages.append("Invalid street address format")
        
        is_valid = len(messages) == 0
        message = '; '.join(messages) if messages else "Address validation successful"
        
        return is_valid, message

    @classmethod
    def _is_valid_ny_zip(cls, zip_code: str) -> bool:
        """Validate if ZIP code is within NY ranges"""
        try:
            if not zip_code or not zip_code.isdigit() or len(zip_code) != 5:
                return False
            
            zip_int = int(zip_code)
            return any(start <= zip_int <= end for start, end in cls.NY_ZIP_RANGES)
        except ValueError:
            return False

    @staticmethod
    def _is_valid_street(street: str) -> bool:
        """Validate street address format"""
        if not street:
            return False
        
        # Check for number at start of street address
        has_number = bool(re.match(r'^\d+', street))
        
        # Check for common street types
        street_types = r'\b(STREET|ST|AVENUE|AVE|ROAD|RD|BOULEVARD|BLVD|LANE|LN|DRIVE|DR|COURT|CT|CIRCLE|CIR|PLACE|PL|SQUARE|SQ)\b'
        has_street_type = bool(re.search(street_types, street))
        
        return has_number and has_street_type 