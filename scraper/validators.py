import re
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from scraper.exceptions import AddressValidationError
from datetime import datetime
from .exceptions import ValidationError

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

class DataValidator:
    """Base class for data validation."""
    
    @staticmethod
    def validate_string(value: str, field_name: str, min_length: int = 1, max_length: int = None) -> str:
        """Validate string fields."""
        if not isinstance(value, str):
            raise ValidationError(
                f"{field_name} must be a string",
                details={'field': field_name, 'value': value, 'type': type(value).__name__}
            )
        
        if len(value.strip()) < min_length:
            raise ValidationError(
                f"{field_name} must be at least {min_length} characters long",
                details={'field': field_name, 'value': value, 'min_length': min_length}
            )
        
        if max_length and len(value) > max_length:
            raise ValidationError(
                f"{field_name} must be no more than {max_length} characters long",
                details={'field': field_name, 'value': value, 'max_length': max_length}
            )
        
        return value.strip()

    @staticmethod
    def validate_date(value: str, field_name: str, allow_future: bool = False) -> datetime:
        """Validate date strings."""
        try:
            date = datetime.strptime(value, '%Y-%m-%d')
            
            if not allow_future and date > datetime.now():
                raise ValidationError(
                    f"{field_name} cannot be in the future",
                    details={'field': field_name, 'value': value}
                )
            
            return date
            
        except ValueError as e:
            raise ValidationError(
                f"Invalid date format for {field_name}. Expected YYYY-MM-DD",
                details={'field': field_name, 'value': value, 'error': str(e)}
            )

    @staticmethod
    def validate_boolean(value: Any, field_name: str) -> bool:
        """Validate boolean fields."""
        if isinstance(value, bool):
            return value
        
        if isinstance(value, str):
            value = value.lower()
            if value in ('true', '1', 'yes', 'on'):
                return True
            if value in ('false', '0', 'no', 'off'):
                return False
        
        raise ValidationError(
            f"{field_name} must be a boolean value",
            details={'field': field_name, 'value': value, 'type': type(value).__name__}
        )

class BusinessValidator(DataValidator):
    """Validator for business data."""
    
    STATE_CODES = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC', 'PR', 'VI', 'GU', 'MP', 'AS'
    }

    ZIP_CODE_PATTERN = re.compile(r'^\d{5}(-\d{4})?$')
    PHONE_PATTERN = re.compile(r'^\+?1?\d{10}$|^\+?1-\d{3}-\d{3}-\d{4}$')
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')

    def validate_business_name(self, name: str) -> str:
        """Validate business name."""
        return self.validate_string(name, 'business_name', min_length=2, max_length=200)

    def validate_address(self, address: str) -> str:
        """Validate street address."""
        return self.validate_string(address, 'address', min_length=5, max_length=200)

    def validate_city(self, city: str) -> str:
        """Validate city name."""
        return self.validate_string(city, 'city', min_length=2, max_length=100)

    def validate_state(self, state: str) -> str:
        """Validate state code."""
        state = self.validate_string(state, 'state', min_length=2, max_length=2)
        if state not in self.STATE_CODES:
            raise ValidationError(
                "Invalid state code",
                details={'field': 'state', 'value': state, 'valid_codes': sorted(self.STATE_CODES)}
            )
        return state

    def validate_zip_code(self, zip_code: str) -> str:
        """Validate ZIP code."""
        zip_code = self.validate_string(zip_code, 'zip_code', min_length=5)
        if not self.ZIP_CODE_PATTERN.match(zip_code):
            raise ValidationError(
                "Invalid ZIP code format",
                details={'field': 'zip_code', 'value': zip_code}
            )
        return zip_code

    def validate_phone(self, phone: Optional[str]) -> Optional[str]:
        """Validate phone number."""
        if phone is None:
            return None
            
        phone = self.validate_string(phone, 'phone')
        if not self.PHONE_PATTERN.match(phone):
            raise ValidationError(
                "Invalid phone number format",
                details={'field': 'phone', 'value': phone}
            )
        return phone

    def validate_email(self, email: Optional[str]) -> Optional[str]:
        """Validate email address."""
        if email is None:
            return None
            
        email = self.validate_string(email, 'email')
        if not self.EMAIL_PATTERN.match(email):
            raise ValidationError(
                "Invalid email format",
                details={'field': 'email', 'value': email}
            )
        return email

    def validate_violation_type(self, violation_type: str) -> str:
        """Validate violation type."""
        return self.validate_string(violation_type, 'violation_type', min_length=5, max_length=100)

    def validate_dates(self, data: Dict[str, str]) -> Dict[str, datetime]:
        """Validate all date fields."""
        dates = {}
        
        # Published date must be in the past
        dates['nsl_published_date'] = self.validate_date(
            data['nsl_published_date'], 
            'nsl_published_date'
        )
        
        # Effective date can be in the future
        dates['nsl_effective_date'] = self.validate_date(
            data['nsl_effective_date'], 
            'nsl_effective_date',
            allow_future=True
        )
        
        # Remediated date is optional and must be between published and now
        if 'remediated_date' in data and data['remediated_date']:
            remediated_date = self.validate_date(
                data['remediated_date'],
                'remediated_date'
            )
            
            if remediated_date < dates['nsl_published_date']:
                raise ValidationError(
                    "Remediated date cannot be before published date",
                    details={
                        'remediated_date': data['remediated_date'],
                        'published_date': data['nsl_published_date']
                    }
                )
                
            dates['remediated_date'] = remediated_date
        else:
            dates['remediated_date'] = None
        
        return dates

    def validate_all(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate all business data fields."""
        validated = {}
        
        # Required string fields
        validated['business_name'] = self.validate_business_name(data['business_name'])
        validated['address'] = self.validate_address(data['address'])
        validated['city'] = self.validate_city(data['city'])
        validated['state'] = self.validate_state(data['state'])
        validated['zip_code'] = self.validate_zip_code(data['zip_code'])
        validated['violation_type'] = self.validate_violation_type(data['violation_type'])
        
        # Optional fields
        validated['phone'] = self.validate_phone(data.get('phone'))
        validated['email'] = self.validate_email(data.get('email'))
        
        # Date fields
        validated.update(self.validate_dates(data))
        
        # Boolean fields
        validated['verified'] = self.validate_boolean(data.get('verified', False), 'verified')
        
        return validated 