from scrapy import Item, Field
from typing import Optional, Dict, Any, Union, ClassVar, Set
from dataclasses import dataclass, field
from datetime import datetime
import re
from abc import ABC, abstractmethod

class BaseDataValidator(ABC):
    """Clase base para validación de datos"""
    
    @abstractmethod
    def validate(self) -> None:
        """Validar los datos del objeto"""
        pass

@dataclass
class BusinessData(BaseDataValidator):
    """Data class for business information
    
    Attributes:
        id (int): Unique identifier for the business
        business_name (str): Name of the business
        address (str): Full address of the business
        state (Optional[str]): Two-letter state code
        zip_code (Optional[str]): 5-digit or 9-digit ZIP code
        created_at (str): Creation date in YYYY-MM-DD format
    """
    # Constantes de clase para validación
    MIN_NAME_LENGTH: ClassVar[int] = 2
    MAX_NAME_LENGTH: ClassVar[int] = 200
    MIN_ADDRESS_LENGTH: ClassVar[int] = 5
    MAX_ADDRESS_LENGTH: ClassVar[int] = 200
    DATE_FORMAT: ClassVar[str] = '%Y-%m-%d'
    ZIP_CODE_PATTERN: ClassVar[re.Pattern] = re.compile(r'^\d{5}(?:-\d{4})?$')
    REQUIRED_FIELDS: ClassVar[Set[str]] = {'id', 'business_name', 'address', 'created_at'}

    id: int
    business_name: str
    address: str
    state: Optional[str] = None
    zip_code: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().strftime('%Y-%m-%d'))

    def __post_init__(self):
        """Validate types and values after initialization"""
        self.validate()

    def validate(self) -> None:
        """Validate all data fields
        
        Raises:
            TypeError: If any field has an invalid type
            ValueError: If any field has an invalid value
        """
        self._validate_types()
        self._validate_values()

    def _validate_types(self) -> None:
        """Validate types of all fields
        
        Raises:
            TypeError: If any field has an invalid type
        """
        if not isinstance(self.id, int):
            raise TypeError(f"id must be an integer, got {type(self.id)}")
        if not isinstance(self.business_name, str):
            raise TypeError(f"business_name must be a string, got {type(self.business_name)}")
        if not isinstance(self.address, str):
            raise TypeError(f"address must be a string, got {type(self.address)}")
        if self.state is not None and not isinstance(self.state, str):
            raise TypeError(f"state must be a string or None, got {type(self.state)}")
        if self.zip_code is not None and not isinstance(self.zip_code, str):
            raise TypeError(f"zip_code must be a string or None, got {type(self.zip_code)}")
        if not isinstance(self.created_at, str):
            raise TypeError(f"created_at must be a string, got {type(self.created_at)}")

    def _validate_values(self) -> None:
        """Validate values of all fields
        
        Raises:
            ValueError: If any field has an invalid value
        """
        if self.id <= 0:
            raise ValueError("id must be a positive integer")
            
        business_name_len = len(self.business_name.strip())
        if not self.MIN_NAME_LENGTH <= business_name_len <= self.MAX_NAME_LENGTH:
            raise ValueError(
                f"business_name length must be between {self.MIN_NAME_LENGTH} and {self.MAX_NAME_LENGTH} characters"
            )
            
        address_len = len(self.address.strip())
        if not self.MIN_ADDRESS_LENGTH <= address_len <= self.MAX_ADDRESS_LENGTH:
            raise ValueError(
                f"address length must be between {self.MIN_ADDRESS_LENGTH} and {self.MAX_ADDRESS_LENGTH} characters"
            )
            
        if self.state is not None:
            if len(self.state) != 2 or not self.state.isalpha():
                raise ValueError("state must be a two-letter code")
                
        if self.zip_code is not None:
            if not self.ZIP_CODE_PATTERN.match(self.zip_code):
                raise ValueError("zip_code must be in format XXXXX or XXXXX-XXXX")
                
        try:
            date = datetime.strptime(self.created_at, self.DATE_FORMAT)
            if date > datetime.now():
                raise ValueError("created_at cannot be in the future")
        except ValueError as e:
            if "does not match format" in str(e):
                raise ValueError(f"created_at must be in format {self.DATE_FORMAT}")
            raise

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BusinessData':
        """Create BusinessData from dictionary
        
        Args:
            data: Dictionary containing business data
            
        Returns:
            BusinessData: New instance with data from dictionary
            
        Raises:
            TypeError: If data types are invalid
            KeyError: If required fields are missing
            ValueError: If data values are invalid
        """
        missing_fields = cls.REQUIRED_FIELDS - set(data.keys())
        if missing_fields:
            raise KeyError(f"Missing required fields: {missing_fields}")

        # Limpieza y conversión de datos
        cleaned_data = cls._clean_data(data)
        return cls(**cleaned_data)

    @classmethod
    def _clean_data(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and convert data from dictionary
        
        Args:
            data: Dictionary containing business data
            
        Returns:
            Dict[str, Any]: Cleaned and converted data
        """
        cleaned_data = {
            'id': int(data['id']) if isinstance(data['id'], (str, int)) and str(data['id']).isdigit() else data['id'],
            'business_name': str(data['business_name']).strip(),
            'address': str(data['address']).strip(),
            'created_at': str(data['created_at']).strip()
        }
        
        # Campos opcionales
        if 'state' in data and data['state']:
            cleaned_data['state'] = str(data['state']).strip().upper()
        if 'zip_code' in data and data['zip_code']:
            cleaned_data['zip_code'] = str(data['zip_code']).strip()

        return cleaned_data

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary
        
        Returns:
            Dict[str, Any]: Dictionary representation with non-None values
        """
        return {
            key: value for key, value in {
                'id': self.id,
                'business_name': self.business_name,
                'address': self.address,
                'state': self.state,
                'zip_code': self.zip_code,
                'created_at': self.created_at
            }.items() if value is not None
        }

    def __str__(self) -> str:
        """String representation of the business data"""
        return (
            f"Business(id={self.id}, "
            f"name='{self.business_name}', "
            f"address='{self.address}', "
            f"state='{self.state or 'N/A'}', "
            f"zip='{self.zip_code or 'N/A'}', "
            f"created='{self.created_at}')"
        )

class BusinessItem(Item):
    """Define the structure of scraped business data
    
    Fields:
        id (int): Unique identifier for the business
        business_name (str): Name of the business
        address (str): Full address of the business
        state (str): Two-letter state code
        zip_code (str): 5-digit or 9-digit ZIP code
        created_at (str): Creation date in YYYY-MM-DD format
    """
    REQUIRED_FIELDS: ClassVar[Set[str]] = {'id', 'business_name', 'address', 'created_at'}

    id = Field()
    business_name = Field()
    address = Field()
    state = Field()
    zip_code = Field()
    created_at = Field()

    @classmethod
    def from_business_data(cls, data: BusinessData) -> 'BusinessItem':
        """Create a BusinessItem from a BusinessData instance
        
        Args:
            data: BusinessData instance containing validated business information
            
        Returns:
            BusinessItem: New item with data from BusinessData
            
        Raises:
            ValueError: If required fields are missing or invalid
            TypeError: If data is not a BusinessData instance
        """
        if not isinstance(data, BusinessData):
            raise TypeError("Data must be an instance of BusinessData")
            
        item = cls()
        
        # Validate required fields
        missing_fields = cls.REQUIRED_FIELDS - set(data.__dataclass_fields__)
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
            
        # Copy all fields
        for field in data.__dataclass_fields__:
            value = getattr(data, field)
            if field in cls.REQUIRED_FIELDS and not value:
                raise ValueError(f"Required field '{field}' cannot be empty")
            if value is not None:  # Solo copiar campos con valor
                item[field] = value
            
        return item

    def to_dict(self) -> Dict[str, Any]:
        """Convert item to dictionary
        
        Returns:
            Dict[str, Any]: Dictionary representation with non-None values
        """
        return {key: value for key, value in self.items() if value is not None}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BusinessItem':
        """Create BusinessItem from dictionary
        
        Args:
            data: Dictionary containing business data
            
        Returns:
            BusinessItem: New item with data from dictionary
            
        Raises:
            KeyError: If required fields are missing
            ValueError: If data values are invalid
            TypeError: If data types are invalid
        """
        business_data = BusinessData.from_dict(data)
        return cls.from_business_data(business_data)
        
    def __str__(self) -> str:
        """String representation of the business item
        
        Returns:
            str: Formatted string with business details
        """
        return (
            f"Business(name='{self.get('business_name')}', "
            f"address='{self.get('address')}', "
            f"state='{self.get('state', 'N/A')}', "
            f"zip='{self.get('zip_code', 'N/A')}')"
        ) 