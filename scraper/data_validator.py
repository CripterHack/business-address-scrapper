"""Sistema de validación de datos para el scraper."""

import logging
import re
from typing import Dict, Any, Optional, List, Union, Type, TypeVar
from datetime import datetime
from dataclasses import dataclass, field
import json
from pathlib import Path

from .exceptions import ValidationError
from .metrics import MetricsManager

logger = logging.getLogger(__name__)

T = TypeVar('T')

@dataclass
class ValidationConfig:
    """Configuración de validación."""
    max_string_length: int = 1000
    min_string_length: int = 1
    max_list_items: int = 1000
    max_dict_items: int = 1000
    allowed_types: List[Type] = field(default_factory=lambda: [str, int, float, bool, dict, list])
    date_formats: List[str] = field(default_factory=lambda: ['%Y-%m-%d', '%Y/%m/%d'])
    max_file_size_mb: int = 100

@dataclass
class ValidationRule:
    """Regla de validación."""
    field_type: Type
    required: bool = True
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[callable] = None
    nested_rules: Optional[Dict[str, 'ValidationRule']] = None

class DataValidator:
    """Validador de datos genérico."""
    
    def __init__(
        self,
        config: Optional[ValidationConfig] = None,
        metrics: Optional[MetricsManager] = None
    ):
        """Inicializa el validador.
        
        Args:
            config: Configuración de validación
            metrics: Gestor de métricas opcional
        """
        self.config = config or ValidationConfig()
        self.metrics = metrics
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
    
    def validate(
        self,
        data: Any,
        rules: Optional[Dict[str, ValidationRule]] = None,
        path: str = ''
    ) -> bool:
        """Valida datos según reglas especificadas.
        
        Args:
            data: Datos a validar
            rules: Reglas de validación
            path: Ruta actual en la estructura de datos
            
        Returns:
            bool: True si los datos son válidos
        """
        try:
            if rules is None:
                return self._validate_type(data, path)
            
            if not isinstance(data, dict):
                self._add_error(path, "Data must be a dictionary")
                return False
            
            # Validar campos requeridos
            for field, rule in rules.items():
                field_path = f"{path}.{field}" if path else field
                
                if field not in data:
                    if rule.required:
                        self._add_error(field_path, "Required field is missing")
                    continue
                
                field_value = data[field]
                
                # Validar tipo
                if not isinstance(field_value, rule.field_type):
                    self._add_error(
                        field_path,
                        f"Invalid type. Expected {rule.field_type.__name__}, "
                        f"got {type(field_value).__name__}"
                    )
                    continue
                
                # Validar longitud
                if isinstance(field_value, (str, list, dict)):
                    if rule.min_length and len(field_value) < rule.min_length:
                        self._add_error(
                            field_path,
                            f"Length must be at least {rule.min_length}"
                        )
                    if rule.max_length and len(field_value) > rule.max_length:
                        self._add_error(
                            field_path,
                            f"Length must be at most {rule.max_length}"
                        )
                
                # Validar patrón
                if isinstance(field_value, str) and rule.pattern:
                    if not re.match(rule.pattern, field_value):
                        self._add_error(field_path, "Value does not match pattern")
                
                # Validar valores permitidos
                if rule.allowed_values is not None:
                    if field_value not in rule.allowed_values:
                        self._add_error(
                            field_path,
                            f"Value must be one of: {rule.allowed_values}"
                        )
                
                # Validar reglas anidadas
                if rule.nested_rules and isinstance(field_value, dict):
                    self.validate(field_value, rule.nested_rules, field_path)
                
                # Ejecutar validador personalizado
                if rule.custom_validator:
                    try:
                        rule.custom_validator(field_value)
                    except ValidationError as e:
                        self._add_error(field_path, str(e))
            
            return len(self.errors) == 0
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}", exc_info=True)
            self._add_error(path, f"Validation error: {str(e)}")
            return False
    
    def validate_file(
        self,
        file_path: Union[str, Path],
        expected_type: str = 'json'
    ) -> bool:
        """Valida un archivo.
        
        Args:
            file_path: Ruta del archivo
            expected_type: Tipo esperado del archivo
            
        Returns:
            bool: True si el archivo es válido
        """
        try:
            file_path = Path(file_path)
            
            # Validar existencia
            if not file_path.exists():
                self._add_error('file', "File does not exist")
                return False
            
            # Validar tamaño
            size_mb = file_path.stat().st_size / (1024 * 1024)
            if size_mb > self.config.max_file_size_mb:
                self._add_error(
                    'file',
                    f"File size ({size_mb:.1f}MB) exceeds limit "
                    f"({self.config.max_file_size_mb}MB)"
                )
                return False
            
            # Validar tipo
            if expected_type == 'json':
                return self._validate_json_file(file_path)
            elif expected_type == 'csv':
                return self._validate_csv_file(file_path)
            else:
                self._add_error('file', f"Unsupported file type: {expected_type}")
                return False
            
        except Exception as e:
            logger.error(f"Error validating file: {str(e)}", exc_info=True)
            self._add_error('file', f"Validation error: {str(e)}")
            return False
    
    def _validate_json_file(self, file_path: Path) -> bool:
        """Valida un archivo JSON."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validar estructura
            if not isinstance(data, (dict, list)):
                self._add_error('json', "Root must be an object or array")
                return False
            
            # Validar tamaño
            if isinstance(data, dict) and len(data) > self.config.max_dict_items:
                self._add_error(
                    'json',
                    f"Too many items in object ({len(data)})"
                )
                return False
            
            if isinstance(data, list) and len(data) > self.config.max_list_items:
                self._add_error(
                    'json',
                    f"Too many items in array ({len(data)})"
                )
                return False
            
            return True
            
        except json.JSONDecodeError as e:
            self._add_error('json', f"Invalid JSON: {str(e)}")
            return False
    
    def _validate_csv_file(self, file_path: Path) -> bool:
        """Valida un archivo CSV."""
        try:
            import pandas as pd
            
            # Leer primeras filas para validar estructura
            df = pd.read_csv(file_path, nrows=5)
            
            # Validar columnas
            if len(df.columns) == 0:
                self._add_error('csv', "File has no columns")
                return False
            
            # Validar tipos de datos
            for column in df.columns:
                if df[column].dtype not in ['object', 'int64', 'float64', 'bool']:
                    self._add_warning(
                        'csv',
                        f"Column '{column}' has unusual type: {df[column].dtype}"
                    )
            
            return True
            
        except Exception as e:
            self._add_error('csv', f"Invalid CSV: {str(e)}")
            return False
    
    def _validate_type(self, value: Any, path: str) -> bool:
        """Valida el tipo de un valor."""
        if type(value) not in self.config.allowed_types:
            self._add_error(
                path,
                f"Type {type(value).__name__} not allowed"
            )
            return False
        
        if isinstance(value, str):
            return self._validate_string(value, path)
        elif isinstance(value, (list, dict)):
            return self._validate_container(value, path)
        
        return True
    
    def _validate_string(self, value: str, path: str) -> bool:
        """Valida una cadena."""
        if len(value) < self.config.min_string_length:
            self._add_error(
                path,
                f"String too short (min {self.config.min_string_length})"
            )
            return False
        
        if len(value) > self.config.max_string_length:
            self._add_error(
                path,
                f"String too long (max {self.config.max_string_length})"
            )
            return False
        
        return True
    
    def _validate_container(self, value: Union[list, dict], path: str) -> bool:
        """Valida un contenedor (lista o diccionario)."""
        if isinstance(value, list) and len(value) > self.config.max_list_items:
            self._add_error(
                path,
                f"Too many items in list (max {self.config.max_list_items})"
            )
            return False
        
        if isinstance(value, dict) and len(value) > self.config.max_dict_items:
            self._add_error(
                path,
                f"Too many items in dict (max {self.config.max_dict_items})"
            )
            return False
        
        # Validar elementos recursivamente
        if isinstance(value, list):
            for i, item in enumerate(value):
                if not self._validate_type(item, f"{path}[{i}]"):
                    return False
        else:
            for key, item in value.items():
                if not self._validate_type(item, f"{path}.{key}"):
                    return False
        
        return True
    
    def _add_error(self, path: str, message: str) -> None:
        """Añade un error."""
        self.errors.append({
            'path': path,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
        
        if self.metrics:
            self.metrics.record_validation_error(path)
    
    def _add_warning(self, path: str, message: str) -> None:
        """Añade una advertencia."""
        self.warnings.append({
            'path': path,
            'message': message,
            'timestamp': datetime.now().isoformat()
        })
        
        if self.metrics:
            self.metrics.record_validation_warning(path)
    
    def get_errors(self) -> List[Dict[str, Any]]:
        """Obtiene los errores de validación."""
        return self.errors
    
    def get_warnings(self) -> List[Dict[str, Any]]:
        """Obtiene las advertencias de validación."""
        return self.warnings
    
    def clear(self) -> None:
        """Limpia errores y advertencias."""
        self.errors.clear()
        self.warnings.clear()
    
    def generate_report(self, output_file: Optional[str] = None) -> Dict[str, Any]:
        """Genera un reporte de validación.
        
        Args:
            output_file: Archivo de salida opcional
            
        Returns:
            Dict[str, Any]: Reporte de validación
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'valid': len(self.errors) == 0,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': self.errors,
            'warnings': self.warnings
        }
        
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(report, f, indent=2)
                logger.info(f"Validation report saved to {output_file}")
            except Exception as e:
                logger.error(f"Error saving validation report: {str(e)}")
        
        return report 