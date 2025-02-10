"""Validador de datos de entrada para el scraper."""

import csv
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path
import pandas as pd

from .validators import BusinessValidator, ValidationResult
from .exceptions import ValidationError

logger = logging.getLogger(__name__)

class InputValidator:
    """Validador de datos de entrada."""
    
    def __init__(self):
        self.business_validator = BusinessValidator()
        
    def validate_csv_file(self, file_path: str) -> ValidationResult:
        """Valida un archivo CSV de entrada.
        
        Args:
            file_path: Ruta al archivo CSV
            
        Returns:
            ValidationResult: Resultado de la validación
        """
        try:
            # Verificar existencia del archivo
            if not Path(file_path).exists():
                return ValidationResult(
                    is_valid=False,
                    error_message="File does not exist",
                    details={'file_path': file_path}
                )
            
            # Leer archivo con pandas
            df = pd.read_csv(file_path)
            
            # Validar columnas requeridas
            required_columns = ['business_name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                return ValidationResult(
                    is_valid=False,
                    error_message=f"Missing required columns: {', '.join(missing_columns)}",
                    details={'missing_columns': missing_columns}
                )
            
            # Validar datos
            errors = []
            warnings = []
            
            for idx, row in df.iterrows():
                try:
                    # Validar nombre de negocio
                    if not self.business_validator._validate_business_name(row['business_name']):
                        errors.append({
                            'row': idx + 1,
                            'business_name': row['business_name'],
                            'errors': self.business_validator.errors
                        })
                    
                    # Validar dirección si existe
                    if 'address' in df.columns and pd.notna(row['address']):
                        address_result = self.business_validator.validate({
                            'business_name': row['business_name'],
                            'address': row['address']
                        })
                        if not address_result.is_valid:
                            errors.append({
                                'row': idx + 1,
                                'business_name': row['business_name'],
                                'address': row['address'],
                                'errors': address_result.error_message
                            })
                        if address_result.warnings:
                            warnings.append({
                                'row': idx + 1,
                                'business_name': row['business_name'],
                                'warnings': address_result.warnings
                            })
                
                except Exception as e:
                    errors.append({
                        'row': idx + 1,
                        'business_name': row['business_name'],
                        'error': str(e)
                    })
            
            # Validar datos duplicados
            duplicates = df['business_name'].duplicated()
            if duplicates.any():
                warnings.append({
                    'type': 'duplicates',
                    'count': duplicates.sum(),
                    'rows': df[duplicates].index.tolist()
                })
            
            return ValidationResult(
                is_valid=len(errors) == 0,
                error_message=f"Found {len(errors)} errors" if errors else None,
                warnings=warnings,
                details={
                    'errors': errors,
                    'total_rows': len(df),
                    'valid_rows': len(df) - len(errors)
                }
            )
            
        except Exception as e:
            logger.error(f"Error validating CSV file: {str(e)}", exc_info=True)
            return ValidationResult(
                is_valid=False,
                error_message=f"Error validating file: {str(e)}"
            )
    
    def validate_batch_data(self, data: List[Dict[str, Any]]) -> ValidationResult:
        """Valida un lote de datos.
        
        Args:
            data: Lista de diccionarios con datos
            
        Returns:
            ValidationResult: Resultado de la validación
        """
        errors = []
        warnings = []
        
        for idx, item in enumerate(data):
            try:
                result = self.business_validator.validate(item)
                if not result.is_valid:
                    errors.append({
                        'index': idx,
                        'data': item,
                        'errors': result.error_message
                    })
                if result.warnings:
                    warnings.append({
                        'index': idx,
                        'data': item,
                        'warnings': result.warnings
                    })
            except Exception as e:
                errors.append({
                    'index': idx,
                    'data': item,
                    'error': str(e)
                })
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            error_message=f"Found {len(errors)} errors" if errors else None,
            warnings=warnings,
            details={
                'errors': errors,
                'total_items': len(data),
                'valid_items': len(data) - len(errors)
            }
        )
    
    def generate_error_report(self, result: ValidationResult, output_file: str) -> None:
        """Genera un reporte de errores en formato CSV.
        
        Args:
            result: Resultado de la validación
            output_file: Archivo de salida
        """
        try:
            if not result.details or 'errors' not in result.details:
                logger.warning("No errors to report")
                return
            
            errors = result.details['errors']
            if not errors:
                logger.info("No errors found to report")
                return
            
            # Crear DataFrame con errores
            df = pd.DataFrame(errors)
            
            # Guardar reporte
            df.to_csv(output_file, index=False)
            logger.info(f"Error report saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error generating error report: {str(e)}", exc_info=True)
            raise 