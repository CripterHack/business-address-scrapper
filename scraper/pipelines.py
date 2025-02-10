import logging
import pandas as pd
from datetime import datetime
import re
from typing import Dict, Any, Optional, List
import os
from dataclasses import dataclass, asdict
from itertools import islice
import numpy as np

@dataclass
class BusinessData:
    id: int
    business_name: str
    address: str
    state: Optional[str] = None
    zip_code: Optional[str] = None
    created_at: Optional[str] = None

class BusinessDataPipeline:
    CHUNK_SIZE = 1000  # Number of items to process before writing to disk

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.items = []
        self.processed_count = 0
        self.skipped_count = 0
        self.error_count = 0
        self._ensure_output_dir()
        self.stats = {
            'total_items': 0,
            'valid_items': 0,
            'invalid_items': 0,
            'processing_errors': 0,
            'chunks_written': 0
        }

    def _ensure_output_dir(self):
        """Ensure the output directory exists"""
        output_dir = 'output'
        self.logger.debug(f"Checking if output directory exists: {output_dir}")
        if not os.path.exists(output_dir):
            self.logger.info(f"Creating output directory: {output_dir}")
            os.makedirs(output_dir)
        else:
            self.logger.debug("Output directory already exists")

    def process_item(self, item, spider):
        """Process each scraped item"""
        self.stats['total_items'] += 1
        try:
            self.logger.debug(f"Processing item for business: {item.get('business_name')}")
            
            # Validate and clean the data
            cleaned_item = self._clean_item(item)
            if not cleaned_item:
                self.logger.warning(f"Item cleaning failed for business: {item.get('business_name')}")
                self.stats['invalid_items'] += 1
                return None
            
            # Add timestamp
            cleaned_item['scraped_at'] = datetime.now().isoformat()
            
            # Store the item
            self.items.append(cleaned_item)
            self.processed_count += 1
            self.stats['valid_items'] += 1
            
            # Write to disk if we've reached the chunk size
            if len(self.items) >= self.CHUNK_SIZE:
                self.logger.info(f"Chunk size reached ({self.CHUNK_SIZE} items). Writing to disk...")
                self._write_chunk()
            
            self.logger.debug(f"Successfully processed item for business: {cleaned_item.get('business_name')}")
            return cleaned_item
            
        except Exception as e:
            self.logger.error(f"Error processing item: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            self.error_count += 1
            self.stats['processing_errors'] += 1
            return None

    def _clean_item(self, item):
        """Clean and validate item data"""
        try:
            # Required fields
            required_fields = ['id', 'business_name', 'address', 'created_at']
            for field in required_fields:
                if not item.get(field):
                    self.logger.warning(f"Missing required field {field} for business: {item.get('business_name')}")
                    return None

            # Clean and format the data
            cleaned_item = {
                'id': item['id'],
                'business_name': item['business_name'].strip(),
                'address': item['address'].strip(),
                'created_at': item['created_at']
            }

            # Extraer state y zip_code del address si no están presentes
            if not item.get('state') or not item.get('zip_code'):
                address_parts = self._parse_address_components(cleaned_item['address'])
                if address_parts:
                    # Actualizar la dirección para excluir state y zip_code si se encontraron
                    if address_parts.get('clean_address'):
                        cleaned_item['address'] = address_parts['clean_address']
                    if not item.get('state') and address_parts.get('state'):
                        cleaned_item['state'] = self._normalize_state(address_parts['state'])
                    if not item.get('zip_code') and address_parts.get('zip_code'):
                        cleaned_item['zip_code'] = address_parts['zip_code']
            else:
                cleaned_item['state'] = self._normalize_state(item['state'].strip())
                cleaned_item['zip_code'] = item['zip_code'].strip()

            return cleaned_item

        except Exception as e:
            self.logger.error(f"Error cleaning item: {str(e)}")
            self.stats['processing_errors'] += 1
            return None

    def _normalize_state(self, state):
        """Normaliza el código de estado a formato de dos letras"""
        if not state:
            return None
            
        # Diccionario de nombres de estados completos a códigos
        state_mapping = {
            'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
            'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
            'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
            'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
            'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
            'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
            'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
            'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
            'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
            'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
            'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
            'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
            'WISCONSIN': 'WI', 'WYOMING': 'WY'
        }
        
        state = state.upper().strip()
        
        # Si ya es un código de dos letras válido
        if len(state) == 2 and state in state_mapping.values():
            return state
            
        # Si es un nombre completo
        if state in state_mapping:
            return state_mapping[state]
            
        # Buscar coincidencias parciales
        for full_name, code in state_mapping.items():
            if full_name.startswith(state):
                return code
                
        return state if len(state) == 2 else None

    def _parse_address_components(self, address):
        """Extract state and zip code from address string"""
        if not address:
            return None

        try:
            # Patrones para encontrar state y zip_code
            state_zip_patterns = [
                r'(?:,\s*)?([A-Za-z]{2})\s+(\d{5}(?:-\d{4})?)\s*$',  # Estado y ZIP al final
                r'(?:,\s*)?([A-Za-z]{2})\s*$',  # Solo estado al final
                r'(?:,\s*)?(\d{5}(?:-\d{4})?)\s*$'  # Solo ZIP al final
            ]

            address = address.strip()
            result = {'clean_address': address}

            for pattern in state_zip_patterns:
                match = re.search(pattern, address, re.IGNORECASE)
                if match:
                    groups = match.groups()
                    if len(groups) == 2:  # Estado y ZIP
                        result['state'] = groups[0].upper()
                        result['zip_code'] = groups[1]
                        result['clean_address'] = address[:match.start()].strip().rstrip(',')
                    elif len(groups) == 1:  # Solo estado o solo ZIP
                        if groups[0].isalpha():  # Es estado
                            result['state'] = groups[0].upper()
                        else:  # Es ZIP
                            result['zip_code'] = groups[0]
                        result['clean_address'] = address[:match.start()].strip().rstrip(',')
                    break

            return result

        except Exception as e:
            self.logger.error(f"Error parsing address components: {str(e)}")
            return None

    def _write_chunk(self):
        """Write current items to a chunk file"""
        try:
            if not self.items:
                self.logger.debug("No items to write")
                return

            chunk_num = self.stats['chunks_written'] + 1
            chunk_file = f'output/chunk_{chunk_num:04d}.csv'
            self.logger.info(f"Writing {len(self.items)} items to chunk file: {chunk_file}")
            
            # Asegurar que las columnas estén en el orden correcto
            columns = ['id', 'business_name', 'address', 'state', 'zip_code', 'created_at']
            df = pd.DataFrame(self.items)
            df = df[columns]  # Reordenar columnas
            df.to_csv(chunk_file, index=False)
            
            self.items = []  # Clear the items list
            self.stats['chunks_written'] += 1
            self.logger.debug(f"Successfully wrote chunk file: {chunk_file}")
            
        except Exception as e:
            self.logger.error(f"Error writing chunk file: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            raise StorageError(f"Failed to write chunk file: {str(e)}")

    def close_spider(self, spider):
        """Called when the spider is closed"""
        try:
            self.logger.info("Spider closing. Processing final items...")
            
            # Write any remaining items
            if self.items:
                self.logger.info(f"Writing final {len(self.items)} items")
                self._write_chunk()
            
            # Combine all chunks
            chunk_pattern = 'output/chunk_*.csv'
            self.logger.info("Combining chunk files...")
            chunk_files = sorted(os.glob(chunk_pattern))
            
            if not chunk_files:
                self.logger.warning("No chunk files found to combine")
                return
            
            self.logger.debug(f"Found {len(chunk_files)} chunk files to combine")
            
            # Leer y combinar chunks asegurando el orden de las columnas
            columns = ['id', 'business_name', 'address', 'state', 'zip_code', 'created_at']
            all_chunks = []
            for f in chunk_files:
                df = pd.read_csv(f)
                df = df[columns]  # Reordenar columnas
                all_chunks.append(df)
            
            final_df = pd.concat(all_chunks, ignore_index=True)
            
            # Save final output
            output_file = os.getenv('CSV_OUTPUT_FILE', 'business_data.csv')
            self.logger.info(f"Saving combined data to {output_file}")
            final_df.to_csv(output_file, index=False)
            
            # Clean up chunks
            self.logger.debug("Cleaning up chunk files...")
            for chunk_file in chunk_files:
                os.remove(chunk_file)
            
            # Log final statistics
            self.logger.info("Pipeline processing completed. Final statistics:")
            self.logger.info(f"Total items processed: {self.stats['total_items']}")
            self.logger.info(f"Valid items: {self.stats['valid_items']}")
            self.logger.info(f"Invalid items: {self.stats['invalid_items']}")
            self.logger.info(f"Processing errors: {self.stats['processing_errors']}")
            self.logger.info(f"Chunks written: {self.stats['chunks_written']}")
            self.logger.info(f"Total items saved: {self.processed_count}")
            self.logger.info(f"Items skipped: {self.skipped_count}")
            self.logger.info(f"Errors encountered: {self.error_count}")
            
        except Exception as e:
            self.logger.error(f"Error in spider cleanup: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            raise StorageError(f"Failed to complete pipeline cleanup: {str(e)}")

class DuplicateFilterPipeline:
    """Pipeline to filter out duplicate business entries"""
    
    def __init__(self):
        self.businesses_seen = set()
        self.logger = logging.getLogger(__name__)

    def process_item(self, item: Dict[str, Any], spider) -> Optional[Dict[str, Any]]:
        """Process each item and filter duplicates"""
        try:
            business_key = self._generate_business_key(item)
            
            if business_key in self.businesses_seen:
                self.logger.info(f"Duplicate business found: {item['business_name']}")
                return None
            
            self.businesses_seen.add(business_key)
            return item
        except Exception as e:
            self.logger.error(f"Error in duplicate filter: {e}")
            return None

    def _generate_business_key(self, item: Dict[str, Any]) -> str:
        """Generate a unique key for a business based on name and address"""
        try:
            name = str(item.get('business_name', '')).lower().strip()
            address = str(item.get('address', '')).lower().strip()
            return f"{name}|{address}"
        except Exception as e:
            self.logger.error(f"Error generating business key: {e}")
            return '' 