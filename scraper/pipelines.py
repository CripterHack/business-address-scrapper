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
    business_name: str
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    violation_type: Optional[str] = None
    nsl_published_date: Optional[str] = None
    nsl_effective_date: Optional[str] = None
    remediated_date: Optional[str] = None
    verified: bool = False
    source_url: Optional[str] = None
    relevance_score: float = 0.0
    scraped_at: Optional[str] = None

class BusinessDataPipeline:
    CHUNK_SIZE = 1000  # Number of items to process before writing to disk

    def __init__(self):
        self.items = []
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing BusinessDataPipeline")
        self.processed_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self._ensure_output_dir()
        self.stats = {
            'total_items': 0,
            'valid_items': 0,
            'invalid_items': 0,
            'chunks_written': 0,
            'processing_errors': 0,
            'validation_errors': 0
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
            
            # Validate New York address
            if not self._validate_ny_address(cleaned_item):
                self.logger.warning(f"Skipping non-NY address for business: {cleaned_item.get('business_name')}")
                self.skipped_count += 1
                self.stats['validation_errors'] += 1
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

    def _clean_item(self, item: Dict[str, Any]) -> BusinessData:
        """Clean and standardize item data"""
        try:
            return BusinessData(
                business_name=self._clean_text(item.get('business_name', '')),
                address=self._clean_text(item.get('address', '')),
                city=self._clean_text(item.get('city', '')),
                state=self._clean_state(item.get('state', '')),
                zip_code=self._clean_zip(item.get('zip_code', '')),
                violation_type=self._clean_text(item.get('violation_type', '')),
                nsl_published_date=self._clean_date(item.get('nsl_published_date', '')),
                nsl_effective_date=self._clean_date(item.get('nsl_effective_date', '')),
                remediated_date=self._clean_date(item.get('remediated_date', '')),
                verified=bool(item.get('verified', False)),
                source_url=str(item.get('source_url', '')),
                relevance_score=float(item.get('relevance_score', 0.0))
            )
        except (ValueError, TypeError) as e:
            self.logger.error(f"Data type validation error: {e}")
            raise

    def _clean_text(self, text: str) -> str:
        """Clean and standardize text fields"""
        if not isinstance(text, str):
            text = str(text)
        
        # Remove extra whitespace and special characters
        cleaned = re.sub(r'[^\w\s,.-]', '', text)
        cleaned = ' '.join(cleaned.split())
        return cleaned.strip()

    def _clean_state(self, state: str) -> str:
        """Clean and validate state code"""
        if not state:
            return ''
        
        state = str(state).upper().strip()
        # Ensure it's a valid two-letter state code
        if len(state) == 2 and state.isalpha():
            return state
        return ''

    def _clean_zip(self, zip_code: str) -> str:
        """Clean and validate ZIP code"""
        if not zip_code:
            return ''
        
        # Extract just the digits
        digits = ''.join(filter(str.isdigit, str(zip_code)))
        
        # Validate basic ZIP format (5 digits or ZIP+4)
        if len(digits) in [5, 9]:
            return digits[:5]  # Return just the first 5 digits
        return ''

    def _clean_date(self, date_str: str) -> str:
        """Clean and standardize date format"""
        if not date_str:
            return ''
        
        try:
            # Try parsing various date formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d']:
                try:
                    return datetime.strptime(str(date_str).strip(), fmt).strftime('%Y-%m-%d')
                except ValueError:
                    continue
            return ''
        except Exception:
            return ''

    def _validate_ny_address(self, item: BusinessData) -> bool:
        """Validate that the address is in New York"""
        state = item.state.upper()
        zip_code = item.zip_code
        
        # Check state is NY
        if state != 'NY':
            return False
        
        # Validate ZIP code is in NY range (100xx-149xx)
        if zip_code and len(zip_code) == 5:
            try:
                zip_prefix = int(zip_code[:3])
                if not (100 <= zip_prefix <= 149):
                    return False
            except ValueError:
                return False
        
        return True

    def _write_chunk(self):
        """Write current items to a chunk file"""
        try:
            if not self.items:
                self.logger.debug("No items to write")
                return

            chunk_num = self.stats['chunks_written'] + 1
            chunk_file = f'output/chunk_{chunk_num:04d}.csv'
            self.logger.info(f"Writing {len(self.items)} items to chunk file: {chunk_file}")
            
            df = pd.DataFrame(self.items)
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
            all_chunks = pd.concat(
                [pd.read_csv(f) for f in chunk_files],
                ignore_index=True
            )
            
            # Save final output
            output_file = os.getenv('CSV_OUTPUT_FILE', 'business_data.csv')
            self.logger.info(f"Saving combined data to {output_file}")
            all_chunks.to_csv(output_file, index=False)
            
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
            self.logger.info(f"Validation errors: {self.stats['validation_errors']}")
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