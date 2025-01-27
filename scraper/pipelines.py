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
        self.items: List[BusinessData] = []
        self.logger = logging.getLogger(__name__)
        self.processed_count = 0
        self._ensure_output_dir()

    def _ensure_output_dir(self):
        """Ensure the output directory exists"""
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def process_item(self, item: Dict[str, Any], spider) -> Optional[Dict[str, Any]]:
        """Process each scraped item"""
        try:
            # Validate and clean the data
            cleaned_item = self._clean_item(item)
            
            # Validate New York address
            if not self._validate_ny_address(cleaned_item):
                self.logger.warning(f"Skipping non-NY address for business: {cleaned_item.business_name}")
                return None
            
            # Add timestamp
            cleaned_item.scraped_at = datetime.now().isoformat()
            
            # Store the item
            self.items.append(cleaned_item)
            self.processed_count += 1
            
            # Write to disk if we've reached the chunk size
            if len(self.items) >= self.CHUNK_SIZE:
                self._write_chunk()
            
            return asdict(cleaned_item)
        except Exception as e:
            self.logger.error(f"Error processing item: {e}")
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
        """Write current chunk of items to disk"""
        if not self.items:
            return

        try:
            # Convert items to DataFrame
            df = pd.DataFrame([asdict(item) for item in self.items])
            
            # Generate chunk filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            chunk_file = f'output/chunk_{timestamp}_{len(self.items)}.csv'
            
            # Save to CSV
            df.to_csv(chunk_file, index=False)
            
            self.logger.info(f"Saved chunk of {len(self.items)} items to {chunk_file}")
            
            # Clear the items list
            self.items = []
        except Exception as e:
            self.logger.error(f"Error saving chunk: {e}")

    def close_spider(self, spider):
        """Called when the spider is closed"""
        try:
            # Write any remaining items
            self._write_chunk()
            
            # Combine all chunks
            chunk_pattern = 'output/chunk_*.csv'
            all_chunks = pd.concat(
                [pd.read_csv(f) for f in sorted(os.glob(chunk_pattern))],
                ignore_index=True
            )
            
            # Save final output
            output_file = os.getenv('CSV_OUTPUT_FILE', 'business_data.csv')
            all_chunks.to_csv(output_file, index=False)
            
            # Clean up chunks
            for chunk_file in os.glob(chunk_pattern):
                os.remove(chunk_file)
            
            self.logger.info(f"Saved {self.processed_count} total items to {output_file}")
        except Exception as e:
            self.logger.error(f"Error in spider cleanup: {e}")

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