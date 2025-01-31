import scrapy
import json
import pandas as pd
from datetime import datetime
from retry import retry
import validators
import tldextract
from duckduckgo_search import DDGS
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

from scraper.settings import (
    NY_STATE_CODE,
    DUCKDUCKGO_REGION,
    DUCKDUCKGO_SAFESEARCH,
    DUCKDUCKGO_TIMEOUT,
    DUCKDUCKGO_MAX_RESULTS,
    DUCKDUCKGO_BACKEND,
    CSV_OUTPUT_FILE,
    INPUT_FILE,
    TEMP_INPUT_FILE
)
from scraper.llama_processor import LlamaProcessor

class BusinessSpider(scrapy.Spider):
    name = 'business_spider'
    
    def __init__(self, *args, **kwargs):
        super(BusinessSpider, self).__init__(*args, **kwargs)
        self.setup_logging()
        self.initialize_stats()
        self.setup_files()
        self.initialize_components()

    def setup_logging(self) -> None:
        """Configure spider-specific logging"""
        self.logger.setLevel('DEBUG')
        self.logger.info("Initializing BusinessSpider with enhanced logging")

    def initialize_stats(self) -> None:
        """Initialize statistics tracking"""
        self.stats = {
            'processed_businesses': 0,
            'successful_searches': 0,
            'failed_searches': 0,
            'valid_urls': 0,
            'invalid_urls': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'start_time': datetime.now().isoformat(),
            'businesses_total': 0,
            'businesses_remaining': 0
        }
        self.logger.info("Statistics tracking initialized")

    def setup_files(self) -> None:
        """Setup input and output file paths"""
        # Verificar y crear directorios necesarios
        output_dir = os.path.dirname(CSV_OUTPUT_FILE)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        self.output_file = CSV_OUTPUT_FILE
        
        # Para archivos subidos vía web, usamos el archivo temporal
        self.input_file = TEMP_INPUT_FILE
        if not os.path.exists(self.input_file):
            self.logger.warning(f"No input file found at {self.input_file}. Waiting for file upload...")
        
        self.logger.info(f"Output file path: {self.output_file}")

    def initialize_components(self) -> None:
        """Initialize spider components"""
        self.businesses = self._load_businesses()
        self.logger.info(f"Loaded {len(self.businesses)} businesses to process")
        
        self.llama_processor = LlamaProcessor()
        self.logger.info("LLaMA processor initialized")
        
        self.allowed_domains = self._load_allowed_domains()
        self.logger.info(f"Loaded {len(self.allowed_domains)} allowed domains")
        
        self.ddgs = DDGS()
        self.logger.info("DuckDuckGo search initialized")

    def _load_businesses(self) -> List[str]:
        """Load businesses from CSV file"""
        try:
            if not os.path.exists(self.input_file):
                self.logger.error("No input file available. Please upload a CSV file through the web interface.")
                return []
            
            self.logger.debug(f"Loading businesses from {self.input_file}")
            df = pd.read_csv(self.input_file)
            
            if 'business_name' not in df.columns:
                self.logger.error("CSV file must contain 'business_name' column")
                return []
            
            businesses = df['business_name'].tolist()
            self.stats['businesses_total'] = len(businesses)
            self.stats['businesses_remaining'] = len(businesses)
            
            self.logger.info(f"Successfully loaded {len(businesses)} businesses from CSV")
            return businesses
            
        except Exception as e:
            self.logger.error(f"Error loading businesses: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            return []

    def _load_allowed_domains(self) -> List[str]:
        """Load list of allowed domains for scraping"""
        domains = [
            'ny.gov', 'nyc.gov', 'yellowpages.com', 'bbb.org',
            'yelp.com', 'bloomberg.com', 'linkedin.com', 'manta.com'
        ]
        self.logger.debug(f"Loaded allowed domains: {domains}")
        return domains

    def _is_valid_url(self, url: str) -> bool:
        """Validate URL and domain"""
        try:
            self.logger.debug(f"Validating URL: {url}")
            
            if not validators.url(url):
                self.logger.debug(f"Invalid URL format: {url}")
                self.stats['invalid_urls'] += 1
                return False
            
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            
            is_allowed = any(allowed in domain for allowed in self.allowed_domains)
            if is_allowed:
                self.logger.debug(f"Valid domain found: {domain}")
                self.stats['valid_urls'] += 1
            else:
                self.logger.debug(f"Domain not in allowed list: {domain}")
                self.stats['invalid_urls'] += 1
            
            return is_allowed
            
        except Exception as e:
            self.logger.error(f"Error validating URL {url}: {str(e)}")
            self.stats['invalid_urls'] += 1
            return False

    @retry(tries=3, delay=2)
    def _duckduckgo_search(self, business_name: str) -> Dict[str, Any]:
        """Perform DuckDuckGo search with retry mechanism"""
        try:
            self.logger.info(f"Starting DuckDuckGo search for business: {business_name}")
            
            enhanced_query = self.llama_processor.enhance_query(business_name)
            self.logger.debug(f"Enhanced query: {enhanced_query}")
            
            results = list(self.ddgs.text(
                enhanced_query,
                region=DUCKDUCKGO_REGION,
                safesearch=DUCKDUCKGO_SAFESEARCH,
                timelimit="y1",
                max_results=DUCKDUCKGO_MAX_RESULTS,
                backend=DUCKDUCKGO_BACKEND
            ))
            
            self.logger.info(f"Successfully retrieved {len(results)} results")
            self.stats['successful_searches'] += 1
            
            # Log the first result to debug the structure
            if results:
                self.logger.debug(f"First result structure: {results[0]}")
            
            formatted_results = {
                'data': {
                    'result': {
                        'items': [
                            {
                                'url': result.get('link', result.get('url', '')),  # Try both 'link' and 'url'
                                'title': result.get('title', ''),
                                'description': result.get('body', result.get('snippet', ''))  # Try both 'body' and 'snippet'
                            }
                            for result in results
                            if result.get('link') or result.get('url')  # Only include results with a valid URL
                        ]
                    }
                }
            }
            
            return formatted_results
            
        except Exception as e:
            self.logger.error(f"Error in DuckDuckGo search for {business_name}: {str(e)}")
            self.stats['failed_searches'] += 1
            raise

    def start_requests(self):
        """Generate initial requests for each business"""
        self.logger.info("Starting requests generation")
        total_businesses = len(self.businesses)
        
        for index, business in enumerate(self.businesses, 1):
            try:
                self.logger.info(f"Processing business {index}/{total_businesses}: {business}")
                search_results = self._duckduckgo_search(business)
                
                if 'data' in search_results and 'result' in search_results['data']:
                    results = self.llama_processor.analyze_search_results(
                        search_results['data']['result']['items']
                    )
                    
                    valid_urls = 0
                    for result in results:
                        url = result.get('url')
                        if url and self._is_valid_url(url):
                            valid_urls += 1
                            self.logger.debug(f"Found valid URL for {business}: {url}")
                            yield scrapy.Request(
                                url=url,
                                callback=self.parse_business_page,
                                errback=self.handle_error,
                                meta={
                                    'business_name': business,
                                    'search_snippet': result.get('description', ''),
                                    'relevance_score': result.get('relevance_score', 0),
                                    'dont_redirect': True,
                                    'handle_httpstatus_list': [404, 403],
                                    'download_timeout': DUCKDUCKGO_TIMEOUT
                                },
                                dont_filter=False
                            )
                    
                    self.logger.info(f"Generated {valid_urls} requests for business: {business}")
                    self.stats['businesses_remaining'] -= 1
                
                self.stats['processed_businesses'] += 1
                
            except Exception as e:
                self.logger.error(f"Error processing business {business}: {str(e)}")
                self.logger.error("Stack trace:", exc_info=True)

    def parse_business_page(self, response):
        """Extract business information from the target page"""
        business_name = response.meta['business_name']
        self.logger.info(f"Parsing page for business: {business_name}")
        self.logger.debug(f"URL: {response.url}")
        
        try:
            search_snippet = response.meta['search_snippet']
            relevance_score = response.meta.get('relevance_score', 0)

        # Initialize data dictionary
            data = {
                'business_name': business_name,
                'address': None,
                'city': None,
                'state': None,
                'zip_code': None,
                'violation_type': None,
                'nsl_published_date': None,
                'nsl_effective_date': None,
                'remediated_date': None,
                'verified': False,
                'source_url': response.url,
                'relevance_score': relevance_score
            }

            # Extract address information
            address_found = False
            for selector in self.address_selectors:
                address_parts = response.xpath(selector).getall()
                if address_parts:
                    self.logger.debug(f"Found address using selector: {selector}")
                    address_text = ' '.join([part.strip() for part in address_parts if part.strip()])
                    if self._validate_ny_address(address_text):
                        data.update(self._parse_address(address_text))
                        data['verified'] = True
                        address_found = True
                        break

            if not address_found:
                self.logger.warning(f"No valid NY address found for business: {business_name}")

            # Extract dates and violation type
            self._extract_dates(response, data)
            self._extract_violation_type(response, data)

            self.logger.info(f"Successfully parsed data for business: {business_name}")
            self.stats['successful_scrapes'] += 1
            yield data

        except Exception as e:
            self.logger.error(f"Error parsing page for business {business_name}: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            self.stats['failed_scrapes'] += 1

    def handle_error(self, failure):
        """Handle request errors"""
        if failure.check(TimeoutError):
            self.logger.error(f"Timeout on {failure.request.url}")
        else:
            self.logger.error(f"Error on {failure.request.url}: {str(failure.value)}")

    def _validate_ny_address(self, address):
        """Validate if the address is in New York"""
        return NY_STATE_CODE in address.upper()

    def _parse_address(self, address_text):
        """Parse address string into components"""
        components = address_text.split(',')
        result = {
            'address': components[0].strip() if len(components) > 0 else None,
            'city': components[1].strip() if len(components) > 1 else None,
            'state': None,
            'zip_code': None
        }

        if len(components) > 2:
            state_zip = components[2].strip().split()
            if len(state_zip) > 0:
                result['state'] = state_zip[0]
            if len(state_zip) > 1:
                result['zip_code'] = state_zip[1]

        return result

    def _parse_date(self, date_string):
        """Parse date string into standard format"""
        try:
            return datetime.strptime(date_string.strip(), '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            try:
                return datetime.strptime(date_string.strip(), '%m/%d/%Y').strftime('%Y-%m-%d')
            except ValueError:
                return None 

    def closed(self, reason: str):
        """Called when spider is closed"""
        self.stats['end_time'] = datetime.now().isoformat()
        self.logger.info("Spider closing. Final statistics:")
        for stat, value in self.stats.items():
            self.logger.info(f"{stat}: {value}")
        
        # Guardar estadísticas en un archivo
        try:
            stats_file = f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            Path('stats').mkdir(parents=True, exist_ok=True)
            with open(f"stats/{stats_file}", 'w', encoding='utf-8') as f:
                import json
                json.dump(self.stats, f, indent=2)
            self.logger.info(f"Statistics saved to stats/{stats_file}")
        except Exception as e:
            self.logger.error(f"Error saving statistics: {str(e)}") 