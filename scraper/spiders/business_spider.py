import scrapy
import json
import pandas as pd
import requests
from urllib.parse import quote, urlparse
from datetime import datetime
from retry import retry
import validators
import tldextract
from scraper.settings import QWANT_API_URL, QWANT_API_KEY, NY_STATE_CODE
from scraper.llama_processor import LlamaProcessor

class BusinessSpider(scrapy.Spider):
    name = 'business_spider'
    
    def __init__(self, *args, **kwargs):
        super(BusinessSpider, self).__init__(*args, **kwargs)
        self.logger.info("Initializing BusinessSpider")
        self.businesses = self._load_businesses()
        self.logger.info(f"Loaded {len(self.businesses)} businesses to process")
        self.llama_processor = LlamaProcessor()
        self.logger.info("LLaMA processor initialized")
        self.allowed_domains = self._load_allowed_domains()
        self.logger.info(f"Loaded {len(self.allowed_domains)} allowed domains")
        self.stats = {
            'processed_businesses': 0,
            'successful_searches': 0,
            'failed_searches': 0,
            'valid_urls': 0,
            'invalid_urls': 0,
            'successful_scrapes': 0,
            'failed_scrapes': 0
        }
        
    def _load_businesses(self):
        """Load businesses from CSV file"""
        try:
            self.logger.debug("Attempting to load businesses from CSV")
            df = pd.read_csv('businesses.csv')
            businesses = df['business_name'].tolist()
            self.logger.info(f"Successfully loaded {len(businesses)} businesses from CSV")
            return businesses
        except Exception as e:
            self.logger.error(f"Error loading businesses: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            return []

    def _load_allowed_domains(self):
        """Load list of allowed domains for scraping"""
        return [
            'ny.gov', 'nyc.gov', 'yellowpages.com', 'bbb.org',
            'yelp.com', 'bloomberg.com', 'linkedin.com', 'manta.com'
        ]

    def _is_valid_url(self, url):
        """Validate URL and domain"""
        try:
            self.logger.debug(f"Validating URL: {url}")
            
            # Basic URL validation
            if not validators.url(url):
                self.logger.debug(f"Invalid URL format: {url}")
                self.stats['invalid_urls'] += 1
                return False
            
            # Extract domain
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            
            # Check if domain is allowed
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
    def _qwant_search(self, business_name):
        """Perform Qwant API search with retry mechanism"""
        try:
            self.logger.info(f"Starting Qwant search for business: {business_name}")
            
            # Enhance query using LLaMA
            enhanced_query = self.llama_processor.enhance_query(business_name)
            self.logger.debug(f"Enhanced query: {enhanced_query}")
            
            headers = {
                'X-API-Key': QWANT_API_KEY,
                'Accept': 'application/json'
            }
            params = {
                'q': enhanced_query,
                'count': 10,
                'locale': 'en_US',
                't': 'web'
            }
            
            self.logger.debug(f"Making API request to Qwant with params: {params}")
            response = requests.get(
                QWANT_API_URL,
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Successfully retrieved {len(data.get('data', {}).get('result', {}).get('items', []))} results")
            self.stats['successful_searches'] += 1
            return data
            
        except requests.Timeout:
            self.logger.error(f"Timeout while searching for {business_name}")
            self.stats['failed_searches'] += 1
            raise
        except requests.RequestException as e:
            self.logger.error(f"Error in Qwant search for {business_name}: {str(e)}")
            self.stats['failed_searches'] += 1
            raise

    def start_requests(self):
        """Generate initial requests for each business"""
        self.logger.info("Starting requests generation")
        total_businesses = len(self.businesses)
        
        for index, business in enumerate(self.businesses, 1):
            try:
                self.logger.info(f"Processing business {index}/{total_businesses}: {business}")
                search_results = self._qwant_search(business)
                
                if 'data' in search_results and 'result' in search_results['data']:
                    # Use LLaMA to analyze and score results
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
                                    'download_timeout': 30
                                },
                                dont_filter=False
                            )
                    
                    self.logger.info(f"Generated {valid_urls} requests for business: {business}")
                
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

    def closed(self, reason):
        """Called when spider is closed"""
        self.logger.info("Spider closing. Final statistics:")
        for stat, value in self.stats.items():
            self.logger.info(f"{stat}: {value}") 