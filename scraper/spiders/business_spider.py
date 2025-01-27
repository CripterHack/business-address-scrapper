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
        self.businesses = self._load_businesses()
        self.llama_processor = LlamaProcessor()
        self.allowed_domains = self._load_allowed_domains()
        
    def _load_businesses(self):
        """Load businesses from CSV file"""
        try:
            df = pd.read_csv('businesses.csv')
            return df['business_name'].tolist()
        except Exception as e:
            self.logger.error(f"Error loading businesses: {e}")
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
            # Basic URL validation
            if not validators.url(url):
                return False
            
            # Extract domain
            ext = tldextract.extract(url)
            domain = f"{ext.domain}.{ext.suffix}"
            
            # Check if domain is allowed
            return any(allowed in domain for allowed in self.allowed_domains)
        except Exception:
            return False

    @retry(tries=3, delay=2)
    def _qwant_search(self, business_name):
        """Perform Qwant API search with retry mechanism"""
        try:
            # Enhance query using LLaMA
            enhanced_query = self.llama_processor.enhance_query(business_name)
            
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
            
            response = requests.get(
                QWANT_API_URL,
                headers=headers,
                params=params,
                timeout=30  # 30 seconds timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.Timeout:
            self.logger.error(f"Timeout while searching for {business_name}")
            raise
        except requests.RequestException as e:
            self.logger.error(f"Error in Qwant search: {e}")
            raise

    def start_requests(self):
        """Generate initial requests for each business"""
        for business in self.businesses:
            try:
                search_results = self._qwant_search(business)
                
                if 'data' in search_results and 'result' in search_results['data']:
                    # Use LLaMA to analyze and score results
                    results = self.llama_processor.analyze_search_results(
                        search_results['data']['result']['items']
                    )
                    
                    for result in results:
                        url = result.get('url')
                        if url and self._is_valid_url(url):
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
            except Exception as e:
                self.logger.error(f"Error processing business {business}: {e}")

    def parse_business_page(self, response):
        """Extract business information from the target page"""
        business_name = response.meta['business_name']
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

        # Extract address information using various selectors
        address_selectors = [
            '//address//text()',
            '//*[contains(@itemtype, "PostalAddress")]//text()',
            '//*[contains(@class, "address")]//text()',
            '//*[contains(@class, "location")]//text()',
            '//p[contains(text(), "Address")]//following-sibling::p[1]//text()'
        ]

        for selector in address_selectors:
            address_parts = response.xpath(selector).getall()
            if address_parts:
                # Process and validate address
                address_text = ' '.join([part.strip() for part in address_parts if part.strip()])
                if self._validate_ny_address(address_text):
                    data.update(self._parse_address(address_text))
                    data['verified'] = True
                    break

        # Extract dates if available
        date_selectors = {
            'nsl_published_date': [
                '//*[contains(text(), "Published") or contains(text(), "published")]//following::text()[1]',
                '//*[contains(@class, "publish-date")]//text()'
            ],
            'nsl_effective_date': [
                '//*[contains(text(), "Effective") or contains(text(), "effective")]//following::text()[1]',
                '//*[contains(@class, "effective-date")]//text()'
            ],
            'remediated_date': [
                '//*[contains(text(), "Remediated") or contains(text(), "remediated")]//following::text()[1]',
                '//*[contains(@class, "remediation-date")]//text()'
            ]
        }

        for date_field, selectors in date_selectors.items():
            for selector in selectors:
                date_text = response.xpath(selector).get()
                if date_text:
                    try:
                        data[date_field] = self._parse_date(date_text)
                        if data[date_field]:  # If successfully parsed
                            break
                    except ValueError:
                        continue

        # Extract violation type
        violation_selectors = [
            '//*[contains(text(), "Violation") or contains(text(), "violation")]//following::text()[1]',
            '//*[contains(@class, "violation-type")]//text()'
        ]

        for selector in violation_selectors:
            violation_text = response.xpath(selector).get()
            if violation_text:
                data['violation_type'] = violation_text.strip()
                break

        yield data

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