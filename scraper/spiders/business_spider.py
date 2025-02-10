from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator, Union, TypeVar
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

import scrapy
import json
import pandas as pd
from retry import retry
import validators
import tldextract
from duckduckgo_search import DDGS
import os
import time
import random
from scrapy_splash import SplashRequest
import re
import logging
from urllib.parse import urlparse

from scraper.settings import (
    NY_STATE_CODE,
    DUCKDUCKGO_REGION,
    DUCKDUCKGO_SAFESEARCH,
    DUCKDUCKGO_MAX_RESULTS,
    DUCKDUCKGO_BACKEND,
    CSV_OUTPUT_FILE,
    TEMP_INPUT_FILE,
    SPLASH_LUA_SCRIPT,
    CACHE_CLEANUP_THRESHOLD,
    MAX_CACHE_SIZE
)
from scraper.llama_processor import LlamaProcessor
from scraper.address_extractor import AddressExtractor
from scraper.exceptions import ValidationError, DataExtractionError, AddressExtractionError
from scraper.validators import AddressValidator, BusinessValidator, URLValidator
from scraper.metrics import MetricsManager
from scraper.cache import CacheManager
from scraper.utils import clean_text
from ..cache.events import EventManager, EventType, EventPriority

T = TypeVar('T')

@dataclass
class SearchResult:
    """Estructura de datos para resultados de búsqueda."""
    title: str
    url: str
    description: str
    score: float = 0.0
    metadata: Dict[str, Any] = None

@dataclass
class BusinessData:
    """Estructura de datos para información de negocios."""
    name: str
    address: str
    state: str
    zip_code: str
    confidence: float
    source_url: str
    metadata: Dict[str, Any] = None

class BusinessSpider(scrapy.Spider):
    name = "business_spider"

    def __init__(self, state: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        super(BusinessSpider, self).__init__(*args, **kwargs)
        self.target_state = state.upper() if state else None
        self.setup_logging()
        self.initialize_stats()
        self.setup_files()
        self.initialize_components()
        self.results: List[Dict[str, Any]] = []

        if self.target_state:
            self.logger.info(f"Filtering results for state: {self.target_state}")

    def setup_logging(self) -> None:
        """Configure spider-specific logging"""
        self.logger.setLevel("DEBUG")
        self.logger.info("Initializing BusinessSpider with enhanced logging")

    def initialize_stats(self) -> None:
        """Initialize statistics tracking"""
        self.stats = {
            "processed_businesses": 0,
            "successful_searches": 0,
            "failed_searches": 0,
            "valid_urls": 0,
            "invalid_urls": 0,
            "successful_scrapes": 0,
            "failed_scrapes": 0,
            "start_time": datetime.now().isoformat(),
            "businesses_total": 0,
            "businesses_remaining": 0,
            "errors": {}
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
            self.logger.warning(
                f"No input file found at {self.input_file}. Waiting for file upload..."
            )

        self.logger.info(f"Output file path: {self.output_file}")

    def initialize_components(self) -> None:
        """Initialize spider components."""
        # Core components
        self.metrics = MetricsManager()
        self.cache_manager = CacheManager()
        self.address_extractor = AddressExtractor()
        self.url_validator = URLValidator()
        self.address_validator = AddressValidator()
        self.business_validator = BusinessValidator()
        
        # AI components
        self.is_ai_enabled = os.getenv('ENABLE_AI_FEATURES', 'false').lower() == 'true'
        if self.is_ai_enabled:
            self.llama_processor = LlamaProcessor()
            self.logger.info("LLaMA processor initialized")
        else:
            self.llama_processor = None
            self.logger.info("AI features disabled")

        # Search components
        self.allowed_domains = self._load_allowed_domains()
        self.logger.info(f"Loaded {len(self.allowed_domains)} allowed domains")

        # Initialize DuckDuckGo search
        self.ddgs = DDGS()
        self.logger.info("DuckDuckGo search initialized")

    def _load_businesses(self) -> List[str]:
        """Load businesses from CSV file"""
        try:
            if not os.path.exists(self.input_file):
                self.logger.error(
                    "No input file available. Please upload a CSV file through the web interface."
                )
                return []

            self.logger.debug(f"Loading businesses from {self.input_file}")
            df = pd.read_csv(self.input_file)

            if "business_name" not in df.columns:
                self.logger.error("CSV file must contain 'business_name' column")
                return []

            businesses = df["business_name"].tolist()
            self.stats["businesses_total"] = len(businesses)
            self.stats["businesses_remaining"] = len(businesses)

            self.logger.info(f"Successfully loaded {len(businesses)} businesses from CSV")
            return businesses

        except Exception as e:
            self.logger.error(f"Error loading businesses: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            return []

    def _load_allowed_domains(self) -> List[str]:
        """Load list of allowed domains for scraping"""
        domains = [
            "chamberofcommerce.com",
            "fda.gov",
            "yelp.com",
            "linkedin.com",
            "ny.gov",
            "nyc.gov",
            "mapquest.com",
            "yellowpages.com",
            "bbb.org",
            "bloomberg.com",
            "manta.com",
            "maps.google.com",
            "google.com",
        ]
        self.logger.debug(f"Loaded allowed domains: {domains}")
        return domains

    def _get_search_variations(self, business_name: str) -> List[str]:
        """Generate search variations for a business name."""
        # Variaciones base con palabras clave de dirección
        search_variations: List[str] = [
            f"{business_name} business address contact",
            f"{business_name} location headquarters",
            f"{business_name} store directions",
            f"{business_name} company address",
            f"{business_name} official address",
            f"{business_name} main office location",
            f"{business_name} contact us address",
            f"{business_name} physical location",
            f"{business_name} store locator",
        ]

        # Búsquedas específicas por sitio
        site_specific_variations: List[str] = [
            f"site:yelp.com {business_name} address",
            f"site:bbb.org {business_name} business details",
            f"site:chamberofcommerce.com {business_name} company info",
            f"site:yellowpages.com {business_name}",
            f"site:mapquest.com {business_name}",
            f"site:google.com/maps {business_name}",
            f"site:manta.com {business_name} business",
        ]

        # Variaciones específicas por estado si está definido
        if self.target_state:
            state_variations: List[str] = [
                f"{business_name} {self.target_state} address",
                f"{business_name} {self.target_state} location",
                f"site:ny.gov {business_name} business",
                f"site:nyc.gov {business_name} location",
                f"{business_name} in {self.target_state}",
            ]
            search_variations.extend(state_variations)

        # Añadir variaciones con el nombre del negocio entre comillas
        quoted_variations = [
            f'"{business_name}" address',
            f'"{business_name}" location',
            f'"{business_name}" contact information',
        ]
        search_variations.extend(quoted_variations)

        return site_specific_variations + search_variations

    def _process_search_query(self, query: str, all_results: List[Dict[str, Any]]) -> None:
        """Process a single search query."""
        try:
            # Try to get cached results first
            cached_results = self._get_cached_search(query)
            if cached_results:
                self.logger.info(f"Using cached results for query: {query}")
                all_results.extend(cached_results)
                return

            # Perform search using the initialized DDGS instance
            batch_results = list(self.ddgs.text(
                query,
                region=DUCKDUCKGO_REGION,
                safesearch=DUCKDUCKGO_SAFESEARCH,
                max_results=DUCKDUCKGO_MAX_RESULTS
            ))

            if batch_results:
                self.logger.info(f"Found {len(batch_results)} results for query: {query}")
                all_results.extend(batch_results)
                self._cache_search_results(query, batch_results)
                self._search_attempts = 0
            else:
                self._search_attempts = getattr(self, "_search_attempts", 0) + 1
                self.logger.warning(f"No results found for query: {query}")

        except Exception as e:
            self._handle_error(e, f"search query '{query}'", None)

    def _handle_search_error(self, e: Exception, query: str) -> bool:
        """Handle search error and return True if should retry."""
        self._handle_error(e, f"search query '{query}'", None)

        if "Ratelimit" in str(e):
            delay = 60 + (30 * getattr(self, "_search_attempts", 0))
            self.logger.warning(f"Rate limit detected, waiting {delay} seconds")
            time.sleep(delay)
            return True

        return False

    def _process_search_results(
        self, all_results: List[Dict[str, Any]], business_name: str
    ) -> Dict[str, Any]:
        """Process and deduplicate search results."""
        if not all_results:
            return self._create_empty_result(business_name)

        start_time = time.time()
        unique_results: Dict[str, Dict[str, Any]] = {}
        
        try:
            # Analyze results using LLaMA if available
            if self.llama_processor and self.is_ai_enabled:
                analyzed_results = self.llama_processor.analyze_search_results(all_results)
                for result in analyzed_results:
                    self._add_unique_result(result, business_name, unique_results)
            else:
                # Basic analysis without AI
                for result in all_results:
                    self._add_unique_result(result, business_name, unique_results)

            sorted_results = sorted(
                unique_results.values(),
                key=lambda x: x.get("relevance_score", 0),
                reverse=True
            )

            return self._create_result_response(
                sorted_results, business_name, time.time() - start_time
            )

        except Exception as e:
            self.logger.error(
                f"Error processing search results for {business_name}: {str(e)}",
                exc_info=True
            )
            return self._create_empty_result(business_name, str(e))

    def _add_unique_result(
        self, 
        result: Dict[str, Any], 
        business_name: str,
        unique_results: Dict[str, Dict[str, Any]]
    ) -> None:
        """Add a unique result to the collection."""
        if not result.get("url"):
            return

        url = result["url"]
        if url in unique_results:
            return

        if not self.url_validator.validate(url):
            self.stats["invalid_urls"] += 1
            return

        self.stats["valid_urls"] += 1
        result["relevance_score"] = self._calculate_relevance_score(result, business_name)
        unique_results[url] = result

    def _create_empty_result(
        self, 
        business_name: str, 
        error: Optional[str] = None
    ) -> Dict[str, Any]:
        """Crea un resultado vacío."""
        return {
            "data": {
                "result": {
                    "items": [],
                    "metadata": {
                        "business_name": business_name,
                        "total_results": 0,
                        "error": error,
                        "processing_time": 0
                    }
                }
            }
        }

    def _create_result_response(
        self,
        sorted_results: List[Dict[str, Any]],
        business_name: str,
        processing_time: float
    ) -> Dict[str, Any]:
        """Crea la respuesta con los resultados procesados."""
        return {
            "data": {
                "result": {
                    "items": sorted_results[:int(DUCKDUCKGO_MAX_RESULTS)],
                    "metadata": {
                        "business_name": business_name,
                        "total_results": len(sorted_results),
                        "processing_time": round(processing_time, 2),
                        "ai_enhanced": bool(self.llama_processor and self.is_ai_enabled)
                    }
                }
            }
        }

    def _process_and_yield_result(
        self, result: Dict[str, Any], business: str
    ) -> Generator[Dict[str, Any], None, bool]:
        """Process a search result and yield data if valid."""
        processed_data = self._process_business_result(result, business)
        if processed_data:
            self.stats["successful_scrapes"] += 1
            yield processed_data
            return True
        return False

    def _calculate_relevance_score(self, result: Dict[str, Any], business_name: str) -> float:
        """Calculate relevance score for a search result."""
        try:
            text = f"{result.get('title', '')} {result.get('description', '')}".lower()
            url = result.get('url', '').lower()
            business_terms = clean_text(business_name).lower().split()

            # Base score from business name matches
            matches = sum(1 for term in business_terms if term in text)
            base_score = min(matches / len(business_terms), 1.0) * 0.4

            # Score from keywords
            keywords = ["address", "location", "contact", "business", "company"]
            keyword_score = sum(0.1 for word in keywords if word in text) / len(keywords)

            # Score from trusted domains
            trusted_domains = {
                "high": ["bbb.org", "ny.gov", "nyc.gov"],
                "medium": ["yelp.com", "yellowpages.com", "chamberofcommerce.com"],
                "low": ["manta.com", "bizapedia.com"]
            }

            domain_score = 0.0
            for domain in trusted_domains["high"]:
                if domain in url:
                    domain_score += 0.3
                    break
            for domain in trusted_domains["medium"]:
                if domain in url:
                    domain_score += 0.2
                    break
            for domain in trusted_domains["low"]:
                if domain in url:
                    domain_score += 0.1
                    break

            final_score = min(base_score + keyword_score + domain_score, 1.0)
            return round(final_score, 3)

        except Exception as e:
            self.logger.warning(f"Error calculating relevance score: {str(e)}")
            return 0.1

    def _cleanup_cache(self) -> None:
        """Clean up cache when threshold is reached."""
        try:
            self.cache_manager.cleanup(
                max_size=self.max_cache_size,
                threshold=self.cache_cleanup_threshold
            )
            self.logger.debug("Cache cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during cache cleanup: {str(e)}")

    def _get_cached_search(self, business_name: str) -> Optional[Dict[str, Any]]:
        """Get cached search results."""
        return self.cache_manager.get(f"search:{business_name}")

    def _cache_search_results(self, business_name: str, results: Dict[str, Any]) -> None:
        """Cache search results."""
        try:
            self.cache_manager.set(f"search:{business_name}", results)
        except Exception as e:
            self.logger.error(f"Error caching search results: {str(e)}")

    def _get_cached_address(self, url: str) -> Optional[Dict[str, Any]]:
        """Get cached address."""
        return self.cache_manager.get(f"address:{url}")

    def _cache_address(self, url: str, address_data: Dict[str, Any]) -> None:
        """Cache address data."""
        try:
            if isinstance(address_data, dict):
                self.cache_manager.set(f"address:{url}", address_data)
        except Exception as e:
            self.logger.error(f"Error caching address: {str(e)}")

    def _handle_error(self, error: Exception, context: str, business_name: Optional[str] = None) -> None:
        """Manejo centralizado de errores."""
        error_msg = str(error)
        error_type = type(error).__name__
        error_details = {}

        if isinstance(error, ScraperBaseException):
            error_details = error.to_dict()
        else:
            error_details = {
                'type': error_type,
                'message': error_msg,
                'context': context,
                'business_name': business_name,
                'timestamp': datetime.now().isoformat()
            }

        # Actualizar estadísticas
        self.stats.setdefault("errors", {})
        self.stats["errors"][error_type] = self.stats["errors"].get(error_type, 0) + 1
        
        # Construir mensaje de error
        msg = f"Error in {context}"
        if business_name:
            msg += f" for business '{business_name}'"
        msg += f": {error_msg}"

        # Registrar error según su tipo
        if isinstance(error, (TimeoutError, ConnectionError)):
            self.logger.warning(msg)
        elif isinstance(error, (ValidationError, DataExtractionError)):
            self.logger.info(msg)
        else:
            self.logger.error(msg, exc_info=True)

        # Registrar métricas
        self.metrics.record_error(error_type)

        # Publicar evento de error
        self.event_manager.publish(
            EventType.ERROR,
            {
                'spider_name': self.name,
                'error_type': error_type,
                'error_message': error_msg,
                'error_details': error_details,
                'context': context,
                'business_name': business_name
            }
        )

    def handle_error(self, failure):
        """Handle request errors"""
        error = failure.value
        context = "request"
        
        if failure.check(TimeoutError):
            context = "request timeout"
        elif failure.check(ConnectionError):
            context = "connection error"
        elif failure.check(DNSLookupError):
            context = "DNS lookup error"
        elif failure.check(TooManyRedirectsError):
            context = "too many redirects"
        
        self._handle_error(error, context, None)
        
        # Retornar None para que Scrapy sepa que el error fue manejado
        return None

    def _handle_business_error(self, business: str, error: Exception) -> None:
        """Handle and log business processing errors."""
        self._handle_error(error, "business processing", business)

    def _handle_chamber_error(self, e: Exception, business_name: str) -> None:
        """Handle errors from Chamber of Commerce search."""
        self._handle_error(e, "Chamber of Commerce search", business_name)

    def _extract_chamber_address(
        self, address_text: str, business_name: str
    ) -> Optional[Dict[str, Any]]:
        """Extract and validate address from Chamber of Commerce text."""
        try:
            address_data = self.address_extractor.extract_address_from_text(address_text)

            if address_data and self._validate_state_match(address_data, business_name):
                result = {
                    "id": self.current_id,
                    "business_name": business_name,
                    "address": address_data.street,
                    "city": address_data.city,
                    "state": address_data.state,
                    "zip_code": address_data.zip_code,
                    "confidence_score": 0.9,  # Alta confianza por ser de Chamber
                    "source": "chamber_of_commerce",
                    "created_at": datetime.now().strftime("%Y-%m-%d"),
                }
                msg = f"Successfully found address for {business_name}"
                self.logger.info(f"{msg} in Chamber of Commerce")
                return result
        except (AttributeError, ValueError) as e:
            self.logger.warning(
                f"Error extracting address from Chamber of Commerce: {str(e)}"
            )
        return None

    def _parse_chamber_results(self, response: Any) -> Generator[Dict[str, Any], None, None]:
        """Parse Chamber of Commerce search results."""
        yield from self._parse_search_results(response, "chamber_of_commerce")

    def _parse_duckduckgo_results(self, response: Any) -> Generator[Dict[str, Any], None, None]:
        """Parse DuckDuckGo search results."""
        yield from self._parse_search_results(response, "duckduckgo")

    def _scrape_webpage(self, url: str, business_name: str) -> Optional[scrapy.Request]:
        """Create a request to scrape a webpage."""
        return SplashRequest(
            url=url,
            callback=self.parse_business_page,
            errback=self.handle_error,
            endpoint='execute',
            args={
                'lua_source': SPLASH_LUA_SCRIPT,
                'timeout': 90,
                'wait': 5,
            },
            meta={
                'business_name': business_name,
                'dont_redirect': True,
                'handle_httpstatus_list': [404, 403, 500],
            },
            dont_filter=True,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
        )

    def _search_chamber_of_commerce(self, business_name: str) -> Optional[scrapy.Request]:
        """Search for business address in Chamber of Commerce website."""
        try:
            # Format business name for URL (replace spaces with +)
            formatted_name = business_name.replace(" ", "+")
            
            # Construct the search URL
            search_url = (
                f"https://www.chamberofcommerce.com/search?what={formatted_name}&where="
            )
            
            # Create a SplashRequest with JavaScript rendering
            return self._scrape_webpage(search_url, business_name)
            
        except (ValueError, RuntimeError, ConnectionError) as e:
            self.logger.error(
                f"Error creating Chamber of Commerce request for {business_name}: {str(e)}"
            )
            return None

    def _parse_search_results(self, response: Any, source: str) -> Generator[Dict[str, Any], None, None]:
        """Parsea resultados de búsqueda de cualquier fuente."""
        try:
            business_name = response.meta["business_name"]
            self.logger.info(f"Parsing {source} results for: {business_name}")

            # Extraer dirección usando el extractor
            address_data = self.address_extractor.extract_address_from_text(response.text)
            if address_data and self._validate_state_match(address_data.__dict__, business_name):
                result = {
                    "business_name": business_name,
                    "address": address_data.street,
                    "city": address_data.city,
                    "state": address_data.state,
                    "zip_code": address_data.zip_code,
                    "source_url": response.url,
                    "confidence_score": 0.9 if source == "chamber_of_commerce" else 0.7,
                    "metadata": {
                        "source": source,
                        "extracted_at": datetime.now().isoformat()
                    }
                }
                self.logger.info(f"Successfully extracted address for {business_name}")
                yield result
                return

            # Si no se encontró dirección, proceder con DuckDuckGo
            self.logger.info(f"No address found in {source} for {business_name}, trying DuckDuckGo")
            search_results = self._duckduckgo_search(business_name)
            if self._has_valid_results(search_results):
                for result in search_results["data"]["result"]["items"]:
                    processed_result = self._process_business_result(result, business_name)
                    if processed_result:
                        if isinstance(processed_result, scrapy.Request):
                            yield processed_result
                        else:
                            yield processed_result
                        break

        except Exception as e:
            self.logger.error(
                f"Error parsing {source} results for {business_name}: {str(e)}",
                exc_info=True
            )

    def _validate_state_match(self, address_data: Dict[str, Any], business_name: str) -> bool:
        """Validate if address state matches target state."""
        if not self.target_state:
            return True
            
        validation_result = self.address_validator.validate_state(
            address_data.get('state', ''),
            self.target_state
        )
        
        if not validation_result.is_valid:
            self.logger.warning(
                f"State validation failed for {business_name}: {validation_result.error_message}"
            )
            return False
            
        return True

    def start_requests(self):
        """Generate initial requests for each business"""
        if not self.businesses:
            self.logger.warning("No businesses to process")
            return

        self.stats["businesses_remaining"] = len(self.businesses)
        self.current_id = 1

        for business in self.businesses:
            try:
                self.logger.info(
                    f"Processing business {self.stats['processed_businesses'] + 1}/"
                    f"{self.stats['businesses_total']}: {business}"
                )

                # Guardar el nombre del negocio actual para uso en cálculos de confianza
                self.business_name = business

                # Primero intentar Chamber of Commerce
                chamber_request = self._search_chamber_of_commerce(business)
                if chamber_request:
                    self.logger.info(f"Searching Chamber of Commerce for: {business}")
                    yield chamber_request
                    continue

                # Si no se pudo crear la solicitud para Chamber of Commerce, usar DuckDuckGo
                self.logger.info(f"Falling back to DuckDuckGo search for: {business}")
                search_results = self._duckduckgo_search(business)
                if not self._has_valid_results(search_results):
                    continue

                # Procesar resultados de DuckDuckGo
                for result in search_results["data"]["result"]["items"]:
                    processed_result = self._process_business_result(result, business)
                    if processed_result:
                        if isinstance(processed_result, scrapy.Request):
                            yield processed_result
                        else:
                            yield processed_result
                        break

                self._update_business_stats()

            except Exception as e:
                self._handle_business_error(business, e)

    def _has_valid_results(self, search_results: Dict[str, Any]) -> bool:
        """Check if search results are valid."""
        if not search_results or not search_results.get("data", {}).get("result", {}).get("items"):
            self.logger.warning(f"No search results found for business: {self.business_name}")
            return False
        return True

    def _update_business_stats(self) -> None:
        """Update business processing statistics."""
        self.stats["processed_businesses"] += 1
        self.stats["businesses_remaining"] -= 1

    def parse_business_page(self, response: Any) -> Generator[Dict[str, Any], None, None]:
        """Extract business information from the target page"""
        business_name = response.meta["business_name"]
        self.logger.info(f"Parsing page for business: {business_name}")
        self.logger.debug(f"URL: {response.url}")

        try:
            relevance_score = response.meta.get("relevance_score", 0)

            # Extract address using address extractor
            address_data = self.address_extractor.extract_address_from_text(response.text)
            if not address_data:
                self.logger.warning(f"No valid address found for business: {business_name}")
                return

            # Validate address
            validation_result = self.address_validator.validate(address_data.__dict__)
            
            if not validation_result.is_valid:
                self.logger.warning(
                    f"Address validation failed for {business_name}: {validation_result.error_message}"
                )
                return

            # Create result
            result = {
                "business_name": business_name,
                "address": address_data.street,
                "city": address_data.city,
                "state": address_data.state,
                "zip_code": address_data.zip_code,
                "source_url": response.url,
                "relevance_score": relevance_score,
                "verified": True,
                "metadata": {
                    "source": "webpage",
                    "extracted_at": datetime.now().isoformat()
                }
            }

            self.logger.info(f"Successfully parsed data for business: {business_name}")
            self.stats["successful_scrapes"] += 1
            yield result

        except Exception as e:
            self.logger.error(f"Error parsing page for business {business_name}: {str(e)}")
            self.logger.error("Stack trace:", exc_info=True)
            self.stats["failed_scrapes"] += 1

    def closed(self, reason: str):
        """Called when spider is closed"""
        self.stats["end_time"] = datetime.now().isoformat()
        self.logger.info("Spider closing. Final statistics:")
        for stat, value in self.stats.items():
            self.logger.info(f"{stat}: {value}")

        # Guardar estadísticas en un archivo
        try:
            stats_file = f"stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            Path("stats").mkdir(parents=True, exist_ok=True)
            with open(f"stats/{stats_file}", "w", encoding="utf-8") as f:
                json.dump(self.stats, f, indent=2)
            self.logger.info(f"Statistics saved to stats/{stats_file}")
        except Exception as e:
            self.logger.error(f"Error saving statistics: {str(e)}")

    @retry(tries=2, delay=60, backoff=2, jitter=(1, 3))
    def _duckduckgo_search(self, business_name: str) -> Dict[str, Any]:
        """Perform DuckDuckGo search with retry mechanism."""
        try:
            # Try Chamber of Commerce first
            chamber_result = self._search_chamber_of_commerce(business_name)
            if chamber_result:
                return {"data": {"result": {"items": [chamber_result]}}}
        except (ValueError, RuntimeError, ConnectionError) as e:
            self._handle_chamber_error(e, business_name)

        # If no result from Chamber of Commerce, proceed with DuckDuckGo search
        # Verificar caché primero
        cached_results = self._get_cached_search(business_name)
        if cached_results:
            self.logger.info(f"Using cached search results for: {business_name}")
            return cached_results

        try:
            return self._perform_duckduckgo_search(business_name)
        except (ValueError, RuntimeError, ConnectionError) as e:
            self.logger.error(f"Error in DuckDuckGo search for {business_name}: {str(e)}")
            self.stats["failed_searches"] += 1
            raise

    def _perform_duckduckgo_search(self, business_name: str) -> Dict[str, Any]:
        """Execute the DuckDuckGo search operation."""
        all_results: List[Dict[str, Any]] = []
        self._search_attempts = 0

        self.logger.info(f"Starting enhanced DuckDuckGo search for: {business_name}")
        
        # Obtener query mejorado usando LLaMA si está disponible
        if self.llama_processor:
            llama_response = self.llama_processor.enhance_query(business_name)
            if llama_response.success:
                search_query = llama_response.content
                self.logger.info(f"Using AI-enhanced query: {search_query}")
            else:
                search_query = business_name
                self.logger.warning(f"Using basic query due to AI error: {llama_response.error}")
        else:
            search_query = business_name
            self.logger.info("Using basic query (AI disabled)")

        search_variations = self._get_search_variations(search_query)

        # Dividir las variaciones en grupos más pequeños
        variation_groups = [
            search_variations[i : i + 3] for i in range(0, len(search_variations), 3)
        ]

        for group in variation_groups:
            try:
                for query in group:
                    try:
                        self._process_search_query(query, all_results)
                        if len(all_results) >= int(DUCKDUCKGO_MAX_RESULTS):
                            self.logger.info("Sufficient results found, stopping search")
                            break
                    except (ValueError, RuntimeError, ConnectionError) as e:
                        if not self._handle_search_error(e, query):
                            continue

                # Pausa entre grupos de búsqueda
                if len(all_results) < int(DUCKDUCKGO_MAX_RESULTS):
                    time.sleep(random.uniform(15, 25))

            except (ValueError, RuntimeError, ConnectionError) as e:
                self.logger.error(f"Error processing search group: {str(e)}", exc_info=True)
                continue

        results = self._process_search_results(all_results, business_name)
        self._cache_search_results(business_name, results)
        return results

    def _process_business_result(
        self, result: Dict[str, Any], business: str
    ) -> Optional[Union[Dict[str, Any], scrapy.Request]]:
        """Process a single search result with improved error handling."""
        try:
            # Validate result structure
            if not isinstance(result, dict):
                raise ValidationError("Result must be a dictionary")
            
            # Create SearchResult object
            search_result = SearchResult(
                title=result.get('title', ''),
                url=result.get('url', ''),
                description=result.get('description', ''),
                score=result.get('relevance_score', 0.0),
                metadata={'business_name': business}
            )
            
            # Validate URL
            if not self.url_validator.validate(search_result.url):
                self.logger.warning(f"Invalid URL: {search_result.url}")
                return None
            
            # Try to extract address from result
            try:
                # Check cache first
                cached_address = self._get_cached_address(search_result.url)
                if cached_address:
                    self.logger.debug(f"Using cached address for URL: {search_result.url}")
                    address_data = cached_address
                else:
                    # Extract address using address extractor
                    combined_text = f"{search_result.title}\n{search_result.description}"
                    address_data = self.address_extractor.extract_address_from_text(combined_text)
                    if address_data:
                        self._cache_address(search_result.url, address_data)

                if address_data:
                    # Validate address
                    validation_result = self.address_validator.validate(address_data.__dict__)
                    if validation_result.is_valid and self._validate_state_match(address_data.__dict__, business):
                        # Create and validate BusinessData
                        business_data = BusinessData(
                            name=business,
                            address=address_data.street,
                            state=address_data.state,
                            zip_code=address_data.zip_code,
                            confidence=validation_result.confidence_score,
                            source_url=search_result.url,
                            metadata={
                                'source': 'search_result',
                                'extracted_at': datetime.now().isoformat()
                            }
                        )
                        
                        # Validate business data
                        business_validation = self.business_validator.validate(business_data.__dict__)
                        if business_validation.is_valid:
                            return business_data.__dict__
                        else:
                            self.logger.warning(
                                f"Business validation failed for {business}: {business_validation.error_message}"
                            )
                            return None
            except DataExtractionError as e:
                self.logger.warning(f"Error extracting address: {str(e)}")
            
            # If no address found, try scraping
            if search_result.url:
                return self._scrape_webpage(search_result.url, business)
            
        except Exception as e:
            self.logger.error(
                f"Error processing result for {business}: {str(e)}",
                exc_info=True
            )
            return None
