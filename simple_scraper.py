"""Multi-source address scraper with enhanced anti-detection and performance optimization."""

import os
import sys
import random
import time
from typing import List, Dict, Tuple, Optional, Generator
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
import platform
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading
from datetime import datetime
import psutil
import signal
import subprocess
import re
import requests
import zipfile
import io
import shutil

# Load environment variables
load_dotenv()

# Create logs directory if it doesn't exist
Path('logs').mkdir(exist_ok=True)

# Configure logging with more detailed format
logging.basicConfig(
    level=getattr(logging, os.environ.get('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_chrome_version():
    """Get installed Chrome version."""
    try:
        system = platform.system()
        if system == "Windows":
            try:
                # Intentar obtener la versión directamente del directorio de instalación
                chrome_paths = [
                    r"C:\Program Files\Google\Chrome\Application",
                    r"C:\Program Files (x86)\Google\Chrome\Application",
                    r"C:\Users\%USERNAME%\AppData\Local\Google\Chrome\Application"
                ]
                
                for path in chrome_paths:
                    path = os.path.expandvars(path)
                    if os.path.exists(path):
                        try:
                            # Usar dir para listar directorios
                            cmd = f'dir /B /AD "{path}"'
                            output = os.popen(cmd).read()
                            versions = [line.strip() for line in output.split('\n') if line.strip() and any(c.isdigit() for c in line)]
                            
                            if versions:
                                version = max(versions, key=lambda x: [int(i) for i in x.split('.') if i.isdigit()])
                                logger.info(f"Chrome version detected from directory: {version}")
                                return version
                        except Exception as e:
                            logger.warning(f"Error reading Chrome directory {path}: {str(e)}")
                            continue
                
                # Si no se encuentra en los directorios, intentar con el registro usando reg query
                try:
                    cmd = 'reg query "HKLM\\SOFTWARE\\Wow6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\Google Chrome" /v Version'
                    output = os.popen(cmd).read()
                    if "Version" in output:
                        version = output.strip().split()[-1]
                        logger.info(f"Chrome version detected from registry: {version}")
                        return version
                except Exception as e:
                    logger.warning(f"Error reading Chrome version from registry: {str(e)}")
                
            except Exception as e:
                logger.warning(f"Error detecting Chrome version on Windows: {str(e)}")
                
        elif system == "Linux":
            try:
                output = subprocess.check_output(['google-chrome', '--version'])
                return output.decode('utf-8').strip().split()[-1]
            except:
                try:
                    output = subprocess.check_output(['chromium', '--version'])
                    return output.decode('utf-8').strip().split()[-1]
                except:
                    pass
                
        elif system == "Darwin":  # macOS
            try:
                output = subprocess.check_output(['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '--version'])
                return output.decode('utf-8').strip().split()[-1]
            except:
                pass
                
        # Si todo falla, usar una versión estable conocida
        logger.warning("Using default stable Chrome version")
        return "120.0.6099.130"
        
    except Exception as e:
        logger.warning(f"Error in get_chrome_version: {str(e)}")
        return "120.0.6099.130"

class PerformanceMonitor:
    """Monitor and manage system resources."""
    
    def __init__(self):
        self.max_memory_percent = float(os.environ.get('MAX_MEMORY_PERCENT', 80))
        self.max_cpu_percent = float(os.environ.get('MAX_CPU_PERCENT', 70))
        self._stop_event = threading.Event()
        self._monitor_thread = None
    
    def start_monitoring(self):
        """Start resource monitoring in background thread."""
        self._monitor_thread = threading.Thread(target=self._monitor_resources)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
    
    def stop_monitoring(self):
        """Stop resource monitoring."""
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join()
    
    def _monitor_resources(self):
        """Monitor system resources and log warnings."""
        while not self._stop_event.is_set():
            try:
                memory_percent = psutil.virtual_memory().percent
                cpu_percent = psutil.cpu_percent(interval=1)
                
                if memory_percent > self.max_memory_percent:
                    logger.warning(f"High memory usage: {memory_percent}%")
                if cpu_percent > self.max_cpu_percent:
                    logger.warning(f"High CPU usage: {cpu_percent}%")
                
                time.sleep(5)  # Check every 5 seconds
            except Exception as e:
                logger.error(f"Error monitoring resources: {str(e)}")

class Environment:
    """Environment detection and configuration."""
    
    @staticmethod
    def is_running_in_container() -> bool:
        """Detect if running inside a container."""
        indicators = [
            Path('/.dockerenv').exists(),
            Path('/run/.containerenv').exists(),
            os.environ.get('EXECUTION_ENV') == 'container'
        ]
        return any(indicators)
    
    @staticmethod
    def get_chrome_options(is_container: bool) -> webdriver.ChromeOptions:
        """Get Chrome options based on environment."""
        options = webdriver.ChromeOptions()
        
        # Common anti-detection options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-notifications')
        options.add_argument('--start-maximized')  # Asegurar que la ventana esté maximizada
        options.add_argument('--enable-unsafe-swiftshader')
        
        # Performance optimizations
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-features=site-per-process')
        options.add_argument('--disable-web-security')
        options.add_argument('--dns-prefetch-disable')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-sync')
        options.add_argument('--disable-translate')
        options.add_argument('--hide-scrollbars')
        options.add_argument('--metrics-recording-only')
        options.add_argument('--mute-audio')
        options.add_argument('--no-first-run')
        options.add_argument('--safebrowsing-disable-auto-update')
        
        # Enable network logging
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        # Container-specific options
        if is_container:
            options.add_argument('--no-sandbox')
            options.add_argument('--headless=new')
            
            chrome_binary = os.environ.get('CHROME_BINARY_PATH')
            if chrome_binary:
                options.binary_location = chrome_binary
        else:
            if platform.system() == 'Windows':
                options.add_argument('--disable-gpu')
            
            # Removido el modo headless por defecto
            # if os.environ.get('HEADLESS_MODE', 'true').lower() == 'true':
            #     options.add_argument('--headless=new')
        
        return options
    
    @staticmethod
    def get_chrome_service(is_container: bool) -> ChromeService:
        """Get ChromeDriver service based on environment."""
        try:
            if is_container:
                driver_path = os.environ.get('CHROME_DRIVER_PATH', '/usr/bin/chromedriver')
                return ChromeService(executable_path=driver_path)
            else:
                try:
                    # Detectar versión de Chrome
                    chrome_version = get_chrome_version()
                    logger.info(f"Detected Chrome version: {chrome_version}")
                    
                    try:
                        # Crear directorio para ChromeDriver si no existe
                        driver_dir = os.path.join(os.path.expanduser("~"), '.wdm', 'drivers', 'chromedriver', 'win64', '132.0.6834.160')
                        os.makedirs(driver_dir, exist_ok=True)
                        
                        driver_path = os.path.join(driver_dir, 'chromedriver.exe')
                        
                        # Si el driver ya existe, verificar si funciona
                        if os.path.exists(driver_path):
                            try:
                                service = ChromeService(executable_path=driver_path)
                                driver = webdriver.Chrome(service=service)
                                driver.quit()
                                logger.info(f"Using existing ChromeDriver at: {driver_path}")
                                return service
                            except Exception:
                                logger.info("Existing ChromeDriver not working, downloading new one")
                                if os.path.exists(driver_path):
                                    os.remove(driver_path)
                        
                        # URL directa al ChromeDriver 132
                        url = "https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/132.0.6834.160/win64/chromedriver-win64.zip"
                        
                        logger.info(f"Downloading ChromeDriver from: {url}")
                        response = requests.get(url, stream=True)
                        response.raise_for_status()
                        
                        # Extraer el archivo zip
                        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                            # El archivo chromedriver.exe está dentro de un subdirectorio
                            chrome_driver_path = None
                            for file in zip_file.namelist():
                                if file.endswith('chromedriver.exe'):
                                    chrome_driver_path = file
                                    break
                            
                            if chrome_driver_path:
                                with zip_file.open(chrome_driver_path) as source, open(driver_path, 'wb') as target:
                                    shutil.copyfileobj(source, target)
                            else:
                                raise Exception("chromedriver.exe not found in zip file")
                        
                        # Asignar permisos de ejecución
                        os.chmod(driver_path, 0o755)
                        
                        logger.info(f"ChromeDriver installed at: {driver_path}")
                        return ChromeService(executable_path=driver_path)
                            
                    except Exception as install_error:
                        logger.warning(f"Error installing ChromeDriver: {str(install_error)}")
                        raise
                        
                except Exception as e:
                    logger.error(f"Error setting up ChromeDriver service: {str(e)}")
                    raise
                
        except Exception as e:
            logger.error(f"Critical error setting up ChromeDriver service: {str(e)}")
            raise

class MultiSourceScraper:
    """Scraper for business addresses using multiple sources with enhanced anti-detection."""

    # Load configuration from environment variables
    MAX_RETRIES = int(os.environ.get('MAX_RETRIES', 3))
    CAPTCHA_WAIT_TIME = int(os.environ.get('CAPTCHA_WAIT_TIME', 300))
    CAPTCHA_CHECK_INTERVAL = int(os.environ.get('CAPTCHA_CHECK_INTERVAL', 2))
    MAX_CAPTCHA_ATTEMPTS = int(os.environ.get('MAX_CAPTCHA_ATTEMPTS', 2))
    MAX_WORKERS = int(os.environ.get('SCRAPER_THREADS', 4))
    BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 50))
    
    CLOUDFLARE_INDICATORS = [
        "challenge-running",
        "challenge-form",
        "_cf-challenge",
        "cf_challenge",
        "cf-please-wait",
        "cf_chl_prog",
        "turnstile",
        "cf-browser-verification",
        "cf_challenge-stage"
    ]

    def __init__(self):
        self.results: List[Dict[str, str]] = []
        self.results_lock = threading.Lock()
        self.performance_monitor = PerformanceMonitor()
        self.current_url = None
        self.last_page_source = None
        self.captcha_attempts = 0
        self.cloudflare_blocked = False
        self.driver = None
        self.ua = None
        self.is_container = Environment.is_running_in_container()
        self.initialize()
        
        logger.info(f"Initializing scraper in {'container' if self.is_container else 'local'} environment")
        logger.info(f"Using {self.MAX_WORKERS} worker threads")

    def initialize(self):
        """Initialize scraper components."""
        self.performance_monitor.start_monitoring()
        # Solo registrar señales en el thread principal
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.handle_shutdown)
            signal.signal(signal.SIGTERM, self.handle_shutdown)
        self.initialize_user_agent()

    def handle_shutdown(self, signum, frame):
        """Handle graceful shutdown on signals."""
        logger.info("Shutdown signal received, cleaning up...")
        self.performance_monitor.stop_monitoring()
        self.clean_up()
        sys.exit(0)

    def initialize_user_agent(self):
        """Initialize UserAgent with error handling."""
        try:
            self.ua = UserAgent()
        except Exception as e:
            logger.warning(f"Could not initialize UserAgent, using default: {str(e)}")
            self.ua = None

    def setup_driver(self):
        """Set up a new Chrome WebDriver instance with environment-specific configuration."""
        try:
            options = Environment.get_chrome_options(self.is_container)
            
            # Add random user agent if available
            if self.ua:
                new_user_agent = self.ua.random
                options.add_argument(f'user-agent={new_user_agent}')
                logger.info(f"Using new User-Agent: {new_user_agent}")
            
            service = Environment.get_chrome_service(self.is_container)
            self.driver = webdriver.Chrome(service=service, options=options)
            self._apply_anti_detection_measures()
            
            logger.info("WebDriver setup completed successfully")
            
        except Exception as e:
            logger.error(f"Error setting up WebDriver: {str(e)}")
            raise

    def _apply_anti_detection_measures(self):
        """Apply additional anti-detection measures."""
        if not self.driver:
            return

        user_agent = self.ua.random if self.ua else 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/112.0.0.0'
        self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": user_agent,
            "platform": "Win32",
            "acceptLanguage": "en-US,en;q=0.9"
        })
        
        js_patches = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
        """
        self.driver.execute_script(js_patches)

    def restart_browser(self):
        """Cierra y reinicia el navegador con nueva identidad."""
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            logger.warning(f"Error al cerrar el navegador: {str(e)}")
        finally:
            self.driver = None
            self.current_url = None
            self.last_page_source = None
            self.captcha_attempts = 0
            self.cloudflare_blocked = False
            
            # Esperar un tiempo aleatorio antes de reiniciar
            time.sleep(random.uniform(2, 5))
            
            # Configurar nuevo navegador
            self.setup_driver()
            logger.info("Navegador reiniciado con nueva identidad")

    def clean_up(self):
        """Limpia recursos y cierra el navegador."""
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            logger.warning(f"Error durante la limpieza: {str(e)}")
        finally:
            self.driver = None

    def is_cloudflare_challenge(self):
        """Check if current page is a Cloudflare challenge."""
        try:
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            # Verificar indicadores de Cloudflare en la URL
            cloudflare_url_indicators = [
                "challenge",
                "captcha",
                "cf_chl",
                "cloudflare",
                "turnstile",
                "cf-please-wait"
            ]
            
            for indicator in cloudflare_url_indicators:
                if indicator in current_url:
                    logger.info(f"Detectado Cloudflare por URL ({indicator})")
                    return True
            
            # Verificar indicadores en el contenido de la página
            cloudflare_content_indicators = [
                "checking if the site connection is secure",
                "checking your browser",
                "please wait...",
                "please stand by",
                "verify you are a human",
                "enable javascript and cookies",
                "please enable cookies",
                "one more step",
                "please verify you are a human",
                "just a moment",
                "security check to access",
                "i am human",
                "i am not a robot",
                "human verification"
            ]
            
            for indicator in cloudflare_content_indicators:
                if indicator in page_source:
                    logger.info(f"Detectado Cloudflare por contenido ({indicator})")
                    return True
            
            # Verificar elementos específicos de Cloudflare
            cloudflare_elements = [
                "iframe[src*='challenges']",
                "#challenge-form",
                "#challenge-running",
                "#cf-please-wait",
                "[data-translate='challenge']",
                "[class*='cf-']",
                "#turnstile-wrapper",
                "#cf-content",
                "#cf_challenge-stage"
            ]
            
            for selector in cloudflare_elements:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"Detectado Cloudflare por elemento ({selector})")
                        return True
                except:
                    continue
            
            # Verificar si la página está cargando indefinidamente
            try:
                ready_state = self.driver.execute_script("return document.readyState")
                if ready_state != "complete":
                    logger.info("Página no completamente cargada, posible bloqueo")
                    return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.warning(f"Error checking Cloudflare challenge: {str(e)}")
            return True  # En caso de error, asumimos que hay challenge para ser seguros

    def check_http_errors(self):
        """Verificar errores HTTP en los logs de rendimiento."""
        try:
            # Primero verificar si hay contenido útil en la página
            try:
                page_source = self.driver.page_source.lower()
                content_indicators = [
                    'business-listing',
                    'search-results',
                    'address',
                    'location',
                    'contact-info',
                    'business-card',
                    'company-info',
                    'search-result',
                    'business-unit'
                ]
                
                if any(indicator in page_source for indicator in content_indicators):
                    # Si encontramos contenido útil, definitivamente no es un error
                    return False
            except:
                pass
            
            # Solo verificar errores HTTP si no encontramos contenido útil
            logs = self.driver.get_log('performance')
            error_count = 0
            total_requests = 0
            
            for entry in logs:
                try:
                    if 'message' in entry:
                        message = entry['message']
                        if '"Network.responseReceived"' in message:
                            message_dict = json.loads(message)
                            response = message_dict.get('message', {}).get('params', {}).get('response', {})
                            status = response.get('status')
                            url = response.get('url', '')
                            
                            # Solo contar requests a los dominios principales
                            if 'chamberofcommerce.com' in url or 'trustpilot.com' in url:
                                total_requests += 1
                                if status in [401, 403, 429]:
                                    error_count += 1
                except:
                    continue
            
            # Solo considerar como error si hay un alto porcentaje de errores
            if total_requests > 0 and (error_count / total_requests) > 0.5:
                logger.warning(f"Detectado alto porcentaje de errores HTTP: {error_count}/{total_requests}")
                self.cloudflare_blocked = True
                return True
                
            return False
            
        except Exception as e:
            logger.warning(f"Error al verificar logs HTTP: {str(e)}")
            return False

    def wait_for_manual_captcha(self):
        """Esperar a que el usuario resuelva el CAPTCHA manualmente."""
        if self.captcha_attempts >= self.MAX_CAPTCHA_ATTEMPTS:
            logger.warning("Máximo número de intentos de CAPTCHA alcanzado")
            self.cloudflare_blocked = True
            return False
        
        if self.cloudflare_blocked:
            logger.warning("Detectado bloqueo persistente de Cloudflare")
            return False
        
        self.captcha_attempts += 1
        logger.info(f"Intento de CAPTCHA #{self.captcha_attempts} de {self.MAX_CAPTCHA_ATTEMPTS}")
        
        logger.info("Detectado CAPTCHA/Challenge - Esperando resolución manual...")
        print(f"\n¡ATENCIÓN! Por favor, resuelve el CAPTCHA/Challenge manualmente (Intento {self.captcha_attempts}/{self.MAX_CAPTCHA_ATTEMPTS}).")
        print(f"Tienes {self.CAPTCHA_WAIT_TIME} segundos para resolverlo.")
        
        start_time = time.time()
        self.last_page_source = self.driver.page_source
        last_check_time = time.time()
        
        while time.time() - start_time < self.CAPTCHA_WAIT_TIME:
            try:
                current_time = time.time()
                
                # Verificar errores HTTP
                if self.check_http_errors():
                    logger.warning("Detectados errores HTTP - posible bloqueo")
                    return False
                
                # Solo verificar cada 2 segundos para evitar sobrecarga
                if current_time - last_check_time < self.CAPTCHA_CHECK_INTERVAL:
                    time.sleep(0.5)
                    continue
                
                last_check_time = current_time
                current_page = self.driver.page_source
                
                # Si el contenido cambió, verificar si pasamos el CAPTCHA
                if current_page != self.last_page_source:
                    # Esperar a que la página se estabilice
                    time.sleep(3)
                    
                    # Verificar si hay elementos de búsqueda visibles
                    try:
                        search_indicators = [
                            "address.card-text",
                            "div.search-results",
                            ".business-listing",
                            ".search-result",
                            "div.address"
                        ]
                        
                        for indicator in search_indicators:
                            if self.driver.find_elements(By.CSS_SELECTOR, indicator):
                                logger.info("CAPTCHA/Challenge resuelto - Elementos de búsqueda encontrados")
                                return True
                        
                        # Si no encontramos elementos de búsqueda, verificar si seguimos en el CAPTCHA
                        if self.is_cloudflare_challenge():
                            logger.info("Aún en pantalla de CAPTCHA")
                            self.last_page_source = current_page
                            continue
                        
                    except Exception as e:
                        logger.warning(f"Error verificando elementos de búsqueda: {str(e)}")
                
                self.last_page_source = current_page
                
            except Exception as e:
                logger.warning(f"Error al verificar CAPTCHA/Challenge: {str(e)}")
            
            time.sleep(self.CAPTCHA_CHECK_INTERVAL)
        
        logger.warning("Tiempo de espera del CAPTCHA/Challenge agotado")
        self.cloudflare_blocked = True
        return False

    def simulate_human_behavior(self):
        """Simulate sophisticated human-like behavior."""
        try:
            # Scroll suave
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            viewed_height = 0
            
            while viewed_height < total_height:
                scroll_amount = random.randint(100, 300)
                self.driver.execute_script(f"window.scrollBy(0, {scroll_amount})")
                viewed_height += scroll_amount
                time.sleep(random.uniform(0.5, 1.0))
            
            # Volver al inicio
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(random.uniform(0.5, 1.0))
            
        except Exception as e:
            logger.warning(f"Error en simulate_human_behavior: {str(e)}")

    def format_address(self, address: str) -> str:
        """Format address to standard format."""
        if not address:
            return ""
        
        # Eliminar email si está presente
        address = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+\s*', '', address)
        
        # Limpiar espacios extra y saltos de línea
        parts = [part.strip() for part in address.split('\n') if part.strip()]
        
        # Filtrar partes vacías y combinar
        parts = [p for p in parts if p]
        if not parts:
            return ""
            
        # Si hay código postal separado, integrarlo con la ciudad
        for i in range(len(parts)):
            if re.match(r'^\d{5}(-\d{4})?$', parts[i]):
                if i > 0 and not re.search(r'\d{5}', parts[i-1]):
                    parts[i-1] = f"{parts[i-1]}, {parts[i]}"
                    parts.pop(i)
                    break
        
        # Combinar las partes con comas
        formatted = ", ".join(parts)
        
        # Eliminar comas duplicadas y espacios extra
        formatted = re.sub(r',\s*,', ',', formatted)
        formatted = re.sub(r'\s+', ' ', formatted)
        formatted = formatted.strip(' ,')
        
        # Marcar como dirección parcial si no contiene números
        if not any(char.isdigit() for char in formatted):
            formatted = f"[PARTIAL] {formatted}"
        
        return formatted

    def scrape_from_chamber(self, business_name: str) -> Tuple[bool, str]:
        """Scrape address from Chamber of Commerce."""
        try:
            # Si ya detectamos bloqueo persistente, no intentar
            if self.cloudflare_blocked:
                logger.info("Saltando Chamber of Commerce debido a bloqueo previo")
                return False, ""
            
            formatted_name = business_name.replace(" ", "+")
            url = f"https://www.chamberofcommerce.com/search?what={formatted_name}&where="
            
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # Verificar errores HTTP inmediatamente
            if self.check_http_errors():
                logger.warning("Detectado bloqueo por errores HTTP")
                self.cloudflare_blocked = True
                return False, ""
            
            # Verificar Cloudflare Challenge
            if self.is_cloudflare_challenge():
                if not self.wait_for_manual_captcha():
                    logger.info("CAPTCHA falló, cambiando a Trustpilot")
                    self.cloudflare_blocked = True
                    return False, ""
            
            # Si llegamos aquí y estamos bloqueados, retornar
            if self.cloudflare_blocked:
                return False, ""

            # Verificar rápidamente si hay resultados
            try:
                # Esperar un corto tiempo para que cargue el indicador de "no resultados"
                no_results_indicators = [
                    "No results found",
                    "We couldn't find any results",
                    "Try adjusting your search",
                    "0 results",
                    "nothing matches",
                    "no matches found"
                ]
                
                # Esperar máximo 5 segundos para que la página cargue lo básico
                WebDriverWait(self.driver, 5).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                
                page_text = self.driver.page_source.lower()
                for indicator in no_results_indicators:
                    if indicator.lower() in page_text:
                        logger.info(f"No se encontraron resultados en Chamber of Commerce para: {business_name}")
                        return False, ""
                
                # Verificar rápidamente si hay elementos de resultados
                result_indicators = [
                    "a.card",
                    ".search-results",
                    ".business-listing",
                    ".search-result",
                    ".listing-item",
                    ".result-item"
                ]
                
                results_found = False
                for selector in result_indicators:
                    try:
                        results = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if results:
                            results_found = True
                            break
                    except:
                        continue
                
                if not results_found:
                    logger.info(f"No se encontraron elementos de resultados para: {business_name}")
                    return False, ""
                
            except Exception as e:
                logger.warning(f"Error al verificar resultados: {str(e)}")
                return False, ""
            
            # Solo si encontramos resultados, procedemos con el comportamiento normal
            self.simulate_human_behavior()
            
            # Primero intentar encontrar direcciones directamente
            selectors = [
                "address.card-text",
                "div.address",
                ".business-address",
                "[itemprop='address']",
                ".listing-address"
            ]
            
            for selector in selectors:
                try:
                    elements = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    for element in elements:
                        address = element.text.strip()
                        if address and len(address) > 5:
                            formatted_address = self.format_address(address)
                            logger.info(f"Dirección encontrada en Chamber of Commerce para: {business_name}")
                            return True, formatted_address
                except:
                    continue
            
            logger.info(f"No se encontró dirección en Chamber of Commerce para: {business_name}")
            return False, ""
            
        except Exception as e:
            logger.error(f"Error en Chamber of Commerce para {business_name}: {str(e)}")
            return False, ""

    def scrape_from_trustpilot(self, business_name: str) -> Tuple[bool, str]:
        """Scrape address from Trustpilot."""
        try:
            formatted_name = business_name.replace(" ", "%20")
            url = f"https://www.trustpilot.com/search?query={formatted_name}"
            
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # Verificar Cloudflare Challenge
            if self.is_cloudflare_challenge():
                if not self.wait_for_manual_captcha():
                    return False, ""
            
            self.simulate_human_behavior()
            
            # Buscar y hacer clic en el primer resultado
            result_selectors = [
                "a[name='business-unit-card']",
                ".business-unit-card-link",
                ".search-result-heading a",
                ".business-card a"
            ]
            
            result_found = False
            total_results = 0
            results_with_location = 0
            
            for selector in result_selectors:
                try:
                    results = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    if results:
                        total_results = len(results)
                        logger.info(f"Encontrados {total_results} resultados totales para: {business_name}")
                        
                        for result in results:
                            try:
                                # Obtener el texto del resultado para mejor coincidencia
                                result_text = result.text.lower()
                                business_name_parts = set(business_name.lower().split())
                                
                                # Verificar si el resultado coincide con el nombre del negocio
                                if any(part in result_text for part in business_name_parts):
                                    # Obtener el href antes de hacer clic
                                    result_url = result.get_attribute('href')
                                    if result_url:
                                        logger.info(f"Encontrado resultado relevante para {business_name}")
                                        # Navegar directamente a la URL
                                        self.driver.get(result_url)
                                        result_found = True
                                        time.sleep(random.uniform(3, 5))
                                        break
                            except Exception as e:
                                logger.warning(f"Error al procesar resultado individual: {str(e)}")
                                continue
                        
                        if result_found:
                            break
                except Exception as e:
                    logger.warning(f"Error al intentar acceder al resultado con selector {selector}: {str(e)}")
                    continue
            
            if not result_found:
                logger.info(f"No se encontraron resultados relevantes en Trustpilot para: {business_name}")
                return False, ""
            
            # Verificar nuevamente Cloudflare después de la navegación
            if self.is_cloudflare_challenge():
                if not self.wait_for_manual_captcha():
                    return False, ""
            
            # Esperar a que la página se cargue completamente
            try:
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
            except:
                pass
            
            self.simulate_human_behavior()
            
            # Intentar obtener la dirección del JSON primero
            try:
                script_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "script#__NEXT_DATA__"))
                )
                if script_element:
                    json_data = json.loads(script_element.get_attribute('innerHTML'))
                    try:
                        # Buscar datos de dirección en el JSON
                        props = json_data.get('props', {})
                        page_props = props.get('pageProps', {})
                        business = page_props.get('business', {})
                        location = business.get('location', {})
                        
                        if location:
                            address_parts = []
                            
                            # Recolectar todas las partes de la dirección disponibles
                            if location.get('street'):
                                address_parts.append(location['street'])
                            if location.get('city'):
                                address_parts.append(location['city'])
                            if location.get('state'):
                                address_parts.append(location['state'])
                            if location.get('zipCode'):
                                address_parts.append(location['zipCode'])
                            if location.get('country'):
                                address_parts.append(location['country'])
                            
                            if address_parts:
                                address = ", ".join(address_parts)
                                formatted_address = self.format_address(address)
                                logger.info(f"Dirección encontrada en Trustpilot (JSON) para {business_name}: {formatted_address}")
                                return True, formatted_address
                    except Exception as e:
                        logger.warning(f"Error extrayendo dirección del JSON: {str(e)}")
            except Exception as e:
                logger.warning(f"Error al buscar elemento JSON: {str(e)}")
            
            # Si no se encontró en el JSON, buscar en el HTML
            address_selectors = [
                "address>ul>li>ul",
                "address ul",
                "address li",
                "address",  # Añadir selector de address completo
                "[data-business-unit-location]",  # Selector específico de Trustpilot
                "div[data-initial-business-info]",  # Contenedor de información del negocio
                ".business-info-address",
                ".address-info",
                "[data-address]",
                ".company-information address",
                ".contact-info address",
                ".location-info",
                ".business-location"
            ]
            
            # Intentar extraer dirección del script de datos
            try:
                scripts = self.driver.find_elements(By.TAG_NAME, "script")
                for script in scripts:
                    try:
                        content = script.get_attribute('innerHTML')
                        if '"location":' in content:
                            logger.info("Encontrado script con datos de ubicación")
                            # Extraer la parte relevante del JSON
                            start_idx = content.find('"location":')
                            end_idx = content.find('}', start_idx)
                            location_data = content[start_idx:end_idx+1]
                            logger.info(f"Datos de ubicación encontrados: {location_data}")
                    except:
                        continue
            except Exception as e:
                logger.warning(f"Error al buscar en scripts: {str(e)}")
            
            for selector in address_selectors:
                try:
                    logger.info(f"Intentando selector: {selector}")
                    elements = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    
                    for element in elements:
                        try:
                            # Intentar obtener el texto directamente
                            address = element.text.strip()
                            logger.info(f"Texto encontrado con selector {selector}: {address}")
                            
                            # Intentar obtener atributos que puedan contener la dirección
                            for attr in ['data-address', 'data-location', 'title']:
                                try:
                                    attr_value = element.get_attribute(attr)
                                    if attr_value:
                                        logger.info(f"Atributo {attr} encontrado: {attr_value}")
                                        formatted_address = self.format_address(attr_value)
                                        if formatted_address:
                                            logger.info(f"Dirección encontrada en atributo {attr}: {formatted_address}")
                                            return True, formatted_address
                                except:
                                    continue
                            
                            # Verificar el texto encontrado - permitir direcciones parciales
                            if address and len(address) > 5:
                                formatted_address = self.format_address(address)
                                if formatted_address:
                                    logger.info(f"Dirección encontrada en Trustpilot (HTML) para {business_name}: {formatted_address}")
                                    return True, formatted_address
                                    
                            # Buscar elementos anidados que puedan contener la dirección
                            nested_elements = element.find_elements(By.CSS_SELECTOR, "*")
                            for nested in nested_elements:
                                try:
                                    nested_text = nested.text.strip()
                                    if nested_text and len(nested_text) > 5:
                                        formatted_address = self.format_address(nested_text)
                                        if formatted_address:
                                            logger.info(f"Dirección encontrada en elemento anidado: {formatted_address}")
                                            return True, formatted_address
                                except:
                                    continue
                                    
                        except Exception as e:
                            logger.warning(f"Error al procesar elemento con selector {selector}: {str(e)}")
                            continue
                except Exception as e:
                    logger.warning(f"Error al buscar selector {selector}: {str(e)}")
                    continue
            
            logger.info(f"No se encontró dirección en Trustpilot para: {business_name}")
            return False, ""
            
        except Exception as e:
            logger.error(f"Error en Trustpilot para {business_name}: {str(e)}")
            return False, ""

    def scrape_business(self, business_name: str) -> str:
        """Scrape address using multiple sources with retries."""
        # Asegurar que tenemos un navegador fresco para cada negocio
        self.restart_browser()
        
        try:
            for attempt in range(self.MAX_RETRIES):
                logger.info(f"Intento {attempt + 1} para {business_name}")
                
                try:
                    # Intentar primero con Chamber of Commerce
                    if not self.cloudflare_blocked:
                        success, address = self.scrape_from_chamber(business_name)
                        if success and address and address.strip():
                            logger.info(f"Dirección encontrada en Chamber of Commerce: {address}")
                            return address
                        
                        # Si fuimos bloqueados durante el intento, reiniciar navegador antes de Trustpilot
                        if self.cloudflare_blocked:
                            logger.info("Reiniciando navegador antes de cambiar a Trustpilot")
                            self.restart_browser()
                    
                    # Si Chamber of Commerce no tuvo éxito o estaba bloqueado, intentar con Trustpilot
                    success, address = self.scrape_from_trustpilot(business_name)
                    if success and address:  # Permitir direcciones parciales
                        logger.info(f"Dirección encontrada en Trustpilot: {address}")
                        return address  # Retornar la dirección incluso si es parcial
                    
                    # Si no tuvimos éxito y hay más intentos, reiniciar navegador
                    if attempt < self.MAX_RETRIES - 1:
                        delay = random.uniform(5, 15)
                        logger.info(f"Esperando {delay:.2f} segundos antes del siguiente intento...")
                        time.sleep(delay)
                        self.restart_browser()
                
                except Exception as e:
                    logger.error(f"Error en intento {attempt + 1} para {business_name}: {str(e)}")
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(random.uniform(5, 15))
                        self.restart_browser()
            
            logger.info(f"No se encontró dirección válida para: {business_name}")
            return ""
            
        finally:
            # Asegurar que el navegador se cierra después de procesar cada negocio
            self.clean_up()

    def scrape_businesses(self, input_file: str, output_file: str):
        """Scrape addresses for all businesses using parallel processing."""
        try:
            # Leer el CSV y verificar su estructura
            try:
                df = pd.read_csv(input_file)
                if df.empty:
                    raise ValueError("Input file is empty")
                
                # Verificar que la columna 'Business Name' existe
                if 'Business Name' not in df.columns:
                    # Intentar usar la primera columna
                    df.columns = ['Business Name'] + list(df.columns[1:])
                
                # Limpiar nombres de negocios
                df['Business Name'] = df['Business Name'].apply(lambda x: str(x).strip() if pd.notna(x) else "")
                df = df[df['Business Name'] != ""]  # Eliminar filas sin nombre de negocio
                
                business_names = df['Business Name'].tolist()
                total_businesses = len(business_names)
                
                if total_businesses == 0:
                    raise ValueError("No valid business names found in input file")
                
                logger.info(f"Successfully loaded {total_businesses} businesses from {input_file}")
                
            except Exception as e:
                logger.error(f"Error reading input file: {str(e)}")
                raise
            
            start_time = time.time()
            
            # Crear DataFrame inicial con todas las empresas
            results_df = pd.DataFrame({
                "Business Name": business_names,
                "Address": [""] * total_businesses
            })
            
            # Asegurar que el directorio de salida existe
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # Guardar el archivo CSV inicial
            results_df.to_csv(output_file, index=False)
            logger.info(f"Created initial CSV file: {output_file}")
            
            # Cola para almacenar resultados
            results_queue = queue.Queue()
            processed_count = 0
            addresses_found = 0
            
            # Función para procesar un lote de empresas
            def process_batch(batch_businesses, batch_start_idx):
                batch_results = []
                scraper = MultiSourceScraper()  # Crear una nueva instancia para cada lote
                for idx, business_name in enumerate(batch_businesses):
                    try:
                        address = scraper.scrape_business(business_name)
                        if address:
                            logger.info(f"Dirección encontrada para {business_name}: {address}")
                        batch_results.append((batch_start_idx + idx, business_name, address))
                    except Exception as e:
                        logger.error(f"Error processing {business_name}: {str(e)}")
                        batch_results.append((batch_start_idx + idx, business_name, ""))
                scraper.clean_up()  # Limpiar recursos al terminar el lote
                return batch_results
            
            # Función para escribir resultados en el CSV
            def csv_writer():
                nonlocal addresses_found
                while True:
                    try:
                        # Obtener resultado de la cola
                        result = results_queue.get()
                        if result is None:  # Señal de terminación
                            break
                            
                        # Actualizar DataFrame y guardar
                        for idx, business_name, address in result:
                            if address:  # Eliminar la validación adicional que podría estar filtrando direcciones parciales
                                results_df.at[idx, "Address"] = address
                                addresses_found += 1
                                logger.info(f"Guardando dirección para {business_name}: {address}")
                        
                        # Guardar con encoding UTF-8
                        results_df.to_csv(output_file, index=False, encoding='utf-8')
                        
                        # Actualizar progreso
                        nonlocal processed_count
                        processed_count += len(result)
                        progress = (processed_count / total_businesses) * 100
                        elapsed_time = time.time() - start_time
                        avg_time_per_record = elapsed_time / processed_count if processed_count > 0 else 0
                        estimated_remaining = avg_time_per_record * (total_businesses - processed_count)
                        
                        logger.info(f"Progress: {progress:.1f}% ({processed_count}/{total_businesses})")
                        logger.info(f"Addresses found so far: {addresses_found}")
                        logger.info(f"Average time per record: {avg_time_per_record:.2f}s")
                        logger.info(f"Estimated time remaining: {estimated_remaining/60:.1f} minutes")
                        
                        # Verificar archivo
                        if os.path.exists(output_file):
                            file_size = os.path.getsize(output_file)
                            logger.info(f"CSV file size: {file_size} bytes")
                        
                        results_queue.task_done()
                    except Exception as e:
                        logger.error(f"Error in CSV writer: {str(e)}")
            
            # Iniciar thread escritor
            writer_thread = threading.Thread(target=csv_writer)
            writer_thread.start()
            
            # Procesar en lotes usando ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = []
                for i in range(0, total_businesses, self.BATCH_SIZE):
                    batch = business_names[i:i + self.BATCH_SIZE]
                    futures.append(executor.submit(process_batch, batch, i))
                
                # Procesar resultados conforme se completan
                for future in as_completed(futures):
                    try:
                        batch_results = future.result()
                        results_queue.put(batch_results)
                    except Exception as e:
                        logger.error(f"Error processing batch: {str(e)}")
            
            # Señalizar terminación al escritor
            results_queue.put(None)
            writer_thread.join()
            
            total_time = time.time() - start_time
            logger.info(f"Processing completed in {total_time/60:.1f} minutes")
            logger.info(f"Results saved to: {output_file}")
            
            # Verificación final del archivo
            if os.path.exists(output_file):
                final_df = pd.read_csv(output_file)
                logger.info(f"Final CSV contains {len(final_df)} rows")
                addresses_found = len(final_df[final_df['Address'].notna() & (final_df['Address'] != '')])
                logger.info(f"Addresses found: {addresses_found}")
            else:
                logger.error("Final CSV file not found!")
            
        except Exception as e:
            logger.error(f"Error in scrape_businesses: {str(e)}")
        finally:
            self.performance_monitor.stop_monitoring()
            self.clean_up()

    def process_batch(self, batch: List[str]) -> List[Dict[str, str]]:
        """Process a batch of business names."""
        batch_results = []
        driver = None
        try:
            driver = self.setup_driver()
            for name in batch:
                try:
                    address = self.scrape_business_with_driver(name, driver)
                    # Guardar la dirección incluso si es parcial
                    batch_results.append({
                        "Business Name": name,
                        "Address": address if address and address.strip() else ""  # Guardar dirección parcial si existe
                    })
                except Exception as e:
                    logger.error(f"Error processing {name}: {str(e)}")
                    batch_results.append({
                        "Business Name": name,
                        "Address": ""
                    })
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
        return batch_results

    def scrape_business_with_driver(self, business_name: str, driver: webdriver.Chrome) -> str:
        """Scrape address for a single business using provided driver."""
        if not business_name or pd.isna(business_name):
            return ""
        
        logger.info(f"Processing: {business_name}")
        
        try:
            # Try Chamber of Commerce first
            if not self.cloudflare_blocked:
                success, address = self.scrape_from_chamber(business_name)
                if success and address and address.strip():  # Permitir direcciones parciales
                    return address
            
            # If Chamber fails, try Trustpilot
            success, address = self.scrape_from_trustpilot(business_name)
            if success and address and address.strip():  # Permitir direcciones parciales
                return address
            
            return ""
            
        except Exception as e:
            logger.error(f"Error processing {business_name}: {str(e)}")
            return ""

def main():
    """Main function to run the scraper."""
    if len(sys.argv) != 3:
        print("Usage: python simple_scraper.py input.csv output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        # Registrar manejadores de señales en el thread principal
        def signal_handler(signum, frame):
            logger.info("Shutdown signal received in main thread, cleaning up...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        scraper = MultiSourceScraper()
        scraper.scrape_businesses(input_file, output_file)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
