"""Multi-source address scraper with enhanced anti-detection features."""

import os
import sys
import random
import time
from typing import List, Dict, Tuple, Optional
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

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiSourceScraper:
    """Scraper for business addresses using multiple sources with enhanced anti-detection."""

    MAX_RETRIES = 3
    CAPTCHA_WAIT_TIME = 300
    CAPTCHA_CHECK_INTERVAL = 2
    MAX_CAPTCHA_ATTEMPTS = 2
    
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
        self.current_url = None
        self.last_page_source = None
        self.captcha_attempts = 0
        self.cloudflare_blocked = False
        self.driver = None
        self.ua = None
        self.initialize_user_agent()

    def initialize_user_agent(self):
        """Initialize UserAgent with error handling."""
        try:
            self.ua = UserAgent()
        except Exception:
            self.ua = None
            logger.warning("Could not initialize UserAgent, using default")

    def setup_driver(self):
        """Set up a new Chrome WebDriver instance with enhanced anti-detection features."""
        options = webdriver.ChromeOptions()
        
        # Enhanced anti-detection options
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-notifications')
        options.add_argument('--start-maximized')
        options.add_argument('--enable-unsafe-swiftshader')
        
        # Habilitar logging de red
        options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        # Set random user agent
        if self.ua:
            new_user_agent = self.ua.random
            options.add_argument(f'user-agent={new_user_agent}')
            logger.info(f"Usando nuevo User-Agent: {new_user_agent}")
        
        service = ChromeService()
        self.driver = webdriver.Chrome(service=service, options=options)
        self._apply_anti_detection_measures()

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
            logs = self.driver.get_log('performance')
            for entry in logs:
                try:
                    if 'message' in entry:
                        message = entry['message']
                        if '"Network.responseReceived"' in message:
                            import json
                            message_dict = json.loads(message)
                            response = message_dict.get('message', {}).get('params', {}).get('response', {})
                            status = response.get('status')
                            if status in [401, 403]:
                                logger.warning(f"Detectado error HTTP {status}")
                                self.cloudflare_blocked = True
                                return True
                except:
                    continue
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
                            "div.search-results",
                            ".business-listing",
                            ".search-result",
                            "address.card-text",
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
                            logger.info(f"Dirección encontrada en Chamber of Commerce para: {business_name}")
                            return True, address
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
                "a[name='business-unit-card']"
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
                            # Verificar si el resultado contiene el elemento de ubicación
                            location_elements = result.find_elements(By.CSS_SELECTOR, '[data-business-location-typography="true"]')
                            if location_elements:
                                results_with_location += 1
                                # Obtener el href antes de hacer clic
                                result_url = result.get_attribute('href')
                                if result_url:
                                    logger.info(f"Encontrado resultado con ubicación ({results_with_location}/{total_results})")
                                    # Navegar directamente a la URL en lugar de hacer clic
                                    self.driver.get(result_url)
                                    result_found = True
                                    time.sleep(random.uniform(3, 5))
                                    break
                        
                        if not result_found:
                            logger.info(f"No se encontraron resultados con ubicación de {total_results} resultados totales para: {business_name}")
                        
                        if result_found:
                            break
                except Exception as e:
                    logger.warning(f"Error al intentar acceder al resultado con selector {selector}: {str(e)}")
                    continue
            
            if not result_found:
                if total_results > 0:
                    logger.info(f"Se encontraron {total_results} resultados pero ninguno con ubicación para: {business_name}")
                else:
                    logger.info(f"No se encontraron resultados en Trustpilot para: {business_name}")
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
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#__NEXT_DATA__"))
                )
                if script_element:
                    import json
                    json_data = json.loads(script_element.get_attribute('innerHTML'))
                    try:
                        # Buscar datos de dirección en el JSON
                        props = json_data.get('props', {})
                        page_props = props.get('pageProps', {})
                        business = page_props.get('business', {})
                        location = business.get('location', {})
                        
                        address_parts = []
                        
                        # Recolectar todas las partes de la dirección disponibles
                        if location.get('street'):
                            address_parts.append(location['street'])
                        if location.get('city'):
                            address_parts.append(location['city'])
                        if location.get('state'):
                            address_parts.append(location['state'])
                        if location.get('country'):
                            address_parts.append(location['country'])
                        if location.get('zipCode'):
                            address_parts.append(location['zipCode'])
                        
                        if address_parts:
                            logger.info(f"Dirección encontrada en Trustpilot (JSON) para: {business_name}")
                            return True, ", ".join(address_parts)
                    except Exception as e:
                        logger.warning(f"Error extrayendo dirección del JSON: {str(e)}")
            except Exception as e:
                logger.warning(f"Error al buscar elemento JSON: {str(e)}")
            
            # Si no se encontró en el JSON, buscar en el HTML
            address_selectors = [
                "address>ul>li>ul",
                ".styles_contactInfoAddressList__GhGPc",
                ".business-info-address",
                ".address-info",
                "[data-address]",
                ".company-information address"
            ]
            
            for selector in address_selectors:
                try:
                    elements = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                    )
                    for element in elements:
                        if selector in ["address>ul>li>ul", ".styles_contactInfoAddressList__GhGPc"]:
                            # Para selectores que contienen múltiples elementos li
                            try:
                                address_parts = []
                                li_elements = element.find_elements(By.TAG_NAME, "li")
                                for li in li_elements:
                                    part = li.text.strip()
                                    if part:
                                        address_parts.append(part)
                                if address_parts:
                                    logger.info(f"Dirección encontrada en Trustpilot (HTML) para: {business_name}")
                                    return True, ", ".join(address_parts)
                            except:
                                continue
                        else:
                            # Para otros selectores que contienen la dirección directamente
                            address = element.text.strip()
                            if address and len(address) > 5:
                                logger.info(f"Dirección encontrada en Trustpilot (HTML) para: {business_name}")
                                return True, address
                except:
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
                    # Si no estamos bloqueados por Cloudflare, intentar Chamber of Commerce
                    if not self.cloudflare_blocked:
                        success, address = self.scrape_from_chamber(business_name)
                        if success and address:
                            return address
                        
                        # Si fuimos bloqueados durante el intento, reiniciar navegador antes de Trustpilot
                        if self.cloudflare_blocked:
                            logger.info("Reiniciando navegador antes de cambiar a Trustpilot")
                            self.restart_browser()
                            success, address = self.scrape_from_trustpilot(business_name)
                            if success and address:
                                return address
                    else:
                        # Si ya estábamos bloqueados, intentar con Trustpilot
                        success, address = self.scrape_from_trustpilot(business_name)
                        if success and address:
                            return address
                    
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
            
            return ""
            
        finally:
            # Asegurar que el navegador se cierra después de procesar cada negocio
            self.clean_up()

    def scrape_businesses(self, input_file: str, output_file: str):
        """Scrape addresses for all businesses in the input file."""
        try:
            df = pd.read_csv(input_file)
            if df.empty:
                raise ValueError("Input file is empty")
            
            business_names = df.iloc[:, 0].tolist()
            
            for i, name in enumerate(business_names, 1):
                if not name or pd.isna(name):
                    continue
                
                logger.info(f"Procesando negocio {i} de {len(business_names)}: {name}")
                
                try:
                    address = self.scrape_business(name)
                    self.results.append({
                        "Business Name": name,
                        "Address": address
                    })
                    
                    # Guardar resultados parciales
                    results_df = pd.DataFrame(self.results)
                    results_df.to_csv(output_file, index=False)
                    logger.info(f"Dirección guardada para {name}: {address}")
                    
                except Exception as e:
                    logger.error(f"Error procesando {name}: {str(e)}")
                    continue
                
                # Pausa entre negocios
                if i < len(business_names):
                    time.sleep(random.uniform(3, 7))
            
            logger.info(f"Proceso completado. Resultados guardados en: {output_file}")
            
        except Exception as e:
            logger.error(f"Error: {str(e)}")
        finally:
            self.clean_up()

def main():
    """Main function to run the scraper."""
    if len(sys.argv) != 3:
        print("Usage: python simple_scraper.py input.csv output.csv")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        scraper = MultiSourceScraper()
        scraper.scrape_businesses(input_file, output_file)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
