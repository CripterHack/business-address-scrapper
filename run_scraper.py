from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scraper.spiders.business_spider import BusinessSpider
import logging
import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

def ensure_directories(directories):
    """Create necessary directories if they don't exist"""
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)

def setup_logging():
    """Configure enhanced logging settings"""
    # Ensure logs directory exists
    Path('logs').mkdir(parents=True, exist_ok=True)
    
    # Generate unique log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/scraper_{timestamp}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # También log a consola
        ]
    )
    
    # Configurar logging específico para scrapy
    scrapy_logger = logging.getLogger('scrapy')
    scrapy_logger.setLevel(logging.DEBUG)
    
    return logging.getLogger(__name__)

def check_environment():
    """Check environment and system configuration"""
    logger = logging.getLogger(__name__)
    
    # Verificar variables de entorno críticas
    critical_vars = ['SCRAPER_MODE', 'LOG_LEVEL']
    missing_vars = [var for var in critical_vars if not os.getenv(var)]
    if missing_vars:
        logger.warning(f"Missing recommended environment variables: {', '.join(missing_vars)}")
    
    # Verificar archivo de entrada
    input_file = os.getenv('INPUT_FILE', 'data/input/businesses.csv')
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    logger.info(f"Input file found: {input_file}")
    
    # Verificar permisos de escritura en directorio de salida
    output_dir = os.path.dirname(os.getenv('CSV_OUTPUT_FILE', 'data/output/business_data.csv'))
    if not os.access(output_dir, os.W_OK):
        raise PermissionError(f"No write permission in output directory: {output_dir}")
    
    logger.info(f"Output directory is writable: {output_dir}")

def save_stats(stats, settings):
    """Save crawler statistics to file"""
    logger = logging.getLogger(__name__)
    
    try:
        stats_file = settings.get('STATS_DUMP_FILE', 'scraper_stats.json')
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Statistics saved to {stats_file}")
    except Exception as e:
        logger.error(f"Failed to save statistics: {e}")

def main():
    """Main execution function"""
    # Cargar variables de entorno
    load_dotenv()
    
    # Configurar logging
    logger = setup_logging()
    logger.info("Starting scraper initialization")
    
    try:
        # Obtener configuración
        settings = get_project_settings()
        
        # Asegurar que existan los directorios necesarios
        ensure_directories(settings.get('ENSURE_DIRECTORIES', []))
        logger.info("Directory structure verified")
        
        # Verificar ambiente
        check_environment()
        logger.info("Environment check passed")
        
        # Inicializar el proceso
        process = CrawlerProcess(settings)
        logger.info("Crawler process initialized")
        
        # Ejecutar el spider
        logger.info("Starting business spider...")
        process.crawl(BusinessSpider)
        process.start()
        
        # Guardar estadísticas
        if hasattr(process, 'stats'):
            save_stats(process.stats.get_stats(), settings)
        
        logger.info("Scraping completed successfully!")
        
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        raise
    except PermissionError as e:
        logger.error(f"Permission error: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        raise
    finally:
        logger.info("Scraping process finished")

if __name__ == "__main__":
    main() 