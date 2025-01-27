from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scraper.spiders.business_spider import BusinessSpider
import logging
import os
from dotenv import load_dotenv

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler('scraper.log'),
            logging.StreamHandler()
        ]
    )

def check_environment():
    """Check if required environment variables are set"""
    required_vars = ['QWANT_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing_vars)}\n"
            "Please create a .env file with these variables."
        )

def main():
    """Main function to run the scraper"""
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Check environment variables
        check_environment()
        
        # Get Scrapy settings
        settings = get_project_settings()
        
        # Initialize the crawler process
        process = CrawlerProcess(settings)
        
        # Start the spider
        logger.info("Starting business spider...")
        process.crawl(BusinessSpider)
        process.start()
        
        logger.info("Scraping completed successfully!")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main() 