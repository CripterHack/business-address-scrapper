#!/usr/bin/env python3
"""
Test data cleanup script for the web scraper.
Removes generated test data and resets test environment.
"""

import os
import sys
import logging
import shutil
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TestDataCleaner:
    def __init__(self):
        self.test_data_dir = Path(os.getenv('TEST_DATA_DIR', 'tests/fixtures'))
        self.test_output_dir = Path(os.getenv('TEST_OUTPUT_DIR', 'tests/output'))
        self.test_cache_dir = Path(os.getenv('TEST_CACHE_DIR', 'tests/.cache'))
        
        # Patterns for files to clean
        self.cleanup_patterns = [
            '*.csv',
            '*.json',
            '*.log',
            '*.gz',
            '*.sqlite',
            '*.db'
        ]

    def get_cleanup_paths(self) -> List[Path]:
        """Get all paths that need to be cleaned"""
        cleanup_paths = []
        
        # Add main test directories
        cleanup_paths.extend([
            self.test_data_dir,
            self.test_output_dir,
            self.test_cache_dir
        ])
        
        # Add any additional test-related directories
        cleanup_paths.extend([
            Path('tests/logs'),
            Path('tests/temp'),
            Path('tests/downloads')
        ])
        
        return cleanup_paths

    def clean_directory(self, directory: Path):
        """Clean a single directory"""
        if not directory.exists():
            logger.debug(f"Directory does not exist: {directory}")
            return

        try:
            # If it's a cache or temp directory, remove it entirely
            if any(name in str(directory) for name in ['.cache', 'temp', 'downloads']):
                shutil.rmtree(directory)
                logger.info(f"Removed directory: {directory}")
                return

            # Otherwise, just remove matching files
            for pattern in self.cleanup_patterns:
                for file_path in directory.glob(pattern):
                    if file_path.is_file():
                        file_path.unlink()
                        logger.debug(f"Removed file: {file_path}")

            # Remove empty subdirectories
            for dir_path in directory.glob('**/*'):
                if dir_path.is_dir() and not any(dir_path.iterdir()):
                    dir_path.rmdir()
                    logger.debug(f"Removed empty directory: {dir_path}")

        except Exception as e:
            logger.error(f"Error cleaning directory {directory}: {e}")

    def reset_test_environment(self):
        """Reset the test environment by recreating necessary directories"""
        try:
            for directory in [self.test_data_dir, self.test_output_dir]:
                directory.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Reset directory: {directory}")
            
            # Create placeholder files if needed
            placeholder = self.test_data_dir / '.gitkeep'
            placeholder.touch()
            
        except Exception as e:
            logger.error(f"Error resetting test environment: {e}")
            sys.exit(1)

    def run(self, reset: bool = True):
        """Main cleanup routine"""
        logger.info("Starting test data cleanup")
        
        try:
            # Clean all test directories
            for cleanup_path in self.get_cleanup_paths():
                if cleanup_path.exists():
                    logger.info(f"Cleaning directory: {cleanup_path}")
                    self.clean_directory(cleanup_path)
            
            # Optionally reset the test environment
            if reset:
                logger.info("Resetting test environment")
                self.reset_test_environment()
            
            logger.info("Test data cleanup completed successfully")
            
        except Exception as e:
            logger.error(f"Test data cleanup failed: {e}")
            sys.exit(1)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean test data and reset test environment')
    parser.add_argument('--no-reset', action='store_true',
                       help='Do not reset test environment after cleaning')
    args = parser.parse_args()
    
    cleaner = TestDataCleaner()
    cleaner.run(reset=not args.no_reset)

if __name__ == '__main__':
    main() 