#!/usr/bin/env python3
"""
Log rotation script for the web scraper.
Manages log files by rotating them based on size or age.
"""

import os
import sys
import time
import shutil
import logging
import gzip
from datetime import datetime, timedelta
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

class LogRotator:
    def __init__(self):
        self.log_dir = Path(os.getenv('LOG_DIR', 'logs'))
        self.max_size = int(os.getenv('LOG_MAX_SIZE', 10 * 1024 * 1024))  # 10MB
        self.max_age = int(os.getenv('LOG_MAX_AGE', 30))  # 30 days
        self.max_files = int(os.getenv('LOG_MAX_FILES', 10))
        self.compress = bool(os.getenv('LOG_COMPRESS', 'true').lower() == 'true')

    def setup_log_dir(self):
        """Ensure log directory exists"""
        try:
            self.log_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Log directory ensured: {self.log_dir}")
        except Exception as e:
            logger.error(f"Error creating log directory: {e}")
            sys.exit(1)

    def get_log_files(self) -> List[Path]:
        """Get list of log files to process"""
        return list(self.log_dir.glob('*.log'))

    def should_rotate(self, log_file: Path) -> bool:
        """Check if log file should be rotated based on size or age"""
        if not log_file.exists():
            return False

        # Check file size
        if log_file.stat().st_size > self.max_size:
            logger.debug(f"File {log_file} exceeds size limit")
            return True

        # Check file age
        file_age = datetime.now() - datetime.fromtimestamp(log_file.stat().st_mtime)
        if file_age > timedelta(days=self.max_age):
            logger.debug(f"File {log_file} exceeds age limit")
            return True

        return False

    def rotate_file(self, log_file: Path):
        """Rotate a single log file"""
        try:
            # Generate rotation timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Create rotated file name
            rotated_name = log_file.with_name(f"{log_file.stem}_{timestamp}.log")
            if self.compress:
                rotated_name = rotated_name.with_suffix('.log.gz')

            # Rotate the file
            if self.compress:
                with open(log_file, 'rb') as f_in:
                    with gzip.open(rotated_name, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                shutil.copy2(log_file, rotated_name)

            # Truncate original file
            open(log_file, 'w').close()

            logger.info(f"Rotated {log_file} to {rotated_name}")
            return True
        except Exception as e:
            logger.error(f"Error rotating {log_file}: {e}")
            return False

    def cleanup_old_logs(self):
        """Remove old rotated log files exceeding max_files"""
        try:
            # Get all rotated log files
            pattern = '*.log.gz' if self.compress else '*_*.log'
            rotated_logs = sorted(
                self.log_dir.glob(pattern),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )

            # Remove excess files
            for log_file in rotated_logs[self.max_files:]:
                try:
                    log_file.unlink()
                    logger.info(f"Removed old log file: {log_file}")
                except Exception as e:
                    logger.error(f"Error removing {log_file}: {e}")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def run(self):
        """Main log rotation routine"""
        logger.info("Starting log rotation process")
        
        try:
            # Ensure log directory exists
            self.setup_log_dir()

            # Get list of log files
            log_files = self.get_log_files()
            if not log_files:
                logger.info("No log files found to rotate")
                return

            # Process each log file
            for log_file in log_files:
                if self.should_rotate(log_file):
                    self.rotate_file(log_file)

            # Cleanup old rotated logs
            self.cleanup_old_logs()

            logger.info("Log rotation completed successfully")

        except Exception as e:
            logger.error(f"Log rotation failed: {e}")
            sys.exit(1)

def main():
    """Main entry point"""
    rotator = LogRotator()
    
    # Run once if called directly
    if len(sys.argv) == 1:
        rotator.run()
    
    # Run in daemon mode if --daemon flag is provided
    elif sys.argv[1] == '--daemon':
        interval = int(os.getenv('LOG_ROTATION_INTERVAL', 3600))  # Default: 1 hour
        logger.info(f"Starting log rotation daemon (interval: {interval}s)")
        
        while True:
            try:
                rotator.run()
                time.sleep(interval)
            except KeyboardInterrupt:
                logger.info("Log rotation daemon stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in daemon mode: {e}")
                sys.exit(1)
    else:
        print("Usage: rotate_logs.py [--daemon]")
        sys.exit(1)

if __name__ == '__main__':
    main() 