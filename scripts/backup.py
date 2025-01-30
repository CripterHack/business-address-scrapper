#!/usr/bin/env python3
"""
Backup script for the web scraper.
Creates compressed backups of data files and configurations.
"""

import os
import sys
import shutil
import logging
import tarfile
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ScraperBackup:
    def __init__(self):
        self.backup_path = Path(os.getenv('BACKUP_PATH', 'backups'))
        self.backup_interval = int(os.getenv('BACKUP_INTERVAL', 86400))  # 24 hours
        self.max_backups = int(os.getenv('MAX_BACKUPS', 7))
        self.compression = os.getenv('BACKUP_COMPRESSION', 'gz')

    def create_backup_dirs(self):
        """Create necessary backup directories"""
        try:
            self.backup_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Backup directory ensured: {self.backup_path}")
        except Exception as e:
            logger.error(f"Error creating backup directory: {e}")
            sys.exit(1)

    def get_files_to_backup(self) -> List[Path]:
        """Get list of files to backup"""
        files_to_backup = []
        
        # Data files
        data_patterns = [
            'business_data.csv',
            'output/*.csv',
            '*.json',
            'logs/*.log'
        ]

        # Configuration files
        config_patterns = [
            '.env*',
            'config/*.yaml',
            'config/*.json'
        ]

        # Add all matching files
        for pattern in data_patterns + config_patterns:
            files_to_backup.extend(Path().glob(pattern))

        return files_to_backup

    def create_backup(self) -> Optional[Path]:
        """Create a new backup archive"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.backup_path / f"scraper_backup_{timestamp}.tar.{self.compression}"

        try:
            with tarfile.open(backup_file, f"w:{self.compression}") as tar:
                files = self.get_files_to_backup()
                
                if not files:
                    logger.warning("No files found to backup")
                    return None

                for file_path in files:
                    if file_path.exists():
                        tar.add(file_path, arcname=file_path.name)
                        logger.debug(f"Added {file_path} to backup")

            logger.info(f"Created backup: {backup_file}")
            return backup_file
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            return None

    def cleanup_old_backups(self):
        """Remove old backup files exceeding max_backups"""
        try:
            backup_files = sorted(
                self.backup_path.glob(f"scraper_backup_*.tar.{self.compression}"),
                key=lambda x: x.stat().st_mtime
            )

            while len(backup_files) > self.max_backups:
                oldest_backup = backup_files.pop(0)
                oldest_backup.unlink()
                logger.info(f"Removed old backup: {oldest_backup}")
        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}")

    def verify_backup(self, backup_file: Path) -> bool:
        """Verify the integrity of the backup file"""
        try:
            with tarfile.open(backup_file, f"r:{self.compression}") as tar:
                # Check if archive can be read
                tar.getmembers()
                
                # Verify file count
                member_count = len(tar.getmembers())
                expected_count = len(self.get_files_to_backup())
                
                if member_count != expected_count:
                    logger.warning(
                        f"Backup file count mismatch: {member_count} files in archive, "
                        f"expected {expected_count}"
                    )
                    return False

            logger.info(f"Backup verified successfully: {backup_file}")
            return True
        except Exception as e:
            logger.error(f"Backup verification failed: {e}")
            return False

    def run(self):
        """Main backup routine"""
        logger.info("Starting backup process")
        
        try:
            # Ensure backup directory exists
            self.create_backup_dirs()

            # Create new backup
            backup_file = self.create_backup()
            if not backup_file:
                logger.error("Backup creation failed")
                return

            # Verify backup
            if not self.verify_backup(backup_file):
                logger.error("Backup verification failed")
                return

            # Cleanup old backups
            self.cleanup_old_backups()

            logger.info("Backup process completed successfully")

        except Exception as e:
            logger.error(f"Backup process failed: {e}")
            sys.exit(1)

if __name__ == '__main__':
    backup = ScraperBackup()
    backup.run() 