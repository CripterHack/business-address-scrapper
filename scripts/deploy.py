#!/usr/bin/env python3
"""
Deployment script for the web scraper.
Handles the deployment process to production environment.
"""

import os
import sys
import logging
import shutil
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Deployer:
    def __init__(self):
        # Load production environment
        load_dotenv('.env.prod')
        
        self.deploy_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.project_root = Path(__file__).parent.parent
        self.deploy_root = Path(os.getenv('DEPLOY_ROOT', '/opt/scraper'))
        self.backup_dir = Path(os.getenv('BACKUP_PATH', '/var/backup/scraper'))
        
        # Deployment directories
        self.versions_dir = self.deploy_root / 'versions'
        self.current_dir = self.deploy_root / 'current'
        self.shared_dir = self.deploy_root / 'shared'
        
        # Files to exclude from deployment
        self.exclude_patterns = [
            '.git',
            '__pycache__',
            '*.pyc',
            'tests',
            'docs',
            'scripts',
            'venv',
            '.env*',
            '*.log'
        ]

    def setup_directories(self):
        """Create necessary deployment directories"""
        try:
            for directory in [self.deploy_root, self.versions_dir, self.shared_dir]:
                directory.mkdir(parents=True, exist_ok=True)
                logger.info(f"Ensured directory: {directory}")
        except Exception as e:
            logger.error(f"Error creating deployment directories: {e}")
            sys.exit(1)

    def create_backup(self):
        """Create backup of current deployment"""
        if not self.current_dir.exists():
            logger.info("No current deployment to backup")
            return
        
        try:
            backup_path = self.backup_dir / f"backup_{self.deploy_timestamp}.tar.gz"
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Create backup archive
            subprocess.run(
                ['tar', 'czf', str(backup_path), '-C', str(self.current_dir), '.'],
                check=True
            )
            logger.info(f"Created backup: {backup_path}")
            
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            sys.exit(1)

    def prepare_release(self) -> Path:
        """Prepare new release directory"""
        try:
            # Create new release directory
            release_path = self.versions_dir / f"release_{self.deploy_timestamp}"
            release_path.mkdir(parents=True)
            
            # Copy project files
            for item in self.project_root.iterdir():
                if item.name not in self.exclude_patterns and not any(
                    pattern in str(item) for pattern in self.exclude_patterns
                ):
                    if item.is_dir():
                        shutil.copytree(item, release_path / item.name)
                    else:
                        shutil.copy2(item, release_path / item.name)
            
            logger.info(f"Prepared release at: {release_path}")
            return release_path
            
        except Exception as e:
            logger.error(f"Error preparing release: {e}")
            sys.exit(1)

    def setup_virtualenv(self, release_path: Path):
        """Set up virtual environment for the release"""
        try:
            venv_path = release_path / 'venv'
            
            # Create virtual environment
            subprocess.run(['python', '-m', 'venv', str(venv_path)], check=True)
            
            # Install dependencies
            pip_path = venv_path / 'bin' / 'pip'
            subprocess.run([str(pip_path), 'install', '-r', str(release_path / 'requirements.txt')], check=True)
            
            logger.info("Virtual environment setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up virtual environment: {e}")
            sys.exit(1)

    def update_symlinks(self, release_path: Path):
        """Update symlinks to point to new release"""
        try:
            # Remove existing current symlink if it exists
            if self.current_dir.is_symlink():
                self.current_dir.unlink()
            
            # Create new symlink
            self.current_dir.symlink_to(release_path)
            logger.info(f"Updated current symlink to: {release_path}")
            
        except Exception as e:
            logger.error(f"Error updating symlinks: {e}")
            sys.exit(1)

    def cleanup_old_releases(self):
        """Clean up old releases keeping only the last 5"""
        try:
            releases = sorted(self.versions_dir.glob('release_*'))
            releases_to_keep = 5
            
            if len(releases) > releases_to_keep:
                for release in releases[:-releases_to_keep]:
                    shutil.rmtree(release)
                    logger.info(f"Removed old release: {release}")
                    
        except Exception as e:
            logger.error(f"Error cleaning up old releases: {e}")

    def verify_deployment(self) -> bool:
        """Verify the deployment was successful"""
        try:
            # Check current symlink
            if not self.current_dir.exists():
                logger.error("Current symlink does not exist")
                return False
            
            # Check virtual environment
            venv_python = self.current_dir / 'venv' / 'bin' / 'python'
            if not venv_python.exists():
                logger.error("Virtual environment python not found")
                return False
            
            # Try importing main modules
            result = subprocess.run(
                [str(venv_python), '-c', 'import scraper; print("OK")'],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                logger.error(f"Module import test failed: {result.stderr}")
                return False
            
            logger.info("Deployment verification passed")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying deployment: {e}")
            return False

    def run(self):
        """Main deployment routine"""
        logger.info("Starting deployment process")
        
        try:
            # Run pre-deployment checks
            logger.info("Running pre-deployment checks")
            result = subprocess.run(['python', 'scripts/pre_deploy_check.py'], check=True)
            
            # Setup deployment structure
            self.setup_directories()
            
            # Backup current deployment
            self.create_backup()
            
            # Prepare new release
            release_path = self.prepare_release()
            
            # Setup virtual environment
            self.setup_virtualenv(release_path)
            
            # Update symlinks
            self.update_symlinks(release_path)
            
            # Verify deployment
            if not self.verify_deployment():
                logger.error("Deployment verification failed")
                # TODO: Implement rollback
                sys.exit(1)
            
            # Cleanup old releases
            self.cleanup_old_releases()
            
            logger.info("Deployment completed successfully")
            
        except subprocess.CalledProcessError as e:
            logger.error("Pre-deployment checks failed")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            sys.exit(1)

def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Deploy the web scraper to production')
    parser.add_argument('--force', action='store_true',
                       help='Skip pre-deployment checks')
    args = parser.parse_args()
    
    deployer = Deployer()
    deployer.run()

if __name__ == '__main__':
    main() 