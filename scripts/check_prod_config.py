#!/usr/bin/env python3
"""
Production configuration checker for the web scraper.
Validates all required settings and dependencies for production deployment.
"""

import os
import sys
import logging
import socket
import requests
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass

import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CheckResult:
    name: str
    status: bool
    message: str
    details: Dict[str, Any] = None

class ProductionConfigChecker:
    def __init__(self):
        # Load production environment
        load_dotenv('.env.prod')
        
        self.required_env_vars = [
            'SCRAPER_MODE',
            'LOG_LEVEL',
            'QWANT_API_KEY',
            'LLAMA_MODEL_PATH',
            'CSV_OUTPUT_FILE',
            'INPUT_FILE',
            'DB_HOST',
            'DB_PORT',
            'DB_NAME',
            'DB_USER',
            'DB_PASSWORD'
        ]
        
        self.required_directories = [
            '/var/log/scraper',
            '/var/data/scraper/output',
            '/var/data/scraper/input',
            '/var/cache/scraper',
            '/var/backup/scraper'
        ]
        
        self.results: List[CheckResult] = []

    def check_environment_variables(self) -> CheckResult:
        """Check if all required environment variables are set"""
        missing_vars = []
        for var in self.required_env_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        status = len(missing_vars) == 0
        message = "All required environment variables are set" if status else \
                 f"Missing environment variables: {', '.join(missing_vars)}"
        
        return CheckResult(
            name="Environment Variables",
            status=status,
            message=message,
            details={'missing_vars': missing_vars}
        )

    def check_directories(self) -> CheckResult:
        """Check if required directories exist and are writable"""
        invalid_dirs = []
        for directory in self.required_directories:
            path = Path(directory)
            if not path.exists():
                invalid_dirs.append(f"{directory} (missing)")
            elif not os.access(path, os.W_OK):
                invalid_dirs.append(f"{directory} (not writable)")
        
        status = len(invalid_dirs) == 0
        message = "All required directories are available and writable" if status else \
                 f"Directory issues found: {', '.join(invalid_dirs)}"
        
        return CheckResult(
            name="Directories",
            status=status,
            message=message,
            details={'invalid_dirs': invalid_dirs}
        )

    def check_database_connection(self) -> CheckResult:
        """Check database connectivity"""
        try:
            import psycopg2
            
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                dbname=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                connect_timeout=5
            )
            conn.close()
            
            return CheckResult(
                name="Database Connection",
                status=True,
                message="Successfully connected to database"
            )
            
        except Exception as e:
            return CheckResult(
                name="Database Connection",
                status=False,
                message=f"Database connection failed: {str(e)}"
            )

    def check_external_services(self) -> CheckResult:
        """Check connectivity to external services"""
        services = {
            'Qwant API': 'https://api.qwant.com/v3/status',
            'Metrics Endpoint': os.getenv('METRICS_ENDPOINT')
        }
        
        failed_services = []
        for service_name, url in services.items():
            if not url:
                continue
            try:
                response = requests.get(url, timeout=5)
                if response.status_code != 200:
                    failed_services.append(f"{service_name} (status: {response.status_code})")
            except requests.RequestException as e:
                failed_services.append(f"{service_name} ({str(e)})")
        
        status = len(failed_services) == 0
        message = "All external services are accessible" if status else \
                 f"Service connectivity issues: {', '.join(failed_services)}"
        
        return CheckResult(
            name="External Services",
            status=status,
            message=message,
            details={'failed_services': failed_services}
        )

    def check_system_resources(self) -> CheckResult:
        """Check system resources and limits"""
        issues = []
        
        # Check disk space
        disk = os.statvfs('/')
        free_gb = (disk.f_bavail * disk.f_frsize) / (1024**3)
        if free_gb < 10:  # Less than 10GB free
            issues.append(f"Low disk space: {free_gb:.1f}GB free")
        
        # Check memory
        try:
            import psutil
            memory = psutil.virtual_memory()
            if memory.available < 1024**3:  # Less than 1GB available
                issues.append(f"Low memory: {memory.available/1024**3:.1f}GB available")
        except ImportError:
            issues.append("Could not check memory (psutil not installed)")
        
        # Check file descriptors
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < 1024:
                issues.append(f"Low file descriptor limit: {soft}")
        except ImportError:
            issues.append("Could not check file descriptors")
        
        status = len(issues) == 0
        message = "System resources are sufficient" if status else \
                 f"Resource issues found: {', '.join(issues)}"
        
        return CheckResult(
            name="System Resources",
            status=status,
            message=message,
            details={'issues': issues}
        )

    def check_security_settings(self) -> CheckResult:
        """Check security-related settings"""
        issues = []
        
        # Check SSL/TLS settings
        if not os.getenv('SSL_VERIFY', 'true').lower() == 'true':
            issues.append("SSL verification is disabled")
        
        # Check encryption settings
        if not os.getenv('USE_ENCRYPTION', 'true').lower() == 'true':
            issues.append("Data encryption is disabled")
        
        # Check sensitive environment variables
        for var in ['DB_PASSWORD', 'ENCRYPTION_KEY']:
            if os.getenv(var) and len(os.getenv(var)) < 12:
                issues.append(f"Weak {var}")
        
        status = len(issues) == 0
        message = "Security settings are properly configured" if status else \
                 f"Security issues found: {', '.join(issues)}"
        
        return CheckResult(
            name="Security Settings",
            status=status,
            message=message,
            details={'issues': issues}
        )

    def run_all_checks(self):
        """Run all configuration checks"""
        checks = [
            self.check_environment_variables,
            self.check_directories,
            self.check_database_connection,
            self.check_external_services,
            self.check_system_resources,
            self.check_security_settings
        ]
        
        for check in checks:
            try:
                result = check()
                self.results.append(result)
                
                # Log the result
                log_level = logging.INFO if result.status else logging.ERROR
                logger.log(log_level, f"{result.name}: {result.message}")
                
                if result.details:
                    logger.debug(f"Details: {result.details}")
                
            except Exception as e:
                logger.error(f"Error running {check.__name__}: {e}")
                self.results.append(CheckResult(
                    name=check.__name__,
                    status=False,
                    message=f"Check failed: {str(e)}"
                ))

    def generate_report(self) -> str:
        """Generate a detailed report of all checks"""
        report = ["Production Configuration Check Report", "=" * 40, ""]
        
        for result in self.results:
            status_str = "✓" if result.status else "✗"
            report.append(f"{status_str} {result.name}")
            report.append("-" * 40)
            report.append(result.message)
            if result.details:
                report.append("Details:")
                for key, value in result.details.items():
                    report.append(f"  - {key}: {value}")
            report.append("")
        
        # Add summary
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.status)
        report.append("Summary")
        report.append("=" * 40)
        report.append(f"Total Checks: {total_checks}")
        report.append(f"Passed: {passed_checks}")
        report.append(f"Failed: {total_checks - passed_checks}")
        
        return "\n".join(report)

    def run(self):
        """Main execution routine"""
        logger.info("Starting production configuration check")
        
        try:
            self.run_all_checks()
            
            # Generate and save report
            report = self.generate_report()
            report_file = Path('prod_config_check_report.txt')
            report_file.write_text(report)
            
            logger.info(f"Configuration check completed. Report saved to {report_file}")
            
            # Exit with error if any checks failed
            if any(not result.status for result in self.results):
                logger.error("Some configuration checks failed")
                sys.exit(1)
            
        except Exception as e:
            logger.error(f"Configuration check failed: {e}")
            sys.exit(1)

if __name__ == '__main__':
    checker = ProductionConfigChecker()
    checker.run() 