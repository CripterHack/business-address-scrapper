#!/usr/bin/env python3
"""
Pre-deployment check script for the web scraper.
Runs a series of tests and validations before deployment.
"""

import os
import sys
import logging
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dataclasses import dataclass

from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    name: str
    status: bool
    message: str
    details: Dict[str, Any] = None

class PreDeploymentChecker:
    def __init__(self):
        self.results: List[TestResult] = []
        self.project_root = Path(__file__).parent.parent

    def run_unit_tests(self) -> TestResult:
        """Run unit tests"""
        try:
            result = subprocess.run(
                ['pytest', 'tests/unit', '-v', '--junitxml=test-reports/unit-tests.xml'],
                capture_output=True,
                text=True,
                check=True
            )
            return TestResult(
                name="Unit Tests",
                status=True,
                message="All unit tests passed successfully",
                details={'output': result.stdout}
            )
        except subprocess.CalledProcessError as e:
            return TestResult(
                name="Unit Tests",
                status=False,
                message="Unit tests failed",
                details={'output': e.stdout, 'error': e.stderr}
            )

    def run_integration_tests(self) -> TestResult:
        """Run integration tests"""
        try:
            result = subprocess.run(
                ['pytest', 'tests/integration', '-v', '--junitxml=test-reports/integration-tests.xml'],
                capture_output=True,
                text=True,
                check=True
            )
            return TestResult(
                name="Integration Tests",
                status=True,
                message="All integration tests passed successfully",
                details={'output': result.stdout}
            )
        except subprocess.CalledProcessError as e:
            return TestResult(
                name="Integration Tests",
                status=False,
                message="Integration tests failed",
                details={'output': e.stdout, 'error': e.stderr}
            )

    def check_code_quality(self) -> TestResult:
        """Run code quality checks"""
        issues = []
        
        try:
            # Run flake8
            flake8_result = subprocess.run(
                ['flake8', 'scraper/'],
                capture_output=True,
                text=True
            )
            if flake8_result.returncode != 0:
                issues.append(f"Flake8 issues found:\n{flake8_result.stdout}")

            # Run mypy
            mypy_result = subprocess.run(
                ['mypy', 'scraper/'],
                capture_output=True,
                text=True
            )
            if mypy_result.returncode != 0:
                issues.append(f"Type check issues found:\n{mypy_result.stdout}")

            # Run pylint
            pylint_result = subprocess.run(
                ['pylint', 'scraper/'],
                capture_output=True,
                text=True
            )
            if pylint_result.returncode != 0:
                issues.append(f"Pylint issues found:\n{pylint_result.stdout}")

            status = len(issues) == 0
            message = "Code quality checks passed" if status else \
                     "Code quality issues found"

            return TestResult(
                name="Code Quality",
                status=status,
                message=message,
                details={'issues': issues}
            )

        except Exception as e:
            return TestResult(
                name="Code Quality",
                status=False,
                message=f"Code quality check failed: {str(e)}"
            )

    def check_dependencies(self) -> TestResult:
        """Check project dependencies"""
        try:
            # Check for outdated packages
            result = subprocess.run(
                ['pip', 'list', '--outdated', '--format=json'],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Run safety check
            safety_result = subprocess.run(
                ['safety', 'check'],
                capture_output=True,
                text=True
            )
            
            issues = []
            if result.stdout.strip():
                issues.append("Outdated packages found")
            if safety_result.returncode != 0:
                issues.append("Security vulnerabilities found in dependencies")
            
            status = len(issues) == 0
            message = "Dependency checks passed" if status else \
                     "Dependency issues found"
            
            return TestResult(
                name="Dependencies",
                status=status,
                message=message,
                details={
                    'outdated': result.stdout,
                    'security': safety_result.stdout
                }
            )
            
        except Exception as e:
            return TestResult(
                name="Dependencies",
                status=False,
                message=f"Dependency check failed: {str(e)}"
            )

    def check_documentation(self) -> TestResult:
        """Check documentation completeness"""
        required_docs = [
            'README.md',
            'CHANGELOG.md',
            'docs/API.md',
            'docs/DEPLOYMENT.md'
        ]
        
        missing_docs = []
        for doc in required_docs:
            if not (self.project_root / doc).exists():
                missing_docs.append(doc)
        
        status = len(missing_docs) == 0
        message = "All required documentation is present" if status else \
                 f"Missing documentation: {', '.join(missing_docs)}"
        
        return TestResult(
            name="Documentation",
            status=status,
            message=message,
            details={'missing_docs': missing_docs}
        )

    def check_git_status(self) -> TestResult:
        """Check git repository status"""
        try:
            # Check if we're on the main/master branch
            branch_result = subprocess.run(
                ['git', 'branch', '--show-current'],
                capture_output=True,
                text=True,
                check=True
            )
            current_branch = branch_result.stdout.strip()
            
            # Check for uncommitted changes
            status_result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True,
                text=True,
                check=True
            )
            has_changes = bool(status_result.stdout.strip())
            
            issues = []
            if current_branch not in ['main', 'master']:
                issues.append(f"Not on main/master branch (current: {current_branch})")
            if has_changes:
                issues.append("Uncommitted changes present")
            
            status = len(issues) == 0
            message = "Git repository is clean and ready" if status else \
                     "Git repository issues found"
            
            return TestResult(
                name="Git Status",
                status=status,
                message=message,
                details={
                    'branch': current_branch,
                    'has_changes': has_changes,
                    'issues': issues
                }
            )
            
        except Exception as e:
            return TestResult(
                name="Git Status",
                status=False,
                message=f"Git status check failed: {str(e)}"
            )

    def generate_report(self) -> str:
        """Generate a detailed pre-deployment report"""
        report = ["Pre-Deployment Check Report", "=" * 40, ""]
        
        for result in self.results:
            status_str = "✓" if result.status else "✗"
            report.append(f"{status_str} {result.name}")
            report.append("-" * 40)
            report.append(result.message)
            
            if result.details:
                report.append("Details:")
                for key, value in result.details.items():
                    if isinstance(value, list):
                        report.append(f"  {key}:")
                        for item in value:
                            report.append(f"    - {item}")
                    else:
                        report.append(f"  {key}: {value}")
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
        logger.info("Starting pre-deployment checks")
        
        try:
            # Run all checks
            checks = [
                self.run_unit_tests,
                self.run_integration_tests,
                self.check_code_quality,
                self.check_dependencies,
                self.check_documentation,
                self.check_git_status
            ]
            
            for check in checks:
                try:
                    result = check()
                    self.results.append(result)
                    
                    # Log the result
                    log_level = logging.INFO if result.status else logging.ERROR
                    logger.log(log_level, f"{result.name}: {result.message}")
                    
                except Exception as e:
                    logger.error(f"Error running {check.__name__}: {e}")
                    self.results.append(TestResult(
                        name=check.__name__,
                        status=False,
                        message=f"Check failed: {str(e)}"
                    ))
            
            # Generate and save report
            report = self.generate_report()
            report_file = Path('pre_deploy_check_report.txt')
            report_file.write_text(report)
            
            logger.info(f"Pre-deployment check completed. Report saved to {report_file}")
            
            # Exit with error if any checks failed
            if any(not result.status for result in self.results):
                logger.error("Some pre-deployment checks failed")
                sys.exit(1)
            
        except Exception as e:
            logger.error(f"Pre-deployment check failed: {e}")
            sys.exit(1)

if __name__ == '__main__':
    checker = PreDeploymentChecker()
    checker.run() 