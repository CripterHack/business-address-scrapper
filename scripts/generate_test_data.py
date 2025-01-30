#!/usr/bin/env python3
"""
Test data generation script for the web scraper.
Generates realistic test data for development and testing purposes.
"""

import os
import sys
import logging
import json
import csv
from pathlib import Path
from typing import List, Dict
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from mimesis import Business, Address
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class TestDataGenerator:
    def __init__(self):
        self.faker = Faker()
        self.business_gen = Business()
        self.address_gen = Address('en')
        self.output_dir = Path(os.getenv('TEST_DATA_DIR', 'tests/fixtures'))
        self.num_businesses = int(os.getenv('TEST_BUSINESSES_COUNT', 100))
        
        # New York cities for realistic data
        self.ny_cities = [
            'New York', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse',
            'Albany', 'New Rochelle', 'Mount Vernon', 'Schenectady', 'Utica'
        ]

    def setup_directories(self):
        """Create necessary directories for test data"""
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Test data directory ensured: {self.output_dir}")
        except Exception as e:
            logger.error(f"Error creating test data directory: {e}")
            sys.exit(1)

    def generate_business_name(self) -> str:
        """Generate a realistic business name"""
        name_patterns = [
            lambda: self.business_gen.company(),
            lambda: f"{self.faker.name()}'s {self.business_gen.company_type()}",
            lambda: f"{self.faker.city()} {self.business_gen.company_type()}",
            lambda: f"The {self.faker.word().title()} {self.business_gen.company_type()}"
        ]
        return self.faker.random_element(name_patterns)()

    def generate_ny_address(self) -> Dict[str, str]:
        """Generate a New York address"""
        return {
            'street': f"{self.faker.building_number()} {self.faker.street_name()}",
            'city': self.faker.random_element(self.ny_cities),
            'state': 'NY',
            'zip_code': self.faker.random_element([
                f"1{self.faker.random_number(4, True)}",  # NYC area
                f"12{self.faker.random_number(3, True)}",  # Albany area
                f"14{self.faker.random_number(3, True)}"   # Buffalo area
            ])
        }

    def generate_violation_data(self) -> Dict[str, str]:
        """Generate violation and date information"""
        base_date = datetime.now() - timedelta(days=self.faker.random_int(30, 365))
        return {
            'violation_type': self.faker.random_element([
                'Health Code Violation',
                'Building Code Violation',
                'Fire Safety Violation',
                'Environmental Violation',
                'Licensing Violation'
            ]),
            'nsl_published_date': base_date.strftime('%Y-%m-%d'),
            'nsl_effective_date': (base_date + timedelta(days=30)).strftime('%Y-%m-%d'),
            'remediated_date': self.faker.random_element([
                (base_date + timedelta(days=self.faker.random_int(31, 90))).strftime('%Y-%m-%d'),
                ''
            ])
        }

    def generate_businesses(self) -> List[Dict[str, str]]:
        """Generate list of business records"""
        businesses = []
        for _ in range(self.num_businesses):
            address = self.generate_ny_address()
            violation = self.generate_violation_data()
            
            business = {
                'business_name': self.generate_business_name(),
                'address': address['street'],
                'city': address['city'],
                'state': address['state'],
                'zip_code': address['zip_code'],
                **violation,
                'verified': self.faker.boolean(chance_of_getting_true=80)
            }
            businesses.append(business)
        
        return businesses

    def generate_test_files(self):
        """Generate all test data files"""
        try:
            businesses = self.generate_businesses()
            
            # Generate input CSV
            input_file = self.output_dir / 'test_businesses.csv'
            input_data = pd.DataFrame([{
                'business_name': b['business_name']
            } for b in businesses])
            input_data.to_csv(input_file, index=False)
            logger.info(f"Generated input file: {input_file}")
            
            # Generate expected output CSV
            output_file = self.output_dir / 'expected_output.csv'
            output_data = pd.DataFrame(businesses)
            output_data.to_csv(output_file, index=False)
            logger.info(f"Generated expected output file: {output_file}")
            
            # Generate mock API responses
            mock_responses = self.generate_mock_responses(businesses)
            mock_file = self.output_dir / 'mock_responses.json'
            with open(mock_file, 'w') as f:
                json.dump(mock_responses, f, indent=2)
            logger.info(f"Generated mock responses: {mock_file}")
            
        except Exception as e:
            logger.error(f"Error generating test files: {e}")
            sys.exit(1)

    def generate_mock_responses(self, businesses: List[Dict[str, str]]) -> Dict[str, List[Dict]]:
        """Generate mock API responses for each business"""
        mock_responses = {}
        
        for business in businesses:
            search_results = []
            # Generate 2-5 mock search results per business
            for _ in range(self.faker.random_int(2, 5)):
                result = {
                    'title': f"About {business['business_name']}",
                    'description': self.faker.paragraph(),
                    'url': self.faker.url(),
                    'relevance_score': round(self.faker.random.uniform(0.5, 1.0), 2)
                }
                search_results.append(result)
            
            mock_responses[business['business_name']] = search_results
        
        return mock_responses

    def run(self):
        """Main execution routine"""
        logger.info("Starting test data generation")
        
        try:
            self.setup_directories()
            self.generate_test_files()
            logger.info("Test data generation completed successfully")
        
        except Exception as e:
            logger.error(f"Test data generation failed: {e}")
            sys.exit(1)

if __name__ == '__main__':
    generator = TestDataGenerator()
    generator.run() 