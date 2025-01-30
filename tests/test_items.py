#!/usr/bin/env python3
"""Test suite for scraper items."""

import unittest
from datetime import datetime

from scraper.items import BusinessItem

class TestBusinessItem(unittest.TestCase):
    def setUp(self):
        self.test_data = {
            'business_name': 'Test Business LLC',
            'address': '123 Test St',
            'city': 'New York',
            'state': 'NY',
            'zip_code': '10001',
            'violation_type': 'Health Code Violation',
            'nsl_published_date': '2024-01-01',
            'nsl_effective_date': '2024-02-01',
            'remediated_date': '2024-03-01',
            'verified': True
        }

    def test_item_creation(self):
        item = BusinessItem(**self.test_data)
        
        self.assertEqual(item.business_name, 'Test Business LLC')
        self.assertEqual(item.address, '123 Test St')
        self.assertEqual(item.city, 'New York')
        self.assertEqual(item.state, 'NY')
        self.assertEqual(item.zip_code, '10001')

    def test_date_parsing(self):
        item = BusinessItem(**self.test_data)
        
        self.assertIsInstance(item.nsl_published_date, datetime)
        self.assertIsInstance(item.nsl_effective_date, datetime)
        self.assertIsInstance(item.remediated_date, datetime)

    def test_optional_fields(self):
        data = self.test_data.copy()
        data.pop('remediated_date')
        
        item = BusinessItem(**data)
        self.assertIsNone(item.remediated_date)

    def test_validation(self):
        # Test invalid state
        data = self.test_data.copy()
        data['state'] = 'New York'  # Should be 2-letter code
        
        with self.assertRaises(ValueError):
            BusinessItem(**data)

        # Test invalid zip code
        data = self.test_data.copy()
        data['zip_code'] = '1234'  # Should be 5 digits
        
        with self.assertRaises(ValueError):
            BusinessItem(**data)

    def test_to_dict(self):
        item = BusinessItem(**self.test_data)
        item_dict = item.to_dict()
        
        self.assertIsInstance(item_dict, dict)
        self.assertEqual(item_dict['business_name'], self.test_data['business_name'])
        self.assertEqual(item_dict['zip_code'], self.test_data['zip_code'])

    def test_str_representation(self):
        item = BusinessItem(**self.test_data)
        str_repr = str(item)
        
        self.assertIn(self.test_data['business_name'], str_repr)
        self.assertIn(self.test_data['address'], str_repr)

if __name__ == '__main__':
    unittest.main() 