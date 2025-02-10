#!/usr/bin/env python3
"""Test suite for scraper items."""

import unittest
from datetime import datetime, timedelta
from typing import Dict, Any
from scraper.items import BusinessItem, BusinessData

class TestBusinessData(unittest.TestCase):
    """Test cases for BusinessData class"""

    def setUp(self):
        """Set up test data"""
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.valid_data = {
            'id': 1,
            'business_name': 'Test Business LLC',
            'address': '123 Test St, New York, NY 10001',
            'state': 'NY',
            'zip_code': '10001',
            'created_at': self.current_date
        }

    def create_data(self, **kwargs) -> Dict[str, Any]:
        """Create test data with optional overrides"""
        data = self.valid_data.copy()
        data.update(kwargs)
        return data

    def test_creation_with_valid_data(self):
        """Test creation with valid data"""
        data = BusinessData(**self.valid_data)
        self.assertEqual(data.id, 1)
        self.assertEqual(data.business_name, 'Test Business LLC')
        self.assertEqual(data.state, 'NY')

    def test_optional_fields(self):
        """Test creation with optional fields omitted"""
        data = self.create_data()
        del data['state']
        del data['zip_code']
        
        business = BusinessData(**data)
        self.assertIsNone(business.state)
        self.assertIsNone(business.zip_code)

    def test_type_validation(self):
        """Test type validation for all fields"""
        invalid_types = [
            ('id', 'not an int', TypeError),
            ('business_name', 123, TypeError),
            ('address', ['invalid'], TypeError),
            ('state', 123, TypeError),
            ('zip_code', 12345, TypeError),
            ('created_at', datetime.now(), TypeError)
        ]
        
        for field, value, error in invalid_types:
            with self.subTest(field=field, value=value):
                data = self.create_data(**{field: value})
                with self.assertRaises(error):
                    BusinessData(**data)

    def test_value_validation(self):
        """Test value validation for all fields"""
        invalid_values = [
            ('id', -1, ValueError, "must be a positive integer"),
            ('business_name', 'A', ValueError, "length must be between"),
            ('business_name', 'A' * 201, ValueError, "length must be between"),
            ('address', '12', ValueError, "length must be between"),
            ('state', '123', ValueError, "must be a two-letter code"),
            ('zip_code', '123', ValueError, "must be in format"),
            ('created_at', '2024/01/01', ValueError, "must be in format")
        ]
        
        for field, value, error, message in invalid_values:
            with self.subTest(field=field, value=value):
                data = self.create_data(**{field: value})
                with self.assertRaises(error) as context:
                    BusinessData(**data)
                self.assertIn(message, str(context.exception))

    def test_data_cleaning(self):
        """Test data cleaning functionality"""
        dirty_data = {
            'id': '1',
            'business_name': '  Test Business  ',
            'address': '  123 Test St  ',
            'state': '  ny  ',
            'zip_code': '  10001  ',
            'created_at': self.current_date
        }
        
        data = BusinessData.from_dict(dirty_data)
        self.assertEqual(data.id, 1)
        self.assertEqual(data.business_name, 'Test Business')
        self.assertEqual(data.address, '123 Test St')
        self.assertEqual(data.state, 'NY')
        self.assertEqual(data.zip_code, '10001')

    def test_future_date_validation(self):
        """Test validation of future dates"""
        future_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        data = self.create_data(created_at=future_date)
        
        with self.assertRaises(ValueError) as context:
            BusinessData(**data)
        self.assertIn("cannot be in the future", str(context.exception))

    def test_required_fields(self):
        """Test required fields validation"""
        required_fields = BusinessData.REQUIRED_FIELDS
        
        for field in required_fields:
            with self.subTest(field=field):
                data = self.create_data()
                del data[field]
                
                with self.assertRaises(KeyError) as context:
                    BusinessData.from_dict(data)
                self.assertIn("Missing required fields", str(context.exception))

class TestBusinessItem(unittest.TestCase):
    """Test cases for BusinessItem class"""

    def setUp(self):
        """Set up test data"""
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        self.test_data = {
            'id': 1,
            'business_name': 'Test Business LLC',
            'address': '123 Test St, New York, NY 10001',
            'state': 'NY',
            'zip_code': '10001',
            'created_at': self.current_date
        }

    def test_item_creation(self):
        """Test basic item creation"""
        item = BusinessItem(**self.test_data)
        
        self.assertEqual(item['id'], 1)
        self.assertEqual(item['business_name'], 'Test Business LLC')
        self.assertEqual(item['address'], '123 Test St, New York, NY 10001')
        self.assertEqual(item['state'], 'NY')
        self.assertEqual(item['zip_code'], '10001')
        self.assertEqual(item['created_at'], self.current_date)

    def test_optional_fields(self):
        """Test item creation with optional fields"""
        data = self.test_data.copy()
        data.pop('state')
        data.pop('zip_code')
        
        item = BusinessItem(**data)
        self.assertEqual(item['business_name'], 'Test Business LLC')
        self.assertEqual(item['address'], '123 Test St, New York, NY 10001')
        self.assertNotIn('state', item)
        self.assertNotIn('zip_code', item)

    def test_to_dict(self):
        """Test conversion to dictionary"""
        item = BusinessItem(**self.test_data)
        item_dict = item.to_dict()
        
        self.assertEqual(item_dict['id'], 1)
        self.assertEqual(item_dict['business_name'], 'Test Business LLC')
        self.assertEqual(item_dict['address'], '123 Test St, New York, NY 10001')
        self.assertEqual(item_dict['state'], 'NY')
        self.assertEqual(item_dict['zip_code'], '10001')
        self.assertEqual(item_dict['created_at'], self.current_date)
        
        # Test con campos opcionales omitidos
        data = self.test_data.copy()
        data.pop('state')
        data.pop('zip_code')
        item = BusinessItem(**data)
        item_dict = item.to_dict()
        self.assertNotIn('state', item_dict)
        self.assertNotIn('zip_code', item_dict)

    def test_str_representation(self):
        """Test string representation"""
        item = BusinessItem(**self.test_data)
        str_repr = str(item)
        
        self.assertIn('Test Business LLC', str_repr)
        self.assertIn('123 Test St, New York, NY 10001', str_repr)
        self.assertIn('NY', str_repr)
        self.assertIn('10001', str_repr)
        
        # Test with missing optional fields
        data = self.test_data.copy()
        data.pop('state')
        data.pop('zip_code')
        item = BusinessItem(**data)
        str_repr = str(item)
        
        self.assertIn('N/A', str_repr)

    def test_from_business_data(self):
        """Test creation from BusinessData"""
        business_data = BusinessData(**self.test_data)
        item = BusinessItem.from_business_data(business_data)
        
        self.assertEqual(item['id'], business_data.id)
        self.assertEqual(item['business_name'], business_data.business_name)
        self.assertEqual(item['address'], business_data.address)
        
        # Test invalid input
        with self.assertRaises(TypeError):
            BusinessItem.from_business_data({'id': 1})
            
        # Test empty required field
        with self.assertRaises(ValueError):
            data = self.test_data.copy()
            data['business_name'] = ''
            BusinessItem.from_business_data(BusinessData(**data))

    def test_from_dict(self):
        """Test creation from dictionary"""
        item = BusinessItem.from_dict(self.test_data)
        self.assertEqual(item['id'], 1)
        self.assertEqual(item['business_name'], 'Test Business LLC')
        
        # Test conversión de tipos
        data = self.test_data.copy()
        data['id'] = '1'
        item = BusinessItem.from_dict(data)
        self.assertEqual(item['id'], 1)
        
        # Test datos inválidos
        invalid_data = self.test_data.copy()
        invalid_data['id'] = 'not a number'
        with self.assertRaises(TypeError):
            BusinessItem.from_dict(invalid_data)

if __name__ == '__main__':
    unittest.main() 