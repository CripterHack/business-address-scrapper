import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
from datetime import datetime
from scraper.pipelines import BusinessDataPipeline, DuplicateFilterPipeline, BusinessData

class TestBusinessDataPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = BusinessDataPipeline()
        self.spider = MagicMock()
        self.test_item = {
            'business_name': 'Test Business',
            'address': '123 Main St',
            'city': 'New York',
            'state': 'NY',
            'zip_code': '10001',
            'violation_type': 'Type A',
            'nsl_published_date': '2024-01-01',
            'nsl_effective_date': '2024-01-02',
            'remediated_date': '2024-01-03',
            'verified': True,
            'source_url': 'http://test.com',
            'relevance_score': 0.8
        }

    def test_clean_item(self):
        """Test item cleaning and validation"""
        cleaned_item = self.pipeline._clean_item(self.test_item)
        self.assertIsInstance(cleaned_item, BusinessData)
        self.assertEqual(cleaned_item.business_name, 'Test Business')
        self.assertEqual(cleaned_item.zip_code, '10001')

    def test_clean_text(self):
        """Test text cleaning"""
        test_cases = {
            'Normal Text': 'Normal Text',
            ' Extra  Spaces ': 'Extra Spaces',
            'Special@#$Characters': 'SpecialCharacters',
            '': '',
            None: ''
        }
        for input_text, expected in test_cases.items():
            result = self.pipeline._clean_text(input_text)
            self.assertEqual(result, expected)

    def test_clean_state(self):
        """Test state code cleaning"""
        test_cases = {
            'NY': 'NY',
            'new york': '',
            '123': '',
            '': ''
        }
        for input_state, expected in test_cases.items():
            result = self.pipeline._clean_state(input_state)
            self.assertEqual(result, expected)

    def test_clean_zip(self):
        """Test ZIP code cleaning"""
        test_cases = {
            '10001': '10001',
            '10001-1234': '10001',
            'abc': '',
            '123': '',
            '': ''
        }
        for input_zip, expected in test_cases.items():
            result = self.pipeline._clean_zip(input_zip)
            self.assertEqual(result, expected)

    def test_clean_date(self):
        """Test date cleaning"""
        test_cases = {
            '2024-01-01': '2024-01-01',
            '01/01/2024': '2024-01-01',
            'invalid': '',
            '': ''
        }
        for input_date, expected in test_cases.items():
            result = self.pipeline._clean_date(input_date)
            self.assertEqual(result, expected)

    def test_validate_ny_address(self):
        """Test NY address validation"""
        valid_item = BusinessData(
            business_name='Test',
            state='NY',
            zip_code='10001'
        )
        invalid_item = BusinessData(
            business_name='Test',
            state='CA',
            zip_code='90001'
        )

        self.assertTrue(self.pipeline._validate_ny_address(valid_item))
        self.assertFalse(self.pipeline._validate_ny_address(invalid_item))

    @patch('pandas.DataFrame.to_csv')
    def test_write_chunk(self, mock_to_csv):
        """Test chunk writing"""
        self.pipeline.items = [
            BusinessData(business_name='Test1'),
            BusinessData(business_name='Test2')
        ]
        self.pipeline._write_chunk()
        mock_to_csv.assert_called_once()
        self.assertEqual(len(self.pipeline.items), 0)

    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_ensure_output_dir(self, mock_makedirs, mock_exists):
        """Test output directory creation"""
        mock_exists.return_value = False
        self.pipeline._ensure_output_dir()
        mock_makedirs.assert_called_once_with('output')

class TestDuplicateFilterPipeline(unittest.TestCase):
    def setUp(self):
        self.pipeline = DuplicateFilterPipeline()
        self.spider = MagicMock()

    def test_process_item(self):
        """Test duplicate filtering"""
        item1 = {
            'business_name': 'Test Business',
            'address': '123 Main St'
        }
        item2 = {
            'business_name': 'Test Business',
            'address': '123 Main St'  # Duplicate
        }
        item3 = {
            'business_name': 'Test Business',
            'address': '456 Other St'  # Different address
        }

        # First item should pass
        result1 = self.pipeline.process_item(item1, self.spider)
        self.assertEqual(result1, item1)

        # Duplicate item should be filtered
        result2 = self.pipeline.process_item(item2, self.spider)
        self.assertIsNone(result2)

        # Different address should pass
        result3 = self.pipeline.process_item(item3, self.spider)
        self.assertEqual(result3, item3)

    def test_generate_business_key(self):
        """Test business key generation"""
        item = {
            'business_name': 'Test Business',
            'address': '123 Main St'
        }
        expected_key = 'test business|123 main st'
        result = self.pipeline._generate_business_key(item)
        self.assertEqual(result, expected_key)

    def test_error_handling(self):
        """Test error handling in key generation"""
        invalid_item = None
        result = self.pipeline._generate_business_key(invalid_item)
        self.assertEqual(result, '')

if __name__ == '__main__':
    unittest.main() 