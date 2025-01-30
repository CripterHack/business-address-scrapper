import unittest
from unittest.mock import patch, mock_open, MagicMock
import os
import json
from datetime import datetime
from scraper.utils import (
    setup_logging,
    load_config,
    generate_cache_key,
    is_valid_domain,
    save_to_json,
    load_from_json,
    format_date,
    clean_text,
    extract_domain,
    create_directory,
    get_file_size,
    is_file_empty,
    backup_file,
    restore_backup
)
from scraper.exceptions import ConfigurationError, StorageError

class TestUtils(unittest.TestCase):
    def setUp(self):
        self.test_data = {'key': 'value'}
        self.test_file = 'test.json'

    @patch('logging.basicConfig')
    def test_setup_logging(self, mock_basic_config):
        """Test logging setup"""
        setup_logging('test.log', 'DEBUG')
        mock_basic_config.assert_called_once()

    def test_load_config_success(self):
        """Test successful config loading"""
        mock_content = 'KEY1=value1\nKEY2=value2\n# Comment\n'
        with patch('builtins.open', mock_open(read_data=mock_content)):
            with patch('os.path.exists', return_value=True):
                config = load_config('test.env')
                self.assertEqual(config['KEY1'], 'value1')
                self.assertEqual(config['KEY2'], 'value2')

    def test_load_config_file_not_found(self):
        """Test config loading with missing file"""
        with patch('os.path.exists', return_value=False):
            with self.assertRaises(ConfigurationError):
                load_config('nonexistent.env')

    def test_generate_cache_key(self):
        """Test cache key generation"""
        url = 'http://test.com'
        params = {'key': 'value'}
        
        # Test with URL only
        key1 = generate_cache_key(url)
        self.assertTrue(isinstance(key1, str))
        
        # Test with URL and params
        key2 = generate_cache_key(url, params)
        self.assertTrue(isinstance(key2, str))
        self.assertNotEqual(key1, key2)

    def test_is_valid_domain(self):
        """Test domain validation"""
        allowed_domains = ['example.com', 'test.org']
        
        valid_urls = [
            'https://example.com/path',
            'http://test.org/page'
        ]
        invalid_urls = [
            'https://invalid.com',
            'not_a_url',
            ''
        ]

        for url in valid_urls:
            self.assertTrue(is_valid_domain(url, allowed_domains))

        for url in invalid_urls:
            self.assertFalse(is_valid_domain(url, allowed_domains))

    @patch('json.dump')
    def test_save_to_json(self, mock_dump):
        """Test JSON saving"""
        with patch('builtins.open', mock_open()):
            save_to_json(self.test_data, self.test_file)
            mock_dump.assert_called_once()

    @patch('json.load')
    def test_load_from_json(self, mock_load):
        """Test JSON loading"""
        mock_load.return_value = self.test_data
        with patch('builtins.open', mock_open()):
            data = load_from_json(self.test_file)
            self.assertEqual(data, self.test_data)

    def test_format_date(self):
        """Test date formatting"""
        test_cases = {
            '2024-01-01': '2024-01-01',
            '': None,
            'invalid': None
        }

        for input_date, expected in test_cases.items():
            result = format_date(input_date)
            self.assertEqual(result, expected)

    def test_clean_text(self):
        """Test text cleaning"""
        test_cases = {
            ' test  text ': 'test text',
            '': '',
            None: '',
            '  multiple   spaces  ': 'multiple spaces'
        }

        for input_text, expected in test_cases.items():
            result = clean_text(input_text)
            self.assertEqual(result, expected)

    def test_extract_domain(self):
        """Test domain extraction"""
        test_cases = {
            'https://example.com/path': 'example.com',
            'http://test.org': 'test.org',
            'invalid_url': '',
            '': ''
        }

        for url, expected in test_cases.items():
            result = extract_domain(url)
            self.assertEqual(result, expected)

    @patch('os.makedirs')
    def test_create_directory(self, mock_makedirs):
        """Test directory creation"""
        create_directory('test_dir')
        mock_makedirs.assert_called_once_with('test_dir', exist_ok=True)

    @patch('os.path.getsize')
    def test_get_file_size(self, mock_getsize):
        """Test file size checking"""
        mock_getsize.return_value = 1024
        size = get_file_size('test.txt')
        self.assertEqual(size, 1024)

    def test_is_file_empty(self):
        """Test empty file checking"""
        with patch('scraper.utils.get_file_size', return_value=0):
            self.assertTrue(is_file_empty('empty.txt'))
        
        with patch('scraper.utils.get_file_size', return_value=100):
            self.assertFalse(is_file_empty('nonempty.txt'))

    @patch('builtins.open', new_callable=mock_open)
    def test_backup_file(self, mock_file):
        """Test file backup"""
        with patch('os.path.exists', return_value=True):
            backup_path = backup_file('test.txt')
            self.assertTrue(isinstance(backup_path, str))
            self.assertTrue(backup_path.endswith('.bak'))

    @patch('builtins.open', new_callable=mock_open)
    def test_restore_backup(self, mock_file):
        """Test backup restoration"""
        restore_backup('backup.txt', 'original.txt')
        mock_file.assert_called()

    def test_error_handling(self):
        """Test error handling in utility functions"""
        # Test save_to_json error
        with patch('builtins.open', side_effect=Exception):
            with self.assertRaises(StorageError):
                save_to_json({}, 'test.json')

        # Test load_from_json error
        with patch('builtins.open', side_effect=Exception):
            with self.assertRaises(StorageError):
                load_from_json('test.json')

        # Test create_directory error
        with patch('os.makedirs', side_effect=Exception):
            with self.assertRaises(StorageError):
                create_directory('test_dir')

if __name__ == '__main__':
    unittest.main() 