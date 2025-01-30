#!/usr/bin/env python3
"""Test suite for settings module."""

import os
import unittest
from unittest.mock import patch

from scraper.settings import Settings, load_settings

class TestSettings(unittest.TestCase):
    def setUp(self):
        self.test_config = {
            'scraper': {
                'mode': 'testing',
                'rate_limit': 5,
                'max_retries': 3
            },
            'output': {
                'format': 'csv',
                'directory': 'test_output'
            }
        }

    @patch('scraper.settings.load_yaml_config')
    def test_load_settings(self, mock_load_yaml):
        mock_load_yaml.return_value = self.test_config
        settings = load_settings('test_config.yaml')
        
        self.assertEqual(settings.scraper_mode, 'testing')
        self.assertEqual(settings.rate_limit, 5)
        self.assertEqual(settings.max_retries, 3)
        self.assertEqual(settings.output_format, 'csv')
        self.assertEqual(settings.output_directory, 'test_output')

    def test_settings_validation(self):
        with self.assertRaises(ValueError):
            Settings(scraper_mode='invalid_mode')

    def test_environment_override(self):
        with patch.dict(os.environ, {'SCRAPER_MODE': 'production'}):
            settings = Settings(scraper_mode='testing')
            self.assertEqual(settings.scraper_mode, 'production')

    def test_default_values(self):
        settings = Settings()
        self.assertEqual(settings.scraper_mode, 'development')
        self.assertEqual(settings.rate_limit, 1)
        self.assertTrue(settings.respect_robots_txt)

if __name__ == '__main__':
    unittest.main() 