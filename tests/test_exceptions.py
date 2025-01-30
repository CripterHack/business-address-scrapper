#!/usr/bin/env python3
"""Test suite for custom exceptions."""

import unittest

from scraper.exceptions import (
    ScraperException,
    ValidationError,
    ConfigurationError,
    ProxyError,
    CaptchaError,
    LLaMAError
)

class TestExceptions(unittest.TestCase):
    def test_scraper_exception(self):
        with self.assertRaises(ScraperException) as context:
            raise ScraperException('Test error')
        
        self.assertEqual(str(context.exception), 'Test error')
        self.assertIsInstance(context.exception, Exception)

    def test_validation_error(self):
        with self.assertRaises(ValidationError) as context:
            raise ValidationError('Invalid data format')
        
        self.assertEqual(str(context.exception), 'Invalid data format')
        self.assertIsInstance(context.exception, ScraperException)

    def test_configuration_error(self):
        with self.assertRaises(ConfigurationError) as context:
            raise ConfigurationError('Missing required setting')
        
        self.assertEqual(str(context.exception), 'Missing required setting')
        self.assertIsInstance(context.exception, ScraperException)

    def test_proxy_error(self):
        with self.assertRaises(ProxyError) as context:
            raise ProxyError('Proxy connection failed')
        
        self.assertEqual(str(context.exception), 'Proxy connection failed')
        self.assertIsInstance(context.exception, ScraperException)

    def test_captcha_error(self):
        with self.assertRaises(CaptchaError) as context:
            raise CaptchaError('Failed to solve captcha')
        
        self.assertEqual(str(context.exception), 'Failed to solve captcha')
        self.assertIsInstance(context.exception, ScraperException)

    def test_llama_error(self):
        with self.assertRaises(LLaMAError) as context:
            raise LLaMAError('Model inference failed')
        
        self.assertEqual(str(context.exception), 'Model inference failed')
        self.assertIsInstance(context.exception, ScraperException)

    def test_exception_with_details(self):
        details = {'status_code': 404, 'url': 'http://example.com'}
        
        with self.assertRaises(ScraperException) as context:
            raise ScraperException('Request failed', details=details)
        
        self.assertEqual(context.exception.details, details)

if __name__ == '__main__':
    unittest.main() 