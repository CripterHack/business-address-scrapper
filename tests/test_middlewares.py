#!/usr/bin/env python3
"""Test suite for middleware components."""

import unittest
from unittest.mock import Mock, patch

from scraper.middlewares import (
    ProxyMiddleware,
    RetryMiddleware,
    UserAgentMiddleware
)

class TestProxyMiddleware(unittest.TestCase):
    def setUp(self):
        self.middleware = ProxyMiddleware()
        self.spider = Mock()
        self.request = Mock()

    def test_process_request(self):
        # Test proxy rotation
        with patch('scraper.middlewares.get_proxy') as mock_get_proxy:
            mock_get_proxy.return_value = {'http': 'http://proxy.example.com:8080'}
            self.middleware.process_request(self.request, self.spider)
            self.assertEqual(
                self.request.meta['proxy'],
                'http://proxy.example.com:8080'
            )

    def test_process_response(self):
        response = Mock(status=200)
        result = self.middleware.process_response(self.request, response, self.spider)
        self.assertEqual(result, response)

class TestRetryMiddleware(unittest.TestCase):
    def setUp(self):
        self.middleware = RetryMiddleware()
        self.spider = Mock()
        self.request = Mock()

    def test_process_response_retry(self):
        response = Mock(status=503)
        self.request.meta = {'retry_count': 0}
        
        with self.assertRaises(Exception):  # Should raise retry exception
            self.middleware.process_response(self.request, response, self.spider)

    def test_process_response_max_retries(self):
        response = Mock(status=503)
        self.request.meta = {'retry_count': 3}
        
        result = self.middleware.process_response(self.request, response, self.spider)
        self.assertEqual(result, response)

class TestUserAgentMiddleware(unittest.TestCase):
    def setUp(self):
        self.middleware = UserAgentMiddleware()
        self.spider = Mock()
        self.request = Mock()

    def test_process_request(self):
        self.middleware.process_request(self.request, self.spider)
        self.assertIn('User-Agent', self.request.headers)
        self.assertTrue(
            self.request.headers['User-Agent'].startswith(b'Business Address Scraper')
        )

if __name__ == '__main__':
    unittest.main() 