#!/usr/bin/env python3
"""Test suite for logging configuration."""

import logging
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from scraper.logging_config import (
    configure_logging,
    setup_file_handler,
    setup_console_handler
)

class TestLoggingConfig(unittest.TestCase):
    def setUp(self):
        self.test_log_dir = Path('tests/logs')
        self.test_log_file = self.test_log_dir / 'test.log'
        
        # Create test log directory
        self.test_log_dir.mkdir(exist_ok=True)
        
        # Reset logging configuration before each test
        logging.getLogger().handlers = []

    def tearDown(self):
        # Clean up test log files
        if self.test_log_file.exists():
            self.test_log_file.unlink()
        if self.test_log_dir.exists():
            self.test_log_dir.rmdir()

    def test_configure_logging(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
            logger = configure_logging('test_logger')
            
            self.assertEqual(logger.level, logging.DEBUG)
            self.assertTrue(any(isinstance(h, logging.StreamHandler) 
                              for h in logger.handlers))

    def test_file_handler_setup(self):
        handler = setup_file_handler(str(self.test_log_file))
        
        self.assertIsInstance(handler, logging.FileHandler)
        self.assertTrue(self.test_log_file.exists())
        
        # Test log rotation settings
        self.assertEqual(handler.maxBytes, 10 * 1024 * 1024)  # 10MB
        self.assertEqual(handler.backupCount, 5)

    def test_console_handler_setup(self):
        handler = setup_console_handler()
        
        self.assertIsInstance(handler, logging.StreamHandler)
        self.assertIsNotNone(handler.formatter)

    def test_log_message_format(self):
        logger = configure_logging('test_logger')
        
        with self.assertLogs('test_logger', level='INFO') as log:
            logger.info('Test message')
            
            self.assertEqual(len(log.records), 1)
            self.assertIn('Test message', log.output[0])
            self.assertIn('INFO', log.output[0])
            self.assertIn('test_logger', log.output[0])

    def test_invalid_log_level(self):
        with patch.dict(os.environ, {'LOG_LEVEL': 'INVALID'}):
            logger = configure_logging('test_logger')
            self.assertEqual(logger.level, logging.INFO)  # Should default to INFO

if __name__ == '__main__':
    unittest.main() 