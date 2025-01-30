import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from PIL import Image
import io
from scraper.captcha_solver import CaptchaSolver

class TestCaptchaSolver(unittest.TestCase):
    def setUp(self):
        self.solver = CaptchaSolver()
        # Create a simple test image
        self.test_image = Image.new('RGB', (100, 30), color='white')

    @patch('os.path.exists')
    def test_configure_tesseract_windows(self, mock_exists):
        """Test Tesseract configuration on Windows"""
        with patch.object(sys, 'platform', 'win32'):
            mock_exists.return_value = True
            solver = CaptchaSolver()
            mock_exists.assert_called_once()

    @patch('os.path.exists')
    def test_configure_tesseract_linux(self, mock_exists):
        """Test Tesseract configuration on Linux"""
        with patch.object(sys, 'platform', 'linux'):
            solver = CaptchaSolver()
            mock_exists.assert_not_called()

    def test_preprocess_image(self):
        """Test image preprocessing"""
        processed = self.solver.preprocess_image(self.test_image)
        
        # Check if image was converted to grayscale
        self.assertEqual(processed.mode, 'L')
        
        # Check if image dimensions are preserved
        self.assertEqual(processed.size, self.test_image.size)

    @patch('requests.get')
    def test_solve_captcha_from_url_success(self, mock_get):
        """Test solving captcha from URL - success case"""
        # Create a mock response with a test image
        mock_response = MagicMock()
        img_byte_arr = io.BytesIO()
        self.test_image.save(img_byte_arr, format='PNG')
        mock_response.content = img_byte_arr.getvalue()
        mock_get.return_value = mock_response

        with patch.object(self.solver, 'solve_captcha', return_value='TEST123'):
            result = self.solver.solve_captcha_from_url('http://test.com/captcha.png')
            self.assertEqual(result, 'TEST123')

    @patch('requests.get')
    def test_solve_captcha_from_url_failure(self, mock_get):
        """Test solving captcha from URL - failure case"""
        mock_get.side_effect = Exception('Network error')
        
        result = self.solver.solve_captcha_from_url('http://test.com/captcha.png')
        self.assertIsNone(result)

    @patch('pytesseract.image_to_string')
    def test_solve_captcha_success(self, mock_image_to_string):
        """Test solving captcha - success case"""
        mock_image_to_string.return_value = 'TEST123\n'
        
        result = self.solver.solve_captcha(self.test_image)
        self.assertEqual(result, 'TEST123')

    @patch('pytesseract.image_to_string')
    def test_solve_captcha_failure(self, mock_image_to_string):
        """Test solving captcha - failure case"""
        mock_image_to_string.side_effect = Exception('OCR error')
        
        result = self.solver.solve_captcha(self.test_image)
        self.assertIsNone(result)

    def test_validate_solution(self):
        """Test captcha solution validation"""
        valid_solutions = [
            'ABC123',    # Letters and numbers
            'TEST456',   # Letters and numbers
            'XYZ789'     # Letters and numbers
        ]
        invalid_solutions = [
            '',         # Empty
            'ABC',      # Too short
            '123',      # Only numbers
            'ABCDEF',   # Only letters
            'A' * 13    # Too long
        ]

        for solution in valid_solutions:
            self.assertTrue(self.solver.validate_solution(solution))

        for solution in invalid_solutions:
            self.assertFalse(self.solver.validate_solution(solution))

    def test_validate_solution_edge_cases(self):
        """Test captcha solution validation edge cases"""
        edge_cases = [
            None,       # None value
            123,        # Integer
            [],         # List
            {},         # Dictionary
        ]

        for case in edge_cases:
            self.assertFalse(self.solver.validate_solution(case))

    def tearDown(self):
        """Clean up after tests"""
        self.test_image.close()

if __name__ == '__main__':
    unittest.main() 