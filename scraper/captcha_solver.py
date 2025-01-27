import os
import logging
from PIL import Image
import pytesseract
import requests
from io import BytesIO
import sys

class CaptchaSolver:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._configure_tesseract()

    def _configure_tesseract(self):
        """Configure Tesseract path based on the operating system"""
        if sys.platform.startswith('win'):
            tesseract_path = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
            if os.path.exists(tesseract_path):
                pytesseract.pytesseract.tesseract_cmd = tesseract_path
            else:
                self.logger.warning("Tesseract not found in default Windows path")
        else:
            # On Unix-like systems, tesseract should be in PATH
            pass

    def preprocess_image(self, image):
        """Preprocess the captcha image for better OCR results"""
        # Convert to grayscale
        image = image.convert('L')
        
        # Increase contrast
        image = Image.fromarray(255 * (image > 128).astype('uint8'))
        
        return image

    def solve_captcha_from_url(self, captcha_url):
        """Solve captcha from a URL"""
        try:
            # Download the image
            response = requests.get(captcha_url)
            image = Image.open(BytesIO(response.content))
            
            return self.solve_captcha(image)
        except Exception as e:
            self.logger.error(f"Error solving captcha from URL: {e}")
            return None

    def solve_captcha(self, image):
        """Solve the captcha using Tesseract OCR"""
        try:
            # Preprocess the image
            processed_image = self.preprocess_image(image)
            
            # Configure OCR settings for better accuracy with captchas
            custom_config = r'--oem 3 --psm 7 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
            
            # Perform OCR
            text = pytesseract.image_to_string(processed_image, config=custom_config)
            
            # Clean up the result
            text = text.strip()
            
            self.logger.info(f"Captcha solved: {text}")
            return text
        except Exception as e:
            self.logger.error(f"Error solving captcha: {e}")
            return None

    def validate_solution(self, solution):
        """Validate if the captcha solution looks reasonable"""
        if not solution:
            return False
        
        # Check if the solution has a reasonable length
        if len(solution) < 4 or len(solution) > 12:
            return False
        
        # Check if the solution contains at least one letter and one number
        has_letter = any(c.isalpha() for c in solution)
        has_number = any(c.isdigit() for c in solution)
        
        return has_letter or has_number 