import unittest
from scraper.validators import AddressValidator, Address
from scraper.exceptions import AddressValidationError

class TestAddressValidator(unittest.TestCase):
    def setUp(self):
        self.validator = AddressValidator()
        self.valid_addresses = [
            "123 Main Street, New York, NY 10001",
            "456 Broadway Ave, Brooklyn, NY 11201",
            "789 Queens Blvd, Queens, NY 11375"
        ]
        self.invalid_addresses = [
            "123 Main Street, Los Angeles, CA 90001",  # Wrong state
            "456 Broadway Ave, Brooklyn, NY 00000",    # Invalid ZIP
            "Invalid Address",                         # Malformed
            "",                                        # Empty
        ]

    def test_valid_addresses(self):
        """Test validation of valid NY addresses"""
        for address in self.valid_addresses:
            result = AddressValidator.validate_address(address)
            self.assertTrue(result.is_valid)
            self.assertEqual(result.state, "NY")

    def test_invalid_addresses(self):
        """Test validation of invalid addresses"""
        for address in self.invalid_addresses:
            result = AddressValidator.validate_address(address)
            self.assertFalse(result.is_valid)

    def test_zip_code_validation(self):
        """Test ZIP code validation"""
        valid_zips = ["10001", "11201", "14925"]
        invalid_zips = ["00000", "99999", "123", "abcde"]

        for zip_code in valid_zips:
            self.assertTrue(AddressValidator._is_valid_ny_zip(zip_code))

        for zip_code in invalid_zips:
            self.assertFalse(AddressValidator._is_valid_ny_zip(zip_code))

    def test_street_validation(self):
        """Test street address validation"""
        valid_streets = [
            "123 Main Street",
            "456 Broadway Ave",
            "789 5th Avenue"
        ]
        invalid_streets = [
            "Main Street",      # No number
            "123",             # No street type
            "",               # Empty
        ]

        for street in valid_streets:
            self.assertTrue(AddressValidator._is_valid_street(street))

        for street in invalid_streets:
            self.assertFalse(AddressValidator._is_valid_street(street))

    def test_city_validation(self):
        """Test city validation"""
        for city in AddressValidator.NY_CITIES:
            address = f"123 Main St, {city}, NY 10001"
            result = AddressValidator.validate_address(address)
            self.assertTrue(result.is_valid)

    def test_address_components(self):
        """Test parsing of address components"""
        address = "123 Main Street, New York, NY 10001"
        result = AddressValidator.validate_address(address)
        
        self.assertEqual(result.street, "123 MAIN STREET")
        self.assertEqual(result.city, "NEW YORK")
        self.assertEqual(result.state, "NY")
        self.assertEqual(result.zip_code, "10001")

    def test_error_handling(self):
        """Test error handling for malformed addresses"""
        malformed_addresses = [
            None,
            123,
            {},
            []
        ]

        for address in malformed_addresses:
            with self.assertRaises(AddressValidationError):
                AddressValidator.validate_address(address)

if __name__ == '__main__':
    unittest.main() 