import unittest
from scraper.validators import AddressValidator, Address
from scraper.exceptions import AddressValidationError

class TestAddressValidator(unittest.TestCase):
    def setUp(self):
        self.validator = AddressValidator()
        self.valid_addresses = [
            "123 Main Street, New York, NY 10001",
            "456 Broadway Ave, Brooklyn, NY 11201",
            "789 Queens Blvd, Queens, NY 11375",
            "321 N Main St, Albany, NY 12207",
            "654 SW Broadway Dr, Buffalo, NY 14201"
        ]
        self.invalid_addresses = [
            "123 Main Street, Los Angeles, CA 90001",  # Wrong state
            "456 Broadway Ave, Brooklyn, NY 00000",    # Invalid ZIP
            "Invalid Address",                         # Malformed
            "",                                        # Empty
            "test@example.com",                        # Email
            "http://example.com",                      # URL
            "123-456-7890",                           # Phone number
            "P.O. Box 123",                           # P.O. Box
            "Private Mailbox 456",                     # Private Mailbox
            "General Delivery",                        # General Delivery
        ]

    def test_valid_addresses(self):
        """Test validation of valid addresses"""
        for address in self.valid_addresses:
            result = self.validator.validate(address)
            self.assertTrue(
                result.is_valid,
                f"Address should be valid: {address}, errors: {result.error_message}"
            )
            self.assertEqual(result.confidence_score, 1.0)

    def test_invalid_addresses(self):
        """Test validation of invalid addresses"""
        for address in self.invalid_addresses:
            result = self.validator.validate(address)
            self.assertFalse(
                result.is_valid,
                f"Address should be invalid: {address}"
            )
            self.assertEqual(result.confidence_score, 0.0)
            self.assertIsNotNone(result.error_message)

    def test_address_components(self):
        """Test parsing and validation of address components"""
        address = "123 Main Street, New York, NY 10001"
        result = self.validator._parse_address(address)
        
        self.assertEqual(result['street'], "123 Main Street")
        self.assertEqual(result['city'], "New York")
        self.assertEqual(result['state'], "NY")
        self.assertEqual(result['zip_code'], "10001")

    def test_street_validation(self):
        """Test street address validation"""
        valid_streets = [
            "123 Main Street",
            "456 Broadway Ave",
            "789 5th Avenue",
            "321 N Main St",
            "654 SW Broadway Dr",
            "987 NE Central Pkwy"
        ]
        invalid_streets = [
            "Main Street",      # No number
            "123",             # No street type
            "",               # Empty
            "ABC Main St",    # Invalid number
            "123 Invalid"     # Invalid street type
        ]

        for street in valid_streets:
            self.assertTrue(
                self.validator._validate_street(street),
                f"Street should be valid: {street}"
            )

        for street in invalid_streets:
            self.assertFalse(
                self.validator._validate_street(street),
                f"Street should be invalid: {street}"
            )

    def test_street_abbreviations(self):
        """Test street type abbreviations"""
        abbreviation_tests = [
            "123 Main St",
            "456 Broadway Ave",
            "789 Central Rd",
            "321 Oak Blvd",
            "654 Pine Ln",
            "987 Maple Dr",
            "147 Cedar Ct",
            "258 Park Cir",
            "369 Market Plz",
            "741 Town Sq"
        ]

        for street in abbreviation_tests:
            self.assertTrue(
                self.validator._validate_street(street),
                f"Street with abbreviation should be valid: {street}"
            )

    def test_direction_abbreviations(self):
        """Test cardinal direction abbreviations"""
        direction_tests = [
            "123 N Main St",
            "456 S Broadway",
            "789 E Central Ave",
            "321 W Oak Rd",
            "654 NE Pine Dr",
            "987 NW Maple Ln",
            "147 SE Cedar Ct",
            "258 SW Park Way"
        ]

        for street in direction_tests:
            self.assertTrue(
                self.validator._validate_street(street),
                f"Street with direction should be valid: {street}"
            )

    def test_city_validation(self):
        """Test city name validation"""
        valid_cities = [
            "New York",
            "Albany-West",
            "St. Louis",
            "Winston-Salem",
            "O'Fallon"
        ]
        invalid_cities = [
            "",                 # Empty
            "A",               # Too short
            "New York123",     # Invalid characters
            "Test@City",       # Invalid characters
        ]

        for city in valid_cities:
            result = self.validator.validate({
                'street': '123 Main St',
                'city': city,
                'state': 'NY',
                'zip_code': '10001'
            })
            self.assertTrue(
                result.is_valid,
                f"City should be valid: {city}, errors: {result.error_message}"
            )

        for city in invalid_cities:
            result = self.validator.validate({
                'street': '123 Main St',
                'city': city,
                'state': 'NY',
                'zip_code': '10001'
            })
            self.assertFalse(
                result.is_valid,
                f"City should be invalid: {city}"
            )

    def test_state_validation(self):
        """Test state validation"""
        test_cases = [
            ('NY', 'NY', True),
            ('CA', 'NY', False),
            ('', 'NY', False),
            ('XX', 'NY', False),
            ('ny', 'NY', True),  # Case insensitive
            ('NY', 'ny', True),  # Case insensitive
        ]

        for state, target, expected in test_cases:
            result = self.validator.validate_state(state, target)
            self.assertEqual(
                result.is_valid,
                expected,
                f"State validation failed for {state} against {target}"
            )

    def test_zip_code_validation(self):
        """Test ZIP code validation"""
        valid_zips = [
            "10001",
            "12345-6789",
            "00123",
            "99999"
        ]
        invalid_zips = [
            "",             # Empty
            "1234",         # Too short
            "123456",       # Too long
            "1234-5678",    # Invalid format
            "ABCDE",        # Non-numeric
            "12345-",       # Incomplete ZIP+4
            "-12345"        # Invalid format
        ]

        for zip_code in valid_zips:
            result = self.validator.validate({
                'street': '123 Main St',
                'city': 'New York',
                'state': 'NY',
                'zip_code': zip_code
            })
            self.assertTrue(
                result.is_valid,
                f"ZIP code should be valid: {zip_code}, errors: {result.error_message}"
            )

        for zip_code in invalid_zips:
            result = self.validator.validate({
                'street': '123 Main St',
                'city': 'New York',
                'state': 'NY',
                'zip_code': zip_code
            })
            self.assertFalse(
                result.is_valid,
                f"ZIP code should be invalid: {zip_code}"
            )

    def test_error_handling(self):
        """Test error handling for malformed addresses"""
        malformed_inputs = [
            None,
            123,
            [],
            {},
            set(),
            lambda x: x
        ]

        for input_value in malformed_inputs:
            result = self.validator.validate(input_value)
            self.assertFalse(result.is_valid)
            self.assertEqual(result.confidence_score, 0.0)
            self.assertEqual(result.error_message, "Invalid address format")

if __name__ == '__main__':
    unittest.main() 