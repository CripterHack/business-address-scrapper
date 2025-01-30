import unittest
from unittest.mock import patch, MagicMock
from scrapy.http import Request, Response
from scrapy.exceptions import DropItem
from scraper.spiders.business_spider import BusinessSpider

class TestBusinessSpider(unittest.TestCase):
    def setUp(self):
        self.spider = BusinessSpider()
        self.mock_response = MagicMock(spec=Response)
        self.mock_response.url = "http://test.com"

    @patch('pandas.read_csv')
    def test_load_businesses(self, mock_read_csv):
        """Test loading businesses from CSV"""
        # Mock DataFrame
        mock_df = MagicMock()
        mock_df['business_name'].tolist.return_value = ["Business1", "Business2"]
        mock_read_csv.return_value = mock_df

        businesses = self.spider._load_businesses()
        self.assertEqual(businesses, ["Business1", "Business2"])

    def test_is_valid_url(self):
        """Test URL validation"""
        valid_urls = [
            "https://www.ny.gov/business",
            "http://nyc.gov/info",
            "https://www.yellowpages.com/ny/business"
        ]
        invalid_urls = [
            "https://www.invalid.com",
            "not_a_url",
            "http://malicious.site.com"
        ]

        for url in valid_urls:
            self.assertTrue(self.spider._is_valid_url(url))

        for url in invalid_urls:
            self.assertFalse(self.spider._is_valid_url(url))

    @patch('requests.get')
    def test_qwant_search(self, mock_get):
        """Test Qwant API search"""
        # Mock successful response
        mock_get.return_value.json.return_value = {
            "data": {
                "result": {
                    "items": [
                        {"url": "http://test.com", "description": "Test"}
                    ]
                }
            }
        }
        mock_get.return_value.raise_for_status = MagicMock()

        result = self.spider._qwant_search("Test Business")
        self.assertIn("data", result)
        self.assertIn("result", result["data"])

    def test_parse_business_page(self):
        """Test parsing business page"""
        # Mock response with test data
        html_content = """
        <html>
            <body>
                <address>123 Main St, New York, NY 10001</address>
                <div class="violation-type">Type A</div>
                <div class="publish-date">2024-01-01</div>
            </body>
        </html>
        """
        self.mock_response.xpath = MagicMock()
        self.mock_response.xpath().get.return_value = "Test Data"
        self.mock_response.xpath().getall.return_value = ["123 Main St, New York, NY 10001"]
        self.mock_response.meta = {
            'business_name': 'Test Business',
            'search_snippet': 'Test Description',
            'relevance_score': 0.8
        }

        results = list(self.spider.parse_business_page(self.mock_response))
        self.assertTrue(len(results) > 0)
        self.assertEqual(results[0]['business_name'], 'Test Business')

    def test_validate_ny_address(self):
        """Test NY address validation"""
        valid_address = "NY"
        invalid_address = "CA"

        self.assertTrue(self.spider._validate_ny_address(valid_address))
        self.assertFalse(self.spider._validate_ny_address(invalid_address))

    def test_parse_address(self):
        """Test address parsing"""
        test_address = "123 Main St, New York, NY 10001"
        result = self.spider._parse_address(test_address)

        self.assertEqual(result['address'], "123 Main St")
        self.assertEqual(result['city'], "New York")
        self.assertEqual(result['state'], "NY")
        self.assertEqual(result['zip_code'], "10001")

    def test_parse_date(self):
        """Test date parsing"""
        test_dates = {
            "2024-01-01": "2024-01-01",
            "01/01/2024": "2024-01-01",
            "invalid_date": None
        }

        for input_date, expected in test_dates.items():
            result = self.spider._parse_date(input_date)
            self.assertEqual(result, expected)

    def test_error_handling(self):
        """Test error handling"""
        mock_failure = MagicMock()
        mock_failure.request.url = "http://test.com"
        mock_failure.value = Exception("Test error")
        mock_failure.check.return_value = False

        # Should not raise exception
        self.spider.handle_error(mock_failure)

if __name__ == '__main__':
    unittest.main() 