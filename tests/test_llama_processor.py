import unittest
from unittest.mock import patch, MagicMock
import os
from scraper.llama_processor import LlamaProcessor
from scraper.exceptions import ModelError

class TestLlamaProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = LlamaProcessor()

    @patch('os.path.exists')
    @patch('llama_cpp.Llama')
    def test_load_model_success(self, mock_llama, mock_exists):
        """Test successful model loading"""
        mock_exists.return_value = True
        mock_llama.return_value = MagicMock()
        
        with patch.dict(os.environ, {'LLAMA_MODEL_PATH': '/path/to/model'}):
            processor = LlamaProcessor()
            self.assertIsNotNone(processor.model)

    @patch('os.path.exists')
    def test_load_model_failure(self, mock_exists):
        """Test model loading failure"""
        mock_exists.return_value = False
        
        processor = LlamaProcessor()
        self.assertIsNone(processor.model)

    def test_enhance_query_with_model(self):
        """Test query enhancement with model"""
        self.processor.model = MagicMock()
        self.processor.model.return_value = {
            'choices': [{'text': '"enhanced business query"'}]
        }

        result = self.processor.enhance_query("test business")
        self.assertEqual(result, "enhanced business query")

    def test_enhance_query_fallback(self):
        """Test query enhancement fallback"""
        self.processor.model = None
        business_name = "Test Business"
        
        result = self.processor.enhance_query(business_name)
        expected = f"{business_name} business address location New York NY official records"
        self.assertEqual(result, expected)

    def test_parse_llama_response(self):
        """Test LLaMA response parsing"""
        test_cases = {
            'raw response': 'raw response',
            '"quoted response"': 'quoted response',
            'response with\nnewlines': 'response with newlines',
            '  spaces  ': 'spaces'
        }

        for input_text, expected in test_cases.items():
            result = self.processor._parse_llama_response(input_text)
            self.assertEqual(result, expected)

    def test_analyze_search_results_with_model(self):
        """Test search results analysis with model"""
        self.processor.model = MagicMock()
        self.processor.model.return_value = {
            'choices': [{'text': '0.8'}]
        }

        test_results = [
            {'url': 'http://test1.com', 'title': 'Test 1', 'description': 'Desc 1'},
            {'url': 'http://test2.com', 'title': 'Test 2', 'description': 'Desc 2'}
        ]

        results = self.processor.analyze_search_results(test_results)
        self.assertEqual(len(results), 2)
        self.assertTrue(all('relevance_score' in r for r in results))

    def test_analyze_search_results_without_model(self):
        """Test search results analysis without model"""
        self.processor.model = None
        test_results = [
            {'url': 'http://test.com', 'title': 'Test', 'description': 'Desc'}
        ]

        results = self.processor.analyze_search_results(test_results)
        self.assertEqual(results, test_results)

    def test_calculate_relevance(self):
        """Test relevance score calculation"""
        self.processor.model = MagicMock()
        self.processor.model.return_value = {
            'choices': [{'text': '0.75'}]
        }

        result = {
            'title': 'Test Business',
            'description': 'Business description',
            'url': 'http://test.com'
        }

        score = self.processor._calculate_relevance(result)
        self.assertEqual(score, 0.75)

    def test_calculate_relevance_error(self):
        """Test relevance calculation error handling"""
        self.processor.model = MagicMock()
        self.processor.model.return_value = {
            'choices': [{'text': 'invalid score'}]
        }

        result = {
            'title': 'Test Business',
            'description': 'Business description',
            'url': 'http://test.com'
        }

        score = self.processor._calculate_relevance(result)
        self.assertEqual(score, 0.5)  # Default score on error

if __name__ == '__main__':
    unittest.main() 