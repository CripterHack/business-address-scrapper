import logging
from typing import List, Dict, Optional
import os

try:
    from llama_cpp import Llama
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False


class LlamaProcessor:
    def __init__(self, model_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.model = self._load_model(model_path)
        
        if self.model:
            self.logger.info("LlamaProcessor initialized with LLaMA model")
        else:
            self.logger.info("LlamaProcessor initialized in fallback mode")

    def _load_model(self, model_path: Optional[str] = None) -> Optional[Llama]:
        """Try to load the LLaMA model, return None if not possible"""
        if not LLAMA_AVAILABLE:
            self.logger.warning("llama-cpp-python not available, using fallback mode")
            return None

        try:
            if not model_path:
                model_path = os.getenv('LLAMA_MODEL_PATH')
            
            if not model_path or not os.path.exists(model_path):
                self.logger.warning("LLaMA model path not found, using fallback mode")
                return None
            
            return Llama(
                model_path=model_path,
                n_ctx=2048,
                n_threads=os.cpu_count()
            )
        except Exception as e:
            self.logger.error(f"Error loading LLaMA model: {e}")
            return None

    def enhance_query(self, business_name: str) -> str:
        """Enhance the search query using LLaMA model if available, otherwise fallback"""
        if not self.model:
            return self._fallback_query_enhancement(business_name)

        try:
            prompt = self._create_search_prompt(business_name)
            response = self.model(
                prompt,
                max_tokens=100,
                temperature=0.7,
                top_p=0.95
            )
            
            enhanced_query = self._parse_llama_response(response['choices'][0]['text'])
            self.logger.info(f"Enhanced query with LLaMA: {enhanced_query}")
            return enhanced_query
        except Exception as e:
            self.logger.error(f"Error enhancing query with LLaMA: {e}")
            return self._fallback_query_enhancement(business_name)

    def _create_search_prompt(self, business_name: str) -> str:
        """Create a prompt for the LLaMA model"""
        return f"""
        Given the business name "{business_name}", generate an optimized search query
        to find its address and business information in New York.
        Focus on including terms that would help locate official business records,
        registration information, and physical location.
        Format the response as a search query string.
        """

    def _parse_llama_response(self, response: str) -> str:
        """Parse and clean the LLaMA model's response"""
        # Remove any special characters or formatting
        clean_response = response.strip().replace('\n', ' ')
        
        # Extract the actual query if it's wrapped in quotes or special formatting
        if '"' in clean_response:
            clean_response = clean_response.split('"')[1]
        
        return clean_response

    def _fallback_query_enhancement(self, business_name: str) -> str:
        """Fallback method when LLaMA model is not available"""
        # Add relevant keywords to improve search results
        keywords = "business address location New York NY official records"
        enhanced_query = f"{business_name} {keywords}"
        self.logger.info(f"Using fallback query enhancement: {enhanced_query}")
        return enhanced_query

    def analyze_search_results(self, results: List[Dict]) -> List[Dict]:
        """Analyze search results using LLaMA model if available, otherwise use basic analysis"""
        if not self.model:
            return self._fallback_analyze_results(results)

        try:
            scored_results = []
            for result in results:
                relevance_score = self._calculate_llama_relevance(result)
                scored_results.append({
                    **result,
                    'relevance_score': relevance_score
                })
            
            # Sort by relevance score
            return sorted(scored_results, key=lambda x: x['relevance_score'], reverse=True)
        except Exception as e:
            self.logger.error(f"Error analyzing results with LLaMA: {e}")
            return self._fallback_analyze_results(results)

    def _calculate_llama_relevance(self, result: Dict) -> float:
        """Calculate relevance score using LLaMA model"""
        try:
            prompt = f"""
            Analyze this search result and rate its relevance (0-1) for finding business information:
            Title: {result.get('title', '')}
            Description: {result.get('description', '')}
            URL: {result.get('url', '')}
            """
            
            response = self.model(
                prompt,
                max_tokens=50,
                temperature=0.3
            )
            
            # Extract the numerical score from the response
            score_text = response['choices'][0]['text']
            try:
                score = float(score_text.strip())
                return min(max(score, 0), 1)  # Ensure score is between 0 and 1
            except ValueError:
                return 0.5
        except Exception:
            return 0.5

    def _fallback_analyze_results(self, results: List[Dict]) -> List[Dict]:
        """Basic analysis of search results when LLaMA is not available"""
        try:
            scored_results = []
            keywords = ['address', 'location', 'business', 'new york', 'ny', 'official']
            
            for result in results:
                score = self._calculate_basic_relevance(result, keywords)
                scored_results.append({
                    **result,
                    'relevance_score': score
                })
            
            return sorted(scored_results, key=lambda x: x['relevance_score'], reverse=True)
        except Exception as e:
            self.logger.error(f"Error in fallback analysis: {e}")
            return results

    def _calculate_basic_relevance(self, result: Dict, keywords: List[str]) -> float:
        """Calculate basic relevance score based on keyword presence"""
        try:
            text = f"{result.get('title', '')} {result.get('description', '')}".lower()
            
            # Count keyword matches
            matches = sum(1 for keyword in keywords if keyword in text)
            
            # Calculate score (0-1)
            score = min(matches / len(keywords), 1.0)
            
            return score
        except Exception:
            return 0.5 