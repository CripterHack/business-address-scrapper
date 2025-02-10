import logging
from typing import List, Dict, Optional, Any, Union, TypeVar, Tuple
import os
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from .exceptions import LLaMAError

try:
    from llama_cpp import Llama
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False

T = TypeVar('T')

@dataclass
class LlamaResponse:
    """Estructura de datos para respuestas del modelo LLaMA."""
    success: bool
    content: str
    error: Optional[str] = None
    score: float = 0.0
    metadata: Dict[str, Any] = None

class LlamaProcessor:
    """Procesador de consultas usando el modelo LLaMA."""
    
    MAX_RETRIES = 3
    MAX_THREADS = 4
    DEFAULT_TIMEOUT = 30
    
    def __init__(self, model_path: Optional[str] = None):
        """Inicializa el procesador LLaMA.
        
        Args:
            model_path: Ruta opcional al modelo LLaMA.
        """
        self.logger = logging.getLogger(__name__)
        self.model = None
        self.is_enabled = os.getenv('ENABLE_AI_FEATURES', 'false').lower() == 'true'
        self._executor = ThreadPoolExecutor(max_workers=self.MAX_THREADS)
        
        if self.is_enabled:
            self.model = self._load_model(model_path)
            if self.model:
                self.logger.info("LlamaProcessor initialized with LLaMA model")
            else:
                self.logger.info("LlamaProcessor initialized in fallback mode")
        else:
            self.logger.info("AI features are disabled, using fallback mode")

    def _safe_execute(self, func: callable, *args, **kwargs) -> Tuple[bool, Any, Optional[str]]:
        """Ejecuta una función de manera segura con manejo de errores.
        
        Args:
            func: Función a ejecutar
            *args: Argumentos posicionales
            **kwargs: Argumentos nombrados
            
        Returns:
            Tupla de (éxito, resultado, mensaje de error)
        """
        try:
            result = func(*args, **kwargs)
            return True, result, None
        except Exception as e:
            self.logger.error(f"Error executing {func.__name__}: {str(e)}", exc_info=True)
            return False, None, str(e)

    def _load_model(self, model_path: Optional[str] = None) -> Optional[Llama]:
        """Carga el modelo LLaMA con manejo mejorado de errores."""
        if not LLAMA_AVAILABLE:
            self.logger.warning("llama-cpp-python not available, using fallback mode")
            return None

        try:
            if not model_path:
                model_path = os.getenv("LLAMA_MODEL_PATH")

            if not model_path or not os.path.exists(model_path):
                raise LLaMAError("Model path not found or invalid")

            n_threads = min(os.cpu_count() or 1, self.MAX_THREADS)
            
            return Llama(
                model_path=model_path,
                n_ctx=2048,
                n_threads=n_threads,
                n_batch=512,
                verbose=False
            )
        except Exception as e:
            raise LLaMAError(f"Error loading model: {str(e)}")

    def enhance_query(self, business_name: str) -> LlamaResponse:
        """Mejora la consulta de búsqueda usando el modelo LLaMA."""
        if not self.is_enabled or not self.model:
            return self._fallback_query_enhancement(business_name)

        try:
            prompt = self._create_search_prompt(business_name)
            success, response, error = self._safe_execute(
                self.model,
                prompt,
                max_tokens=100,
                temperature=0.7,
                top_p=0.95,
                stop=["</query>", "\n"]
            )

            if not success:
                return LlamaResponse(
                    success=False,
                    content="",
                    error=error or "Unknown error in query enhancement"
                )

            enhanced_query = self._parse_llama_response(response["choices"][0]["text"])
            
            return LlamaResponse(
                success=True,
                content=enhanced_query,
                score=response["choices"][0].get("score", 0.0),
                metadata={
                    "original_query": business_name,
                    "model_version": getattr(self.model, "version", "unknown"),
                    "enhancement_type": "llama"
                }
            )
        except Exception as e:
            self.logger.error(f"Error enhancing query: {str(e)}", exc_info=True)
            return self._fallback_query_enhancement(business_name)

    def _create_search_prompt(self, business_name: str) -> str:
        """Crea un prompt estructurado para el modelo LLaMA."""
        return f"""<task>Generate an optimized search query for finding business information.</task>
<business>{business_name}</business>
<requirements>
- Include terms for finding official business records
- Focus on physical location and registration info
- Target New York state businesses
- Include relevant industry keywords
- Consider business type and scale
</requirements>
<format>Return only the search query without any additional text.</format>
<query>"""

    def _parse_llama_response(self, response: str) -> str:
        """Procesa y valida la respuesta del modelo."""
        try:
            clean_response = " ".join(response.strip().split())
            
            if clean_response.startswith('"') and clean_response.endswith('"'):
                clean_response = clean_response[1:-1].strip()
            
            if len(clean_response) < 5:
                raise ValueError("Response too short")
            
            # Validación adicional de la respuesta
            if not any(word in clean_response.lower() for word in ["business", "company", "address", "location"]):
                raise ValueError("Response missing key business terms")
                
            return clean_response
            
        except Exception as e:
            self.logger.error(f"Error parsing response: {str(e)}", exc_info=True)
            raise

    def _fallback_query_enhancement(self, business_name: str) -> LlamaResponse:
        """Método de respaldo con lógica mejorada."""
        keywords = [
            "business address",
            "location",
            "New York",
            "NY",
            "official records",
            "contact information"
        ]
        
        enhanced_query = f"{business_name} {' '.join(keywords)}"
        
        return LlamaResponse(
            success=True,
            content=enhanced_query,
            error="Using fallback mode",
            metadata={
                "original_query": business_name,
                "enhancement_type": "fallback",
                "keywords_used": keywords
            }
        )

    def analyze_search_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze and score search results."""
        if not self.is_enabled or not self.model:
            return self._fallback_analyze_results(results)

        try:
            scored_results = []
            for result in results:
                score = self._calculate_llama_relevance(result)
                scored_results.append({
                    **result,
                    "relevance_score": score.score,
                    "ai_analysis": score.content
                })

            return sorted(scored_results, key=lambda x: x["relevance_score"], reverse=True)
        except Exception as e:
            self.logger.error(f"Error analyzing results with LLaMA: {e}", exc_info=True)
            return self._fallback_analyze_results(results)

    def _calculate_llama_relevance(self, result: Dict[str, Any]) -> LlamaResponse:
        """Calculate relevance score using LLaMA model."""
        try:
            prompt = f"""<task>Analyze this search result and rate its relevance for finding business information.</task>
<result>
Title: {result.get('title', '')}
Description: {result.get('description', '')}
URL: {result.get('url', '')}
</result>
<format>Return only a number between 0 and 1 representing relevance score.</format>
<score>"""

            response = self.model(
                prompt,
                max_tokens=50,
                temperature=0.3,
                stop=["</score>", "\n"]
            )

            score_text = response["choices"][0]["text"].strip()
            try:
                score = float(score_text)
                score = min(max(score, 0), 1)  # Asegurar rango 0-1
                return LlamaResponse(
                    success=True,
                    content=str(score),
                    score=score
                )
            except ValueError:
                return LlamaResponse(
                    success=False,
                    content="0.5",
                    error="Invalid score format",
                    score=0.5
                )
        except Exception as e:
            self.logger.error(f"Error calculating relevance: {e}", exc_info=True)
            return LlamaResponse(
                success=False,
                content="0.5",
                error=str(e),
                score=0.5
            )

    def _fallback_analyze_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Basic analysis of search results when LLaMA is not available."""
        try:
            scored_results = []
            keywords = [
                "address", "location", "business", "new york", "ny", "official",
                "contact", "headquarters", "office", "store"
            ]

            for result in results:
                score = self._calculate_basic_relevance(result, keywords)
                scored_results.append({
                    **result,
                    "relevance_score": score,
                    "ai_analysis": "Fallback scoring used"
                })

            return sorted(scored_results, key=lambda x: x["relevance_score"], reverse=True)
        except Exception as e:
            self.logger.error(f"Error in fallback analysis: {e}", exc_info=True)
            return results

    def _calculate_basic_relevance(self, result: Dict[str, Any], keywords: List[str]) -> float:
        """Calculate basic relevance score based on keyword presence."""
        try:
            text = f"{result.get('title', '')} {result.get('description', '')}".lower()
            url = result.get('url', '').lower()

            # Puntuación base por coincidencias de palabras clave
            keyword_matches = sum(1 for keyword in keywords if keyword in text)
            base_score = min(keyword_matches / len(keywords), 1.0)

            # Bonus por URLs confiables
            trusted_domains = ["ny.gov", "nyc.gov", "bbb.org", "chamberofcommerce.com"]
            domain_bonus = 0.2 if any(domain in url for domain in trusted_domains) else 0

            # Bonus por términos específicos en el título
            title = result.get('title', '').lower()
            title_bonus = 0.1 if any(term in title for term in ["address", "location", "contact"]) else 0

            final_score = min(base_score + domain_bonus + title_bonus, 1.0)
            return round(final_score, 2)

        except Exception as e:
            self.logger.warning(f"Error in basic relevance calculation: {e}")
            return 0.5
