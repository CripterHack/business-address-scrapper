"""Address extraction module with multiple strategies."""

import re
import logging
from typing import Dict, Optional, List
from bs4 import BeautifulSoup
import requests
from urllib.parse import urljoin
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AddressData:
    """Estructura de datos para almacenar información de dirección."""

    raw_address: str
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    confidence_score: float = 0.0
    source: str = ""


class AddressExtractor:
    """Extractor de direcciones con múltiples estrategias."""

    def __init__(self):
        self.patterns = {
            "address_with_zip": r"(\d+[^,]+(?:\s+[^,]+)*?)(?:,|\sin\s)([^,]+),\s*([A-Za-z]{2})\s*(\d{5}(?:-\d{4})?)",
            "address_with_state": r"(\d+[^,]+(?:\s+[^,]+)*?)(?:,|\sin\s)([^,]+),\s*([A-Za-z]{2})",
            "address_basic": r"(\d+[^,]+(?:\s+[^,]+)*?)(?:,|\sin\s)([^,]+)",
            "address_with_suite": r"(\d+[^,]+(?:\s+[^,]+)*?(?:\s+(?:Suite|Ste|Apt|Unit|#)\s*[A-Za-z0-9-]+)?)(?:,|\sin\s)([^,]+),\s*([A-Za-z]{2})\s*(\d{5}(?:-\d{4})?)",
            "address_with_keywords": r"(?:located at|address[:\s]+|business\s+address[:\s]+)(\d+[^,]+(?:\s+[^,]+)*?)(?:,|\sin\s)([^,]+)(?:,\s*([A-Za-z]{2})\s*(\d{5}(?:-\d{4})?))?",
        }

        self.strategies = [
            self._extract_from_structured_data,
            self._extract_from_meta_tags,
            self._extract_from_contact_page,
            self._extract_from_text_content,
        ]

    def extract_address(
        self, url: str, business_name: str, max_retries: int = 3
    ) -> Optional[AddressData]:
        """Intenta extraer la dirección usando múltiples estrategias."""
        for attempt in range(max_retries):
            for strategy in self.strategies:
                try:
                    logger.info(
                        f"Intentando estrategia {strategy.__name__} para {business_name} (intento {attempt + 1})"
                    )
                    result = strategy(url, business_name)
                    if result and result.confidence_score > 0.7:  # Umbral de confianza
                        return result
                    time.sleep(1)  # Pausa entre intentos
                except Exception as e:
                    logger.error(f"Error en {strategy.__name__}: {str(e)}")
                    continue

            if attempt < max_retries - 1:
                time.sleep(2)  # Pausa entre ciclos de intentos

        return None

    def _extract_from_structured_data(self, url: str, business_name: str) -> Optional[AddressData]:
        """Extrae dirección de datos estructurados (Schema.org, JSON-LD)."""
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            # Buscar JSON-LD
            scripts = soup.find_all("script", type="application/ld+json")
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        address = data.get("address") or data.get("location", {}).get("address")
                        if address:
                            return AddressData(
                                raw_address=f"{address.get('streetAddress')}, {address.get('addressLocality')}, {address.get('addressRegion')} {address.get('postalCode')}",
                                street=address.get("streetAddress"),
                                city=address.get("addressLocality"),
                                state=address.get("addressRegion"),
                                zip_code=address.get("postalCode"),
                                confidence_score=0.9,
                                source="structured_data",
                            )
                except:
                    continue

            return None
        except Exception as e:
            logger.error(f"Error extrayendo datos estructurados: {str(e)}")
            return None

    def _extract_from_meta_tags(self, url: str, business_name: str) -> Optional[AddressData]:
        """Extrae dirección de meta tags."""
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            meta_tags = {
                "place:location:address": soup.find("meta", {"property": "place:location:address"}),
                "og:street-address": soup.find("meta", {"property": "og:street-address"}),
                "business:contact_data:street_address": soup.find(
                    "meta", {"property": "business:contact_data:street_address"}
                ),
            }

            for tag_name, tag in meta_tags.items():
                if tag and tag.get("content"):
                    address = self._parse_address_text(tag.get("content"))
                    if address:
                        return AddressData(
                            raw_address=tag.get("content"),
                            street=address.get("street"),
                            city=address.get("city"),
                            state=address.get("state"),
                            zip_code=address.get("zip_code"),
                            confidence_score=0.85,
                            source="meta_tags",
                        )

            return None
        except Exception as e:
            logger.error(f"Error extrayendo meta tags: {str(e)}")
            return None

    def _extract_from_contact_page(self, url: str, business_name: str) -> Optional[AddressData]:
        """Busca y extrae dirección de la página de contacto."""
        try:
            # Primero intentar encontrar el enlace de contacto
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            contact_links = soup.find_all(
                "a", href=re.compile(r"contact|location|about|find-us", re.I)
            )

            for link in contact_links:
                contact_url = urljoin(url, link["href"])
                try:
                    contact_response = requests.get(contact_url, timeout=10)
                    contact_soup = BeautifulSoup(contact_response.text, "html.parser")

                    # Buscar en elementos comunes de dirección
                    address_elements = contact_soup.find_all(
                        ["address", "div", "p"],
                        class_=re.compile(r"address|location|contact-info", re.I),
                    )

                    for element in address_elements:
                        text = element.get_text(strip=True)
                        address = self._parse_address_text(text)
                        if address:
                            return AddressData(
                                raw_address=text,
                                street=address.get("street"),
                                city=address.get("city"),
                                state=address.get("state"),
                                zip_code=address.get("zip_code"),
                                confidence_score=0.8,
                                source="contact_page",
                            )
                except:
                    continue

            return None
        except Exception as e:
            logger.error(f"Error extrayendo de página de contacto: {str(e)}")
            return None

    def _extract_from_text_content(self, url: str, business_name: str) -> Optional[AddressData]:
        """Extrae dirección del contenido general de la página."""
        try:
            response = requests.get(url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")

            # Eliminar scripts y estilos
            for script in soup(["script", "style"]):
                script.decompose()

            text = soup.get_text()

            # Buscar párrafos cercanos al nombre del negocio
            paragraphs = text.split("\n")
            for i, paragraph in enumerate(paragraphs):
                if business_name.lower() in paragraph.lower():
                    # Buscar en párrafos cercanos
                    context = " ".join(paragraphs[max(0, i - 2) : min(len(paragraphs), i + 3)])
                    address = self._parse_address_text(context)
                    if address:
                        return AddressData(
                            raw_address=context,
                            street=address.get("street"),
                            city=address.get("city"),
                            state=address.get("state"),
                            zip_code=address.get("zip_code"),
                            confidence_score=0.75,
                            source="text_content",
                        )

            return None
        except Exception as e:
            logger.error(f"Error extrayendo del contenido de texto: {str(e)}")
            return None

    def _parse_address_text(self, text: str) -> Optional[Dict[str, str]]:
        """Parsea texto para encontrar componentes de dirección."""
        for pattern_name, pattern in self.patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                address_data = {
                    "street": groups[0].strip(),
                    "city": groups[1].strip() if len(groups) > 1 else None,
                    "state": groups[2].strip() if len(groups) > 2 else None,
                    "zip_code": groups[3].strip() if len(groups) > 3 else None,
                }
                return address_data

        return None
