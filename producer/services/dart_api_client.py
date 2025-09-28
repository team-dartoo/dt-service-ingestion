import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict, Any

class DartApiClient:
    _BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            status=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1.2,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def fetch_disclosures(self, date: str, page_no: int = 1, page_count: int = 100) -> Dict[str, Any] | None:
        url = f"{self._BASE_URL}/list.json"
        params = {
            "crtfc_key": self.api_key,
            "bgn_de": date,
            "end_de": date,
            "page_no": page_no,
            "page_count": page_count,
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch disclosures from DART API: %s", e)
            return None
        except ValueError as e:
            logging.error(f"Failed to decode JSON response from disclosures API: %s", e)
            return None

    def fetch_document_content(self, rcept_no: str) -> bytes | None:
        url = f"{self._BASE_URL}/document.xml"
        params = {"crtfc_key": self.api_key, "rcept_no": rcept_no}
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            content_bytes = response.content
    
            if content_bytes.startswith(b'PK\x03\x04'):
                return content_bytes
            
            return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request to fetch document {rcept_no} failed: {e}")
            return None

