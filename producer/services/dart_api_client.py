import requests
import logging
from typing import List, Optional, Dict, Any
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from models.disclosure import Disclosure

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

class DartApiClient:
    ''' DART Open API와 통신하는 안정성 강화 클라이언트 클래스 '''
    
    _BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self, api_key: str, timeout: int = 10):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        ''' 재시도 로직 포함된 requests.Session 객체 생성 '''
        
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        return session

    def fetch_disclosures(self, date: str, limit: int = 100) -> Optional[List[Disclosure]]:
        ''' 특정 날짜의 공시 리스트 가져오는 메서드 (limit으로 개수 제한 가능) '''
        
        url = f"{self._BASE_URL}/list.json"
        params = {
            'crtfc_key': self.api_key,
            'bgn_de': date,
            'end_de': date,
            'page_no': 1,
            'page_count': min(limit, 100)  # 최대 100개까지만 한 번에 요청 가능
        }
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            if data.get('status') != '000':
                if data.get('status') == '013':
                    logging.info(f"No disclosures found for date: {date}")
                    return []
                logging.error(f"DART API Error: {data.get('status')} - {data.get('message')}")
                return None

            page_disclosures = data.get('list', [])
            if not page_disclosures:
                return []

            # limit만큼만 반환
            return [Disclosure.from_dict(item) for item in page_disclosures[:limit]]

        except (requests.exceptions.RequestException, ValueError) as e:
            logging.error(f"An error occurred during API call: {e}")
            return None

    def fetch_company_profile(self, corp_code: str) -> Optional[Dict[str, Any]]:
        ''' 특정 기업 상세 정보 조회 메서드 '''
        
        url = f"{self._BASE_URL}/company.json"
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()

            if data.get('status') != '000':
                logging.error(f"DART API Error (Company Profile): {data.get('status')} - {data.get('message')}")
                return None
            
            return data
            
        except (requests.exceptions.RequestException, ValueError) as e:
            logging.error(f"An error occurred during company profile API call for {corp_code}: {e}")
            return None
        
    def fetch_document_content(self, rcept_no: str) -> Optional[bytes]:
        ''' 접수번호 기반 공시 원문(ZIP) Binary Data 조회 '''
        
        url = f"{self._BASE_URL}/document.xml"
        params = {
            'crtfc_key': self.api_key,
            'rcept_no': rcept_no,
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            # Binary Data 반환
            return response.content            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch document for rcept_no {rcept_no}: {e}")
            return None