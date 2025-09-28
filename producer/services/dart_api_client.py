import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry                        # requests 라이브러리의 재시도 기능을 위한 클래스
from typing import Dict, Any

# -------------------- DART OPEN API와 안정적으로 통신하기 위한 클라이언트 클래스 --------------------
class DartApiClient:
    
    _BASE_URL = "https://opendart.fss.or.kr/api"            # DART API의 기본 URL

    def __init__(self, api_key: str, timeout: int = 30):    # 클라이언트 초기화
        self.api_key = api_key                              # API 인증키
        self.timeout = timeout                              # 각 요청에 대한 타임아웃 (초)
        self.session = requests.Session()                   # HTTP 연결을 재사용하여 성능을 향상시키는 세션 객체
        
        retry = Retry(                                      # 재시도(Retry) 전략 객체 생성
            total=5,                                        # 전체 재시도 횟수
            read=5,                                         # Read 오류 발생 시 재시도 횟수
            connect=5,                                      # 연결 오류 발생 시 재시도 횟수
            status=5,                                       # 특정 상태 코드 기반 재시도 횟수
            status_forcelist=[429, 500, 502, 503, 504],     # 재시도를 수행할 HTTP 상태 코드 목록
            backoff_factor=1.2,                             # 재시도 간격 시간 증가 계수 ( exponential backoff )
        )
        adapter = HTTPAdapter(max_retries=retry)            # 재시도 전략을 세션에 적용하기 위한 어댑터
        self.session.mount("http://", adapter)              # http와 https 요청에 재시도 전략 마운트
        self.session.mount("https://", adapter)

    def fetch_disclosures(self, date: str, page_no: int = 1, page_count: int = 100) -> Dict[str, Any] | None:
        url = f"{self._BASE_URL}/list.json"                 # 특정 날짜의 공시 목록 데이터를 DART API로부터 가져옴
        params = {
            "crtfc_key": self.api_key,
            "bgn_de": date,
            "end_de": date,
            "page_no": page_no,
            "page_count": page_count,
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()                     # 200번대 상태 코드가 아닐 경우 예외 발생
            return response.json()                          # 성공 시 JSON 응답을 딕셔너리로 변환하여 반환
        except requests.exceptions.RequestException as e:   # 네트워크 관련 오류 처리
            logging.error(f"Failed to fetch disclosures from DART API: %s", e)
            return None
        except ValueError as e:                             # JSON 디코딩 실패 처리
            logging.error(f"Failed to decode JSON response from disclosures API: %s", e)
            return None

    def fetch_document_content(self, rcept_no: str) -> bytes | None:
        url = f"{self._BASE_URL}/document.xml"              # 접수번호에 해당하는 공시 원문(ZIP)을 다운로드
        params = {"crtfc_key": self.api_key, "rcept_no": rcept_no}
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            content_bytes = response.content
    
            if content_bytes.startswith(b'PK\x03\x04'):     # 응답이 유효한 ZIP 파일인지 시그니처로 엄격하게 검증
                return content_bytes                        # ZIP 파일이 맞으면 바이트 데이터 반환
            
            return None                                     # ZIP 파일이 아닐 경우, DART의 비정상 응답으로 간주하고 None 반환

        except requests.exceptions.RequestException as e:
            logging.error(f"Request to fetch document {rcept_no} failed: {e}")
            return None
