import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DartApiStatus:
    """
    DART Open API 상태 코드 상수 정의.
    
    공식 문서: https://opendart.fss.or.kr/guide/main.do
    """
    SUCCESS = "000"                     # 정상
    INVALID_KEY = "010"                 # 등록되지 않은 키
    DISABLED_KEY = "011"                # 사용할 수 없는 키
    IP_NOT_ALLOWED = "012"              # 접근할 수 없는 IP
    NO_DATA = "013"                     # 조회된 데이터가 없음
    FILE_NOT_FOUND = "014"              # 파일이 존재하지 않음
    RATE_LIMIT_EXCEEDED = "020"         # 요청 제한 초과 (일일 20,000건)
    MAX_COMPANIES_EXCEEDED = "021"      # 조회 가능한 회사 개수 초과 (최대 100건)
    INVALID_FIELD_VALUE = "100"         # 필드의 부적절한 값
    INVALID_ACCESS = "101"              # 부적절한 접근
    SYSTEM_MAINTENANCE = "800"          # 시스템 점검 중
    UNDEFINED_ERROR = "900"             # 정의되지 않은 오류
    KEY_EXPIRED = "901"                 # 개인정보 보유기간 만료 키
    
    # 재시도 불필요한 에러 코드
    NO_RETRY_CODES = {NO_DATA, FILE_NOT_FOUND, INVALID_FIELD_VALUE}
    
    # 재시도 가능한 에러 코드
    RETRYABLE_CODES = {SYSTEM_MAINTENANCE, UNDEFINED_ERROR}
    
    # 심각한 에러 (서비스 중단 필요)
    CRITICAL_CODES = {INVALID_KEY, DISABLED_KEY, KEY_EXPIRED}


class DartApiError(Exception):
    """DART API 관련 예외 클래스"""
    
    def __init__(self, status_code: str, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"DART API Error [{status_code}]: {message}")


class DartApiClient:
    """
    DART Open API와 안정적으로 통신하기 위한 클라이언트 클래스.
    
    기능:
    - 공시 목록 조회 (list.json)
    - 공시 원문 파일 다운로드 (document.xml)
    - 자동 재시도 (exponential backoff)
    - 에러 코드별 처리
    
    Rate Limit:
    - 개인 사용자: 20,000건/일 (자정 KST 리셋)
    - 분당 1,000건 이상 요청 시 서비스 제한
    """
    
    _BASE_URL = "https://opendart.fss.or.kr/api"
    _ZIP_SIGNATURE = b'PK\x03\x04'

    def __init__(self, api_key: str, timeout: int = 30):
        """
        클라이언트 초기화.
        
        Args:
            api_key: DART Open API 인증키 (40자 영숫자)
            timeout: 요청 타임아웃 (초)
        """
        if not api_key or len(api_key) != 40:
            logging.warning(f"API key length is {len(api_key) if api_key else 0}, expected 40")
        
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        
        # 재시도 전략 설정 (HTTP 레벨)
        retry = Retry(
            total=5,
            read=5,
            connect=5,
            status=5,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1.2,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def fetch_disclosures(
        self,
        date: str,
        page_no: int = 1,
        page_count: int = 100,
        corp_code: Optional[str] = None,
        corp_cls: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        특정 날짜의 공시 목록 데이터를 DART API로부터 조회.
        
        Args:
            date: 조회 날짜 (YYYYMMDD)
            page_no: 페이지 번호 (기본: 1)
            page_count: 페이지당 건수 (최대 100)
            corp_code: 기업 고유번호 (선택)
            corp_cls: 법인구분 Y/K/N/E (선택)
            pblntf_ty: 공시유형 A~J (선택)
            
        Returns:
            API 응답 딕셔너리 또는 None (네트워크 오류 시)
            
        Note:
            corp_code 없이 조회 시 최대 3개월 기간 제한 적용
        """
        url = f"{self._BASE_URL}/list.json"
        params = {
            "crtfc_key": self.api_key,
            "bgn_de": date,
            "end_de": date,
            "page_no": str(page_no),
            "page_count": str(min(page_count, 100)),  # 최대 100
        }
        
        # 선택적 파라미터 추가
        if corp_code:
            params["corp_code"] = corp_code
        if corp_cls:
            params["corp_cls"] = corp_cls
        if pblntf_ty:
            params["pblntf_ty"] = pblntf_ty
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch disclosures from DART API: {e}")
            return None
        except ValueError as e:
            logging.error(f"Failed to decode JSON response from disclosures API: {e}")
            return None

    def fetch_document_content(self, rcept_no: str) -> Optional[bytes]:
        """
        접수번호에 해당하는 공시 원문(ZIP)을 다운로드.
        
        Args:
            rcept_no: DART 접수번호 (14자리)
            
        Returns:
            ZIP 파일 바이트 데이터 또는 None
            
        Raises:
            DartApiError: API 레벨 에러 발생 시 (선택적으로 처리 가능)
            
        Note:
            - 성공 시 Content-Type: application/zip
            - 실패 시 XML 형식의 에러 응답 반환
        """
        url = f"{self._BASE_URL}/document.xml"
        params = {"crtfc_key": self.api_key, "rcept_no": rcept_no}
        
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            content_bytes = response.content
    
            # ZIP 파일 시그니처 검증
            if content_bytes.startswith(self._ZIP_SIGNATURE):
                return content_bytes
            
            # ZIP이 아닌 경우 XML 에러 응답 파싱 시도
            error_info = self._parse_xml_error(content_bytes)
            if error_info:
                status_code, message = error_info
                logging.warning(
                    f"DART document API error for rcept_no={rcept_no}: "
                    f"status={status_code}, message={message}"
                )
                
                # 재시도 불필요한 에러는 None 반환
                if status_code in DartApiStatus.NO_RETRY_CODES:
                    return None
                    
                # 심각한 에러는 예외 발생
                if status_code in DartApiStatus.CRITICAL_CODES:
                    raise DartApiError(status_code, message)
            
            return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Request to fetch document {rcept_no} failed: {e}")
            return None
    
    def _parse_xml_error(self, content: bytes) -> Optional[tuple]:
        """
        XML 형식의 에러 응답을 파싱.
        
        DART API 에러 응답 형식:
        <?xml version="1.0" encoding="utf-8"?>
        <r>
            <status>[ERROR_CODE]</status>
            <message>[ERROR_MESSAGE]</message>
        </r>
        
        Returns:
            (status_code, message) 튜플 또는 None
        """
        try:
            # XML 파싱 시도
            root = ET.fromstring(content)
            status_elem = root.find('status')
            message_elem = root.find('message')
            
            if status_elem is not None:
                status_code = status_elem.text or ""
                message = message_elem.text if message_elem is not None else "Unknown error"
                return (status_code, message)
                
        except ET.ParseError:
            # XML이 아닌 경우 무시
            pass
        except Exception as e:
            logging.debug(f"Failed to parse XML error response: {e}")
            
        return None
    
    def check_api_status(self) -> Dict[str, Any]:
        """
        API 키 상태 확인을 위한 테스트 호출.
        
        Returns:
            상태 정보 딕셔너리
        """
        response = self.fetch_disclosures(
            date="19990101",  # 데이터가 없을 날짜
            page_no=1,
            page_count=1
        )
        
        if response is None:
            return {"status": "error", "message": "Network error"}
        
        status_code = response.get("status", "")
        
        return {
            "status": status_code,
            "message": response.get("message", ""),
            "is_valid": status_code in (DartApiStatus.SUCCESS, DartApiStatus.NO_DATA)
        }