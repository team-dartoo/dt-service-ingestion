import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry                # requests 라이브러리의 재시도 기능을 위한 클래스
from typing import Dict, Any

class DartApiClient:
    '''
    DART OPEN API와 통신하는 안정성 강화 클라이언트 클래스
        - 요청 실패 시 자동으로 재시도
        - 모든 요청에 타임아웃을 적용하여 무한 대기 방지
    '''
    _BASE_URL = "https://opendart.fss.or.kr/api"    # DART API의 기본 URL 주소를 정의

    def __init__(self, api_key: str, timeout: int = 30):
        '''
        클라이언트 초기화 메서드
        
        Args
            api_key (str): DART Open API 인증키
            timeout (int): 각 요청에 대한 타임아웃 (초)
        '''
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        
        retry = Retry(                              # 재시도(Retry) 전략 잭체 생성
            total=5,                                        # 전체 재시도 횟수
            read=5,                                         # Read 오류 발생 시 재시도 횟수
            connect=5,                                      # 연결 오류 발생 시 재시도 횟수
            status=5,                                       # 상태 코드 기반 재시도 횟수
            status_forcelist=[429, 500, 502, 503, 504],     # 상태 코드
            backoff_factor=1.2,                             # 재시도 간견 시간
            respect_retry_after_header=True                 # 서버가 'Retry-After' 헤더를 보낼 경우 인정
        )
        adapter = HTTPAdapter(max_retries=retry)    # 생성된 재시도 전략을 HTTP 어댑터에 연결
        self.session.mount("https://", adapter)     # 'https://'로 시작하는 모든 요청에 대해 재시도 어댑터를 사용하도록 세션 설정
        self.session.headers.update({"User-Agent": "dart-ingestion-pipeline/1.0"})

    def fetch_disclosures(self, date: str, page_no: int = 1, page_count: int = 10) -> dict | None:
        '''
        지정된 날짜의 공시 목록을 DART API로부터 호출
        
        Args:
            date (str): 조회할 날짜
            page_no (int): 페이지 번호
            page_count (int): 페이지 당 결과 수
        '''
        url = f"{self._BASE_URL}/list.json"
        params = {
            "crtfc_key": self.api_key,
            "bgn_de": date,
            "end_de": date,
            "page_no": page_no,
            "page_count": min(page_count, 20),      # API 최대치는 100
        }
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)               # 설정한 세션을 사용하여 GET 요청 전송
            response.raise_for_status()                      # 성공 코드가 아닐 경우, 예외 발생 (재시도 로직 동작)
            data = response.json()                           # 응답 본문을 JSON 딕셔너리로 변환
            if data.get("status") != "000":                  # DART API 자체의 에러 코드 확인
                logging.warning("DART API returned an error: status=%s, message=%s", data.get("status"), data.get("message"))
                return None
            return data
        except requests.exceptions.RequestException as e:    # 네트워크 오류, 타임아웃 등 모든 requests 관련 예외처리
            logging.error("Failed to fetch disclosures: %s", e)
            return None
        except ValueError as e:                              # JSON 디코딩 실패: 유효한 JSON 형식이 아닐 경우 예외처리
            logging.error("Failed to decode JSON response from disclosures API: %s", e)
            return None

    def fetch_document_content(self, rcept_no: str) -> bytes | None:
        '''
        접수번호(rcept_no)에 해당하는 공시 원문(ZIP)을 바이너리 데이터로 다운로드
        
        Args:
            rcept_no (str): 다운로드할 공시의 고유 접수번호

        Returns:
            bytes | None: 성공 시 문서의 바이트 데이터, 실패 시 None
        '''
        url = f"{self._BASE_URL}/document.xml"
        params = {"crtfc_key": self.api_key, "rcept_no": rcept_no}
        try:
            response = self.session.get(url, params=params, timeout=self.timeout, stream=True)  # 대용량 파일을 메모리에 한 번에 업로드 X
            response.raise_for_status()                                                         # 점진적으로 다운로드 할 수 있게 하여 메모리 효율성 증가
            
            content_buffer = bytearray()                                            # 스트리밍으로 받은 데이터를 작은 조각(chunk)들로 나누어 안전하게 병합
            for chunk in response.iter_content(chunk_size=65536):                   # 64KB 단위로 읽기
                if chunk:
                    content_buffer.extend(chunk)
            return bytes(content_buffer)                                            # 합쳐진 바이트 배열을 최종적으로 불변(immutable)바이트 객체로 변환하여 반환
        
        except requests.exceptions.RequestException as e:                           # 문서 다운로드 중 발생하는 모든 네트워크 관련 오류 처리
            logging.error(f"Failed to fetch document content for {rcept_no}: {e}")
            return None

