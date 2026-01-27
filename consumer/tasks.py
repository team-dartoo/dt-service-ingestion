"""
Disclosure Worker Tasks

Ingestion Service에서 발행한 Celery 메시지를 수신하여
Disclosure Service의 PUT API를 호출해 공시 정보를 저장한다.

메시지 형식 (Ingestion Service에서 전송):
{
    "corp_code": "00126380",
    "corp_name": "삼성전자",
    "stock_code": "005930",
    "corp_cls": "Y",
    "report_nm": "사업보고서",
    "rcept_no": "20241125000001",
    "flr_nm": "삼성전자",
    "rcept_dt": "20241125",
    "rm": "유연",
    "object_key": "20241125/20241125000001.html",
    "content_type": "text/html; charset=UTF-8",
    "file_size": 123456,
    "polling_date": "20241125"
}
"""

import os
import logging
import time
from typing import Dict, Any, Optional

import httpx
from worker import app

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Disclosure Service 설정
DISCLOSURE_SERVICE_URL = os.getenv(
    "DISCLOSURE_SERVICE_URL", 
    "http://disclosure-api:8000"
)
WORKER_API_KEY = os.getenv("WORKER_API_KEY", "")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))


class DisclosureServiceClient:
    """
    Disclosure Service와 통신하는 HTTP 클라이언트.
    
    PUT /api/disclosures/{rcept_no} 엔드포인트를 호출하여
    공시 정보를 생성/업데이트한다.
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: int = 30,
        max_retries: int = 3
    ):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.max_retries = max_retries
        
        if not self.api_key:
            logger.warning(
                "WORKER_API_KEY is not set. "
                "Disclosure Service calls will fail authentication."
            )
    
    def _get_headers(self) -> Dict[str, str]:
        """API 요청 헤더 생성"""
        return {
            "Content-Type": "application/json",
            "X-Worker-Api-Key": self.api_key,
        }
    
    def upsert_disclosure(self, rcept_no: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        공시 정보를 생성하거나 업데이트한다.
        
        Args:
            rcept_no: DART 접수번호 (14자리)
            data: 공시 데이터 딕셔너리
            
        Returns:
            API 응답 딕셔너리
            
        Raises:
            httpx.HTTPError: HTTP 요청 실패 시
        """
        url = f"{self.base_url}/api/disclosures/{rcept_no}"
        
        # Celery 메시지를 Disclosure Service API 스키마에 맞게 변환
        payload = {
            "rcept_no": rcept_no,
            "corp_code": data.get("corp_code"),
            "corp_name": data.get("corp_name"),
            "stock_code": data.get("stock_code"),
            "corp_cls": data.get("corp_cls"),
            "report_nm": data.get("report_nm"),
            "flr_nm": data.get("flr_nm"),
            "rcept_dt": data.get("rcept_dt"),
            "rm": data.get("rm"),
            "minio_object_name": data.get("object_key"),
            "content_type": data.get("content_type"),
            "file_size": data.get("file_size"),
            "metadata": {
                "polling_date": data.get("polling_date"),
                "source": "ingestion_service",
            }
        }
        
        # None 값 제거
        payload = {k: v for k, v in payload.items() if v is not None}
        
        last_error = None
        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.put(
                        url,
                        json=payload,
                        headers=self._get_headers()
                    )
                    response.raise_for_status()
                    return response.json()
                    
            except httpx.HTTPStatusError as e:
                last_error = e
                status_code = e.response.status_code
                
                # 4xx 에러는 재시도하지 않음 (클라이언트 오류)
                if 400 <= status_code < 500:
                    logger.error(
                        f"Client error calling Disclosure Service: "
                        f"status={status_code}, response={e.response.text}"
                    )
                    raise
                
                # 5xx 에러는 재시도
                logger.warning(
                    f"Server error (attempt {attempt + 1}/{self.max_retries}): "
                    f"status={status_code}"
                )
                
            except httpx.RequestError as e:
                last_error = e
                logger.warning(
                    f"Request error (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
            
            # 지수 백오프 대기
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * 0.5
                time.sleep(wait_time)
        
        raise last_error


# 싱글톤 클라이언트 인스턴스
disclosure_client = DisclosureServiceClient(
    base_url=DISCLOSURE_SERVICE_URL,
    api_key=WORKER_API_KEY,
    timeout=REQUEST_TIMEOUT,
    max_retries=MAX_RETRIES
)


@app.task(
    name="tasks.process_disclosure",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    autoretry_for=(httpx.HTTPError,),
    retry_backoff=True,
)
def process_disclosure(
    self,
    corp_code: str,
    corp_name: str,
    stock_code: Optional[str],
    corp_cls: str,
    report_nm: str,
    rcept_no: str,
    flr_nm: Optional[str],
    rcept_dt: str,
    rm: Optional[str],
    object_key: str,
    content_type: str,
    file_size: int,
    polling_date: str,
):
    """
    Ingestion Service에서 전달한 공시 메타데이터를 처리하는 Celery Task.
    
    1. Disclosure Service PUT API 호출하여 공시 정보 저장
    2. 처리 결과 로깅
    
    Args:
        corp_code: 기업 고유 코드
        corp_name: 기업명
        stock_code: 종목 코드 (비상장사는 None)
        corp_cls: 법인 구분 (Y/K/N/E)
        report_nm: 보고서명
        rcept_no: DART 접수번호 (14자리)
        flr_nm: 제출인명
        rcept_dt: 접수일자 (YYYYMMDD)
        rm: 비고
        object_key: MinIO 객체 경로
        content_type: 콘텐츠 타입
        file_size: 파일 크기 (bytes)
        polling_date: 폴링 수행 날짜
        
    Returns:
        처리 결과 딕셔너리
    """
    start_ts = time.time()
    task_id = self.request.id
    
    logger.info(
        f"🔄 Processing disclosure | task_id={task_id} | "
        f"rcept_no={rcept_no} | corp={corp_name}({corp_code}) | "
        f"report={report_nm[:30]}..."
    )
    
    try:
        # Disclosure Service API 호출 데이터 구성
        disclosure_data = {
            "corp_code": corp_code,
            "corp_name": corp_name,
            "stock_code": stock_code,
            "corp_cls": corp_cls,
            "report_nm": report_nm,
            "rcept_no": rcept_no,
            "flr_nm": flr_nm,
            "rcept_dt": rcept_dt,
            "rm": rm,
            "object_key": object_key,
            "content_type": content_type,
            "file_size": file_size,
            "polling_date": polling_date,
        }
        
        # Disclosure Service PUT API 호출
        result = disclosure_client.upsert_disclosure(rcept_no, disclosure_data)
        
        elapsed = time.time() - start_ts
        logger.info(
            f"✅ Successfully saved disclosure | task_id={task_id} | "
            f"rcept_no={rcept_no} | corp={corp_name}({corp_code}) | "
            f"elapsed={elapsed:.3f}s"
        )
        
        return {
            "status": "success",
            "task_id": task_id,
            "rcept_no": rcept_no,
            "corp_code": corp_code,
            "corp_name": corp_name,
            "object_key": object_key,
            "elapsed_seconds": elapsed,
            "disclosure_service_response": result,
        }
        
    except httpx.HTTPStatusError as e:
        elapsed = time.time() - start_ts
        error_msg = (
            f"❌ Disclosure Service API error | task_id={task_id} | "
            f"rcept_no={rcept_no} | status={e.response.status_code} | "
            f"response={e.response.text[:200]} | elapsed={elapsed:.3f}s"
        )
        logger.error(error_msg)
        
        # 4xx 에러는 재시도하지 않음
        if 400 <= e.response.status_code < 500:
            return {
                "status": "error",
                "task_id": task_id,
                "rcept_no": rcept_no,
                "error_type": "client_error",
                "error_message": str(e),
                "elapsed_seconds": elapsed,
            }
        
        # 5xx 에러는 재시도
        raise self.retry(exc=e)
        
    except httpx.RequestError as e:
        elapsed = time.time() - start_ts
        error_msg = (
            f"❌ Network error | task_id={task_id} | "
            f"rcept_no={rcept_no} | error={e} | elapsed={elapsed:.3f}s"
        )
        logger.error(error_msg, exc_info=True)
        raise self.retry(exc=e)
        
    except Exception as e:
        elapsed = time.time() - start_ts
        error_msg = (
            f"❌ Unexpected error | task_id={task_id} | "
            f"rcept_no={rcept_no} | error={e} | elapsed={elapsed:.3f}s"
        )
        logger.error(error_msg, exc_info=True)
        
        return {
            "status": "error",
            "task_id": task_id,
            "rcept_no": rcept_no,
            "error_type": "unexpected_error",
            "error_message": str(e),
            "elapsed_seconds": elapsed,
        }


# NOTE: 기존 tasks.summarize_report 태스크는 제거되었습니다.
# 새로운 구현에서는 tasks.process_disclosure를 사용하세요.
