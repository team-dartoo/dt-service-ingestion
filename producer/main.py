import os
import time
import logging
import signal
import threading
from datetime import datetime
from typing import List, Dict, Any, Set
from dataclasses import asdict

from dotenv import load_dotenv
from celery import Celery

from services.dart_api_client import DartApiClient, DartApiError, DartApiStatus
from services.storage_client import MinIOClient
from services.content_normalizer import normalize_payload
from models.disclosure import Disclosure
from models.failure_recorder import FailureRecorder

# .env 파일에서 환경 변수 로드
load_dotenv() 

# 로깅 기본 설정
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_name, logging.INFO)

logging.basicConfig(
    level=numeric_level,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

LOG = logging.getLogger(__name__)

# Celery Producer 설정: 후속 작업을 위해 RabbitMQ에 메시지 전달
celery_app = Celery('producer', broker=os.getenv("CELERY_BROKER_URL", "amqp://admin:password@localhost:5672/"))
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=False,
)

PROCESSED_RCEPT_NOS: Set[str] = set()       # 이미 처리한 공시 접수번호 (중복 방지)
FAILED_ATTEMPTS: Dict[str, int] = {}        # 공시별 처리 실패 횟수 (재시도 관리)
PERM_FAILED: set[str] = set()               # 영구 실패 처리된 공시 접수번호

MAX_FAIL = int(os.getenv("MAX_FAIL", "3"))                  # 최대 재시도 횟수
MIN_FILE_SIZE_BYTES = 200                                   # 처리할 파일의 최소 크기 (이보다 작으면 스킵)
FAILED_LOG_DIR = os.getenv("FAILED_LOG_DIR")                # 실패 로그 저장 디렉토리
FAILURE_RECORDER = FailureRecorder(log_dir=FAILED_LOG_DIR)  # 실패 기록 객체 초기화


def process_document(api: DartApiClient, store: MinIOClient, doc: Disclosure, polling_date: str):
    """
    단일 공시 문서의 다운로드, 정규화, 저장 프로세스를 처리.
    
    Args:
        api: DART API 클라이언트
        store: MinIO 스토리지 클라이언트
        doc: 처리할 공시 정보
        polling_date: 폴링 수행 날짜 (YYYYMMDD)
    """
    log_header = f"| {doc.rcept_dt} | {doc.rcept_no} | {doc.corp_name:<15} | {doc.report_nm:<50}"

    # 이미 처리했거나 영구 실패로 마킹된 공시는 즉시 스킵
    if doc.rcept_no in PROCESSED_RCEPT_NOS or doc.rcept_no in PERM_FAILED:
        return

    # MinIO에 동일 접수번호 기반 파일이 이미 존재하는 경우 스킵
    if store.object_exists(f"{doc.rcept_dt}/{doc.rcept_no}*"):
        PROCESSED_RCEPT_NOS.add(doc.rcept_no)
        LOG.info(f"SKIPPED   {log_header} | Reason: Already exists in storage.")
        return

    try:
        # DART API를 통해 공시 원문(ZIP) 다운로드
        zip_bytes = api.fetch_document_content(doc.rcept_no)
        if not zip_bytes:
            raise ValueError("Failed to download a valid document from DART API.")

        # 다운로드한 파일을 정규화 (콘텐츠 분석, 인코딩 변환 등)
        context = asdict(doc)
        context["polling_date"] = polling_date
        content_type, normalized_body, final_filename = normalize_payload(
            object_key=doc.rcept_no,
            body=zip_bytes,
            log_context=context,
        )
        
        # 파일 크기 계산
        file_size = len(normalized_body)
        
        # 정규화된 파일이 너무 작으면 저가치 데이터로 간주하고 스킵
        if file_size < MIN_FILE_SIZE_BYTES:
            reason = f"Processed file too small ({file_size} bytes)."
            LOG.warning(f"SKIPPED   {log_header} | Reason: {reason}")
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)
            FAILURE_RECORDER.record(doc, reason)
            return

        # 최종 파일을 MinIO에 업로드
        object_name = f"{doc.rcept_dt}/{final_filename}"
        if store.upload_document(object_name, normalized_body, content_type):
            LOG.info(f"SUCCESS   {log_header} | Saved as: {object_name}")
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)

            # 후속 처리를 위한 메타데이터 메시지 구성 (전체 DART 필드 + 저장 정보)
            message = {
                # DART API 기본 필드
                "corp_code": doc.corp_code,
                "corp_name": doc.corp_name,
                "stock_code": doc.stock_code,       # 추가: 종목 코드
                "corp_cls": doc.corp_cls,           # 추가: 법인 구분
                "report_nm": doc.report_nm,
                "rcept_no": doc.rcept_no,
                "flr_nm": doc.flr_nm,               # 추가: 제출인명
                "rcept_dt": doc.rcept_dt,
                "rm": doc.rm,                       # 추가: 비고
                # 저장 정보
                "object_key": object_name,
                "content_type": content_type,       # 추가: 콘텐츠 타입
                "file_size": file_size,             # 추가: 파일 크기
                # 메타데이터
                "polling_date": polling_date,
            }

            # Celery Task로 메타데이터 전달 (kwargs 사용)
            try:
                celery_app.send_task(
                    "tasks.process_disclosure",
                    kwargs=message,
                )
                LOG.info(
                    f"ENQUEUED {log_header} | "
                    f"object_key={object_name} | corp_code={doc.corp_code} corp_name={doc.corp_name}"
                )
            except Exception as e:
                error_reason = f"Failed to enqueue Celery task: {e}"
                LOG.error(f"FAILED    {log_header} | Error: {error_reason}")
                FAILURE_RECORDER.record(doc, error_reason)

            if doc.rcept_no in FAILED_ATTEMPTS:
                del FAILED_ATTEMPTS[doc.rcept_no]

        else:
            raise IOError(f"Failed to upload {object_name} to storage.")
            
    except Exception as e:
        error_reason = str(e)
        LOG.error(f"FAILED    {log_header} | Error: {error_reason}")
        FAILURE_RECORDER.record(doc, error_reason)
        
        FAILED_ATTEMPTS[doc.rcept_no] = FAILED_ATTEMPTS.get(doc.rcept_no, 0) + 1
        if FAILED_ATTEMPTS[doc.rcept_no] >= MAX_FAIL:
            PERM_FAILED.add(doc.rcept_no)
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)
            LOG.critical(
                f"CRITICAL  | {doc.rcept_dt} | {doc.rcept_no} | "
                f"Permanently failed after {MAX_FAIL} retries."
            )
            del FAILED_ATTEMPTS[doc.rcept_no]


def polling_loop(api: DartApiClient, store: MinIOClient, handler: 'SignalHandler', interval: int):
    """
    주기적으로 DART API를 호출하여 새로운 공시를 확인하고 처리하는 메인 루프.
    
    DART API 에러 코드에 따른 처리:
    - 000: 정상 처리
    - 013: 조회된 데이터 없음 → 정상 종료
    - 020: 일일 요청 한도 초과 → 익일까지 대기
    - 800: 시스템 점검 → 5분 후 재시도
    - 기타: 로깅 후 다음 폴링 대기
    """
    target_date_str = os.getenv("TARGET_DATE")

    while not handler.is_shutting_down():
        try:
            yyyymmdd = target_date_str or datetime.now().strftime('%Y%m%d')
            LOG.info(f"Starting polling for date: {yyyymmdd}...")
            
            all_disclosures = []
            page_no = 1
            total_pages = 1
            
            while page_no <= total_pages:
                if handler.is_shutting_down():
                    break
                    
                try:
                    response = api.fetch_disclosures(date=yyyymmdd, page_no=page_no, page_count=100)
                    
                    if response is None:
                        LOG.error("API request returned None")
                        break
                    
                    status_code = response.get('status', '')
                    
                    # DART API 상태 코드 처리
                    if status_code == DartApiStatus.SUCCESS:
                        # 정상 처리
                        if page_no == 1:
                            total_pages = int(response.get('total_page', 1))
                            total_count = int(response.get('total_count', 0))
                            LOG.info(f"Total disclosures for {yyyymmdd}: {total_count} (pages: {total_pages})")
                        
                        raw_list = response.get('list', [])
                        if not raw_list and page_no > 1:
                            break
                            
                        for item in raw_list:
                            try:
                                all_disclosures.append(Disclosure.from_dict(item))
                            except TypeError as e:
                                LOG.warning(f"Failed to parse disclosure item: {e}")
                                continue
                        page_no += 1
                        
                    elif status_code == DartApiStatus.NO_DATA:
                        # 조회된 데이터 없음 - 정상 종료
                        LOG.info(f"No disclosures found for {yyyymmdd}")
                        break
                        
                    elif status_code == DartApiStatus.RATE_LIMIT_EXCEEDED:
                        # 일일 요청 한도 초과 (20,000건/일)
                        LOG.warning("Daily API rate limit exceeded. Waiting until next day...")
                        # 자정까지 대기 로직 (간소화: 1시간 대기 후 재확인)
                        handler.wait(timeout=3600)
                        break
                        
                    elif status_code == DartApiStatus.SYSTEM_MAINTENANCE:
                        # 시스템 점검 중
                        LOG.warning("DART system under maintenance. Waiting 5 minutes...")
                        handler.wait(timeout=300)
                        break
                        
                    elif status_code in (DartApiStatus.INVALID_KEY, DartApiStatus.DISABLED_KEY):
                        # API 키 문제 - 심각한 오류
                        LOG.critical(f"API key error (status={status_code}). Please check DART_API_KEY.")
                        handler.wait(timeout=interval)
                        break
                        
                    else:
                        # 기타 오류
                        message = response.get('message', 'Unknown error')
                        LOG.error(f"DART API error: status={status_code}, message={message}")
                        break
                        
                except DartApiError as e:
                    LOG.error(f"DART API error during pagination: {e}")
                    break

            # 새로운 공시 처리
            new_disclosures = [doc for doc in all_disclosures if doc.rcept_no not in PROCESSED_RCEPT_NOS]
            
            if new_disclosures:
                LOG.info(f"Found {len(new_disclosures)} new disclosures for {yyyymmdd}.")
                for doc in new_disclosures:
                    if handler.is_shutting_down():
                        break
                    process_document(api, store, doc, polling_date=yyyymmdd)
            else:
                LOG.info(f"No new disclosures found for {yyyymmdd}.")

        except Exception as e:
            LOG.error(f"An error occurred in the polling loop: {e}", exc_info=True)
        
        finally:
            LOG.info(f"Polling finished. Waiting for {interval} seconds...")
            handler.wait(timeout=interval)


class SignalHandler:
    """Ctrl+C와 같은 종료 신호를 감지하여 안전하게 종료하는 핸들러"""
    
    def __init__(self):
        self._shutdown = threading.Event()
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum, frame):
        self._shutdown.set()

    def is_shutting_down(self):
        return self._shutdown.is_set()
    
    def wait(self, timeout):
        self._shutdown.wait(timeout)


if __name__ == "__main__":
    try:
        api = DartApiClient(api_key=os.environ["DART_API_KEY"])

        bucket_name = (
            os.getenv("MINIO_BUCKET")
            or os.getenv("MINIO_BUCKET_NAME")
            or "dart-disclosures"
        )

        store = MinIOClient(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            bucket_name=bucket_name,
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
        handler = SignalHandler()

        poll_interval_str = os.getenv("POLL_INTERVAL", "300")
        try:
            interval = int(poll_interval_str)
        except ValueError:
            LOG.warning(
                f"Invalid POLL_INTERVAL '{poll_interval_str}', falling back to 300 seconds."
            )
            interval = 300
        
        polling_thread = threading.Thread(
            target=polling_loop, 
            args=(api, store, handler, interval),
            name="Polling"
        )
        polling_thread.start()
        
        LOG.info("DART ingestion service started. Press Ctrl+C to stop.")
        polling_thread.join()
        LOG.info("Shutting down...")

    except KeyError as e:
        LOG.error(f"Configuration error: Environment variable not set: {e}")
    except Exception as e:
        LOG.error(f"An unexpected error occurred during startup: {e}", exc_info=True)