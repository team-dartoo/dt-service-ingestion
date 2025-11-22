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

from services.dart_api_client import DartApiClient
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

# ------------------------------ 단일 공시 문서의 다운로드, 정규화, 저장 프로세스를 처리 ------------------------------
def process_document(api: DartApiClient, store: MinIOClient, doc: Disclosure, polling_date: str):
    
    log_header = f"| {doc.rcept_dt} | {doc.rcept_no} | {doc.corp_name:<15} | {doc.report_nm:<50}"

    # 이미 처리했거나 영구 실패로 마킹된 공시는 즉시 스킵
    if doc.rcept_no in PROCESSED_RCEPT_NOS or doc.rcept_no in PERM_FAILED:
        return

    # MinIO에 동일 접수번호 기반 파일이 이미 존재하는 경우 스킵
    # object_exists 는 'prefix*' 형태를 접두사 검색으로 처리하므로,
    # 확장자와 상관없이 '{rcept_dt}/{rcept_no}...' 로 저장된 파일이 하나라도 있으면 중복으로 간주한다.
    if store.object_exists(f"{doc.rcept_dt}/{doc.rcept_no}*"):
        PROCESSED_RCEPT_NOS.add(doc.rcept_no)
        LOG.info(f"SKIPPED   {log_header} | Reason: Already exists in storage.")
        return

    try:  # DART API를 통해 공시 원문(ZIP) 다운로드
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
        
        # 정규화된 파일이 너무 작으면 저가치 데이터로 간주하고 스킵
        if len(normalized_body) < MIN_FILE_SIZE_BYTES:
            reason = f"Processed file too small ({len(normalized_body)} bytes)."
            LOG.warning(f"SKIPPED   {log_header} | Reason: {reason}")
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)
            FAILURE_RECORDER.record(doc, reason)
            return

        # 최종 파일을 MinIO에 업로드
        object_name = f"{doc.rcept_dt}/{final_filename}"
        if store.upload_document(object_name, normalized_body, content_type):
            LOG.info(f"SUCCESS   {log_header} | Saved as: {object_name}")
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)  # 성공 시 처리 목록에 추가

            # 후속 처리를 위한 메타데이터 메시지 구성
            message = {
                "corp_code": doc.corp_code,
                "corp_name": doc.corp_name,
                "report_nm": doc.report_nm,
                "rcept_no": doc.rcept_no,
                "object_key": object_name,
                "rcept_dt": doc.rcept_dt,
                "polling_date": polling_date,
            }

            # Celery Task로 메타데이터 전달 (kwargs 사용)
            try:
                celery_app.send_task(
                    "tasks.summarize_report",
                    kwargs=message,
                )
                LOG.info(
                    f"ENQUEUED {log_header} | "
                    f"object_key={object_name} | corp_code={doc.corp_code} corp_name={doc.corp_name}"
                )
            except Exception as e:
                # 업로드는 성공했지만 큐 발행이 실패한 경우: 파일은 남기고, 장애 원인만 기록
                error_reason = f"Failed to enqueue Celery task: {e}"
                LOG.error(f"FAILED    {log_header} | Error: {error_reason}")
                FAILURE_RECORDER.record(doc, error_reason)

            # 업로드/큐 발행이 정상 완료된 경우, 실패 횟수 기록 정리
            if doc.rcept_no in FAILED_ATTEMPTS:
                del FAILED_ATTEMPTS[doc.rcept_no]

        else:
            raise IOError(f"Failed to upload {object_name} to storage.")
            
    except Exception as e:
        # 예외 처리: 실패 로그 및 재시도 횟수 관리
        error_reason = str(e)
        LOG.error(f"FAILED    {log_header} | Error: {error_reason}")
        FAILURE_RECORDER.record(doc, error_reason)
        
        FAILED_ATTEMPTS[doc.rcept_no] = FAILED_ATTEMPTS.get(doc.rcept_no, 0) + 1
        if FAILED_ATTEMPTS[doc.rcept_no] >= MAX_FAIL:
            PERM_FAILED.add(doc.rcept_no)  # 최대 횟수 초과 시 영구 실패 처리
            PROCESSED_RCEPT_NOS.add(doc.rcept_no)
            LOG.critical(
                f"CRITICAL  | {doc.rcept_dt} | {doc.rcept_no} | "
                f"Permanently failed after {MAX_FAIL} retries."
            )
            del FAILED_ATTEMPTS[doc.rcept_no]

# ------------------------------ 주기적으로 DART API를 호출하여 새로운 공시를 확인하고 처리하는 메인 루프 ------------------------------
def polling_loop(api: DartApiClient, store: MinIOClient, handler: 'SignalHandler', interval: int):

    target_date_str = os.getenv("TARGET_DATE")                                                  # 특정 날짜 고정 여부 확인

    while not handler.is_shutting_down():
        try:                                                                                    # 폴링할 날짜 결정 (고정 날짜 또는 현재 날짜)
            yyyymmdd = target_date_str or datetime.now().strftime('%Y%m%d')
            LOG.info(f"Starting polling for date: {yyyymmdd}...")
            
            all_disclosures = []                                                                # DART API에서 모든 페이지의 공시 목록 가져오기
            page_no = 1
            total_pages = 1
            while page_no <= total_pages:
                if handler.is_shutting_down(): break
                response = api.fetch_disclosures(date=yyyymmdd, page_no=page_no, page_count=100)
                if response and response.get('status') == '000':
                    if page_no == 1: total_pages = response.get('total_page', 1)
                    raw_list = response.get('list', [])
                    if not raw_list and page_no > 1: break
                    for item in raw_list:
                        all_disclosures.append(Disclosure.from_dict(item))
                    page_no += 1
                else:
                    LOG.error(f"API request failed. Status: {response.get('status') if response else 'N/A'}")
                    break

            new_disclosures = [doc for doc in all_disclosures if doc.rcept_no not in PROCESSED_RCEPT_NOS]
            
            if new_disclosures:
                LOG.info(f"Found {len(new_disclosures)} new disclosures for {yyyymmdd}.")
                for doc in new_disclosures:
                    if handler.is_shutting_down(): break
                    process_document(api, store, doc, polling_date=yyyymmdd)
            else:
                LOG.info(f"No new disclosures found for {yyyymmdd}.")

        except Exception as e:
            LOG.error(f"An error occurred in the polling loop: {e}", exc_info=True)
        
        finally:
            LOG.info(f"Polling finished. Waiting for {interval} seconds...")
            handler.wait(timeout=interval)                                                      # 다음 폴링 주기까지 대기

class SignalHandler:
    def __init__(self):                                                                         # Ctrl+C 와 같은 종료 신호를 감지하여 안전하게 종료
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
    try:                                                                                        # 서비스 초기화
        api = DartApiClient(api_key=os.environ["DART_API_KEY"])

        bucket_name = (                                                                         # MinIO 버킷 이름 결정
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
        
        polling_thread = threading.Thread(                                                     # 메인 폴링 루프를 별도 스레드에서 시작
            target=polling_loop, 
            args=(api, store, handler, interval),
            name="Polling"
        )
        polling_thread.start()
        
        LOG.info("DART ingestion service started. Press Ctrl+C to stop.")
        polling_thread.join()                                                                  # 종료 신호를 받을 때까지 대기
        LOG.info("Shutting down...")

    except KeyError as e:
        LOG.error(f"Configuration error: Environment variable not set: {e}")
    except Exception as e:
        LOG.error(f"An unexpected error occurred during startup: {e}", exc_info=True)