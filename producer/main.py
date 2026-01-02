"""
Ingestion Service Producer - 개선 버전

DART Open API에서 공시 문서를 수집하고 MinIO에 저장한 뒤
Celery 메시지를 발행하는 메인 모듈.

[개선 사항]
1. 환경 변수 시작 시 검증 (ConfigValidationError)
2. 헬스체크 HTTP 서버 추가 (/health, /health/ready, /health/live)
3. 전역 상태를 클래스로 캡슐화 (ProcessingState)
4. 구조화된 로깅

[OOP 원칙 적용]
- SRP: 폴링, 처리, 상태 관리 분리
- OCP: 새 처리 단계 추가 용이
- DIP: 설정 객체를 통한 의존성 주입
"""

import os
import time
import logging
import signal
import threading
from datetime import datetime
from typing import Set, Dict, Optional
from dataclasses import dataclass, field, asdict

from dotenv import load_dotenv
from celery import Celery

# 로컬 모듈
from config import get_config, AppConfig, ConfigValidationError
from health import start_health_server, HealthCheckServer
from services.dart_api_client import DartApiClient, DartApiError, DartApiStatus
from services.mock_dart_client import MockDartApiClient, get_dart_client
from services.storage_client import MinIOClient
from services.content_normalizer import normalize_payload
from models.disclosure import Disclosure
from models.failure_recorder import FailureRecorder

# .env 파일 로드
load_dotenv()


# ============================================================
# 로깅 설정
# ============================================================

def setup_logging(log_level: str) -> logging.Logger:
    """로깅 설정"""
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    
    return logging.getLogger(__name__)


# ============================================================
# 상태 관리 클래스 (전역 상태 캡슐화)
# ============================================================

@dataclass
class ProcessingState:
    """
    공시 처리 상태를 관리하는 클래스.
    
    기존 전역 변수(PROCESSED_RCEPT_NOS, FAILED_ATTEMPTS, PERM_FAILED)를
    클래스로 캡슐화하여 테스트 용이성과 상태 관리를 개선한다.
    """
    processed: Set[str] = field(default_factory=set)
    failed_attempts: Dict[str, int] = field(default_factory=dict)
    permanently_failed: Set[str] = field(default_factory=set)
    
    # 통계
    success_count: int = 0
    skip_count: int = 0
    error_count: int = 0
    
    def is_processed(self, rcept_no: str) -> bool:
        """이미 처리된 공시인지 확인"""
        return rcept_no in self.processed or rcept_no in self.permanently_failed
    
    def mark_processed(self, rcept_no: str):
        """처리 완료로 마킹"""
        self.processed.add(rcept_no)
        self.success_count += 1
        
        # 실패 기록 제거
        if rcept_no in self.failed_attempts:
            del self.failed_attempts[rcept_no]
    
    def mark_skipped(self, rcept_no: str):
        """스킵으로 마킹"""
        self.processed.add(rcept_no)
        self.skip_count += 1
    
    def record_failure(self, rcept_no: str, max_fail: int) -> bool:
        """
        실패 기록.
        
        Returns:
            bool: True면 영구 실패로 마킹됨
        """
        self.failed_attempts[rcept_no] = self.failed_attempts.get(rcept_no, 0) + 1
        self.error_count += 1
        
        if self.failed_attempts[rcept_no] >= max_fail:
            self.permanently_failed.add(rcept_no)
            self.processed.add(rcept_no)
            del self.failed_attempts[rcept_no]
            return True
        
        return False
    
    def get_stats(self) -> Dict:
        """통계 반환"""
        return {
            "processed_count": len(self.processed),
            "success_count": self.success_count,
            "skip_count": self.skip_count,
            "error_count": self.error_count,
            "pending_retry_count": len(self.failed_attempts),
            "permanently_failed_count": len(self.permanently_failed),
        }


# ============================================================
# 시그널 핸들러
# ============================================================

class GracefulShutdown:
    """Graceful shutdown 핸들러"""
    
    def __init__(self):
        self._shutdown_event = threading.Event()
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        logging.getLogger(__name__).info(f"Received signal {signum}, initiating shutdown...")
        self._shutdown_event.set()
    
    def is_shutting_down(self) -> bool:
        return self._shutdown_event.is_set()
    
    def wait(self, timeout: float) -> bool:
        """
        지정 시간 대기 또는 종료 신호 수신 시 즉시 반환.
        
        Returns:
            bool: True면 정상 타임아웃, False면 종료 신호 수신
        """
        return not self._shutdown_event.wait(timeout)


# ============================================================
# 문서 처리 함수
# ============================================================

def process_document(
    api: DartApiClient,
    store: MinIOClient,
    doc: Disclosure,
    polling_date: str,
    state: ProcessingState,
    config: AppConfig,
    failure_recorder: FailureRecorder,
    health_server: Optional[HealthCheckServer],
    celery_app: Celery,
    logger: logging.Logger,
):
    """
    단일 공시 문서 처리.
    
    1. 중복 확인
    2. 원문 다운로드
    3. 인코딩 변환
    4. MinIO 업로드
    5. Celery 메시지 발행
    """
    log_header = f"| {doc.rcept_dt} | {doc.rcept_no} | {doc.corp_name:<15} | {doc.report_nm[:50]}"
    
    # 이미 처리된 공시 스킵
    if state.is_processed(doc.rcept_no):
        return
    
    # MinIO에 이미 존재하는지 확인
    if store.object_exists(f"{doc.rcept_dt}/{doc.rcept_no}*"):
        state.mark_skipped(doc.rcept_no)
        logger.info(f"SKIPPED   {log_header} | Reason: Already exists in storage.")
        return
    
    try:
        # 1. DART API에서 원문 다운로드
        zip_bytes = api.fetch_document_content(doc.rcept_no)
        if not zip_bytes:
            raise ValueError("Failed to download document from DART API.")
        
        if health_server:
            health_server.record_dart_success()
        
        # 2. 콘텐츠 정규화 (ZIP 해제, 인코딩 변환)
        context = asdict(doc)
        context["polling_date"] = polling_date
        
        content_type, normalized_body, final_filename = normalize_payload(
            object_key=doc.rcept_no,
            body=zip_bytes,
            log_context=context,
        )
        
        file_size = len(normalized_body)
        
        # 너무 작은 파일은 스킵
        if file_size < 200:
            reason = f"Processed file too small ({file_size} bytes)."
            logger.warning(f"SKIPPED   {log_header} | Reason: {reason}")
            state.mark_skipped(doc.rcept_no)
            failure_recorder.record(doc, reason)
            return
        
        # 3. MinIO 업로드
        object_name = f"{doc.rcept_dt}/{final_filename}"
        if not store.upload_document(object_name, normalized_body, content_type):
            raise IOError(f"Failed to upload {object_name} to storage.")
        
        logger.info(f"SUCCESS   {log_header} | Saved as: {object_name}")
        
        # 4. Celery 메시지 발행
        message = {
            "corp_code": doc.corp_code,
            "corp_name": doc.corp_name,
            "stock_code": doc.stock_code,
            "corp_cls": doc.corp_cls,
            "report_nm": doc.report_nm,
            "rcept_no": doc.rcept_no,
            "flr_nm": doc.flr_nm,
            "rcept_dt": doc.rcept_dt,
            "rm": doc.rm,
            "object_key": object_name,
            "content_type": content_type,
            "file_size": file_size,
            "polling_date": polling_date,
        }
        
        try:
            celery_app.send_task(
                config.celery.task_name,
                kwargs=message,
            )
            logger.info(f"ENQUEUED  {log_header} | object_key={object_name}")
        except Exception as e:
            error_reason = f"Failed to enqueue Celery task: {e}"
            logger.error(f"FAILED    {log_header} | Error: {error_reason}")
            failure_recorder.record(doc, error_reason)
        
        state.mark_processed(doc.rcept_no)
        
        if health_server:
            health_server.record_processed()
        
    except Exception as e:
        error_reason = str(e)
        logger.error(f"FAILED    {log_header} | Error: {error_reason}")
        failure_recorder.record(doc, error_reason)
        
        if health_server:
            health_server.record_error()
            health_server.record_dart_failure()
        
        is_permanent = state.record_failure(doc.rcept_no, config.polling.max_fail)
        
        if is_permanent:
            logger.critical(
                f"CRITICAL  | {doc.rcept_dt} | {doc.rcept_no} | "
                f"Permanently failed after {config.polling.max_fail} retries."
            )


# ============================================================
# 메인 폴링 루프
# ============================================================

def polling_loop(
    api: DartApiClient,
    store: MinIOClient,
    config: AppConfig,
    state: ProcessingState,
    failure_recorder: FailureRecorder,
    shutdown: GracefulShutdown,
    health_server: Optional[HealthCheckServer],
    celery_app: Celery,
    logger: logging.Logger,
):
    """
    DART API 폴링 메인 루프.
    
    주기적으로 공시 목록을 조회하고 새 공시를 처리한다.
    """
    target_date = config.polling.target_date
    interval = config.polling.interval_seconds
    
    # 헬스 상태 업데이트
    if health_server:
        health_server.set_polling_running(True)
    
    while not shutdown.is_shutting_down():
        try:
            # 날짜 결정 (고정 날짜 또는 오늘)
            yyyymmdd = target_date or datetime.now().strftime('%Y%m%d')
            logger.info(f"Starting polling for date: {yyyymmdd}...")
            
            if health_server:
                health_server.record_poll()
            
            all_disclosures = []
            page_no = 1
            total_pages = 1
            
            # 페이지네이션 처리
            while page_no <= total_pages:
                if shutdown.is_shutting_down():
                    break
                
                try:
                    response = api.fetch_disclosures(date=yyyymmdd, page_no=page_no, page_count=100)
                    
                    if response is None:
                        logger.error("API request returned None")
                        break
                    
                    status_code = response.get('status', '')
                    
                    if status_code == DartApiStatus.SUCCESS:
                        if page_no == 1:
                            total_pages = int(response.get('total_page', 1))
                            total_count = int(response.get('total_count', 0))
                            logger.info(f"Total disclosures for {yyyymmdd}: {total_count} (pages: {total_pages})")
                        
                        raw_list = response.get('list', [])
                        if not raw_list and page_no > 1:
                            break
                        
                        for item in raw_list:
                            try:
                                all_disclosures.append(Disclosure.from_dict(item))
                            except TypeError as e:
                                logger.warning(f"Failed to parse disclosure item: {e}")
                        
                        page_no += 1
                        
                    elif status_code == DartApiStatus.NO_DATA:
                        logger.info(f"No disclosures found for {yyyymmdd}")
                        break
                        
                    elif status_code == DartApiStatus.RATE_LIMIT_EXCEEDED:
                        logger.warning("Daily API rate limit exceeded. Waiting 1 hour...")
                        shutdown.wait(3600)
                        break
                        
                    elif status_code == DartApiStatus.SYSTEM_MAINTENANCE:
                        logger.warning("DART system under maintenance. Waiting 5 minutes...")
                        shutdown.wait(300)
                        break
                        
                    elif status_code in (DartApiStatus.INVALID_KEY, DartApiStatus.DISABLED_KEY):
                        logger.critical(f"API key error (status={status_code}). Check DART_API_KEY.")
                        shutdown.wait(interval)
                        break
                        
                    else:
                        message = response.get('message', 'Unknown error')
                        logger.error(f"DART API error: status={status_code}, message={message}")
                        break
                        
                except DartApiError as e:
                    logger.error(f"DART API error during pagination: {e}")
                    break
            
            # 새 공시 처리
            new_disclosures = [
                doc for doc in all_disclosures
                if not state.is_processed(doc.rcept_no)
            ]
            
            if new_disclosures:
                logger.info(f"Found {len(new_disclosures)} new disclosures for {yyyymmdd}.")
                
                for doc in new_disclosures:
                    if shutdown.is_shutting_down():
                        break
                    
                    process_document(
                        api=api,
                        store=store,
                        doc=doc,
                        polling_date=yyyymmdd,
                        state=state,
                        config=config,
                        failure_recorder=failure_recorder,
                        health_server=health_server,
                        celery_app=celery_app,
                        logger=logger,
                    )
            else:
                logger.info(f"No new disclosures found for {yyyymmdd}.")
            
            # 통계 로깅
            stats = state.get_stats()
            logger.info(
                f"Stats: processed={stats['processed_count']}, "
                f"success={stats['success_count']}, "
                f"skipped={stats['skip_count']}, "
                f"errors={stats['error_count']}, "
                f"pending_retry={stats['pending_retry_count']}"
            )
            
        except Exception as e:
            logger.error(f"Error in polling loop: {e}", exc_info=True)
        
        finally:
            logger.info(f"Polling finished. Waiting for {interval} seconds...")
            if not shutdown.wait(interval):
                break
    
    # 종료 시 헬스 상태 업데이트
    if health_server:
        health_server.set_polling_running(False)


# ============================================================
# 메인 엔트리포인트
# ============================================================

def main():
    """
    메인 함수.
    
    1. 환경 변수 검증
    2. 헬스체크 서버 시작
    3. 클라이언트 초기화
    4. 폴링 루프 시작
    """
    # 1. 설정 로드 및 검증 (실패 시 프로세스 종료)
    config = get_config()
    
    # 2. 로깅 설정
    logger = setup_logging(config.log_level)
    logger.info("="*60)
    logger.info("DART Ingestion Service - Producer Starting")
    logger.info("="*60)
    logger.info(f"Configuration: {config.to_dict()}")
    
    # 3. 헬스체크 서버 시작
    health_server = None
    if config.health.enabled:
        try:
            health_server = start_health_server(
                host=config.health.host,
                port=config.health.port,
            )
            logger.info(f"Health check server started on port {config.health.port}")
        except Exception as e:
            logger.warning(f"Failed to start health check server: {e}")
    
    # 4. Celery 앱 설정
    celery_app = Celery('producer', broker=config.celery.broker_url)
    celery_app.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='Asia/Seoul',
        enable_utc=False,
    )
    
    # RabbitMQ 연결 상태 (Celery 연결 시도로 확인)
    if health_server:
        try:
            # 연결 테스트
            conn = celery_app.connection()
            conn.ensure_connection(max_retries=1)
            conn.release()
            health_server.set_rabbitmq_connected(True)
            logger.info("RabbitMQ connection verified")
        except Exception as e:
            logger.warning(f"RabbitMQ connection check failed: {e}")
            health_server.set_rabbitmq_connected(False)
    
    # 5. 클라이언트 초기화
    try:
        # Mock 모드 또는 실제 DART API 클라이언트 선택
        api = get_dart_client(config)
        
        if config.dart.mock_mode:
            logger.info("🧪 MOCK DART API client initialized")
        else:
            logger.info("DART API client initialized")
        
        if health_server:
            health_server.set_dart_client(api)
        
    except Exception as e:
        logger.critical(f"Failed to initialize DART API client: {e}")
        return 1
    
    try:
        store = MinIOClient(
            endpoint=config.minio.endpoint,
            access_key=config.minio.access_key,
            secret_key=config.minio.secret_key,
            bucket_name=config.minio.bucket_name,
            secure=config.minio.secure,
        )
        logger.info(f"MinIO client initialized (bucket: {config.minio.bucket_name})")
        
        if health_server:
            health_server.set_minio_client(store)
        
    except Exception as e:
        logger.critical(f"Failed to initialize MinIO client: {e}")
        return 1
    
    # 6. 상태 및 실패 기록 초기화
    state = ProcessingState()
    failure_recorder = FailureRecorder(log_dir=config.polling.failed_log_dir)
    
    # 7. 종료 핸들러
    shutdown = GracefulShutdown()
    
    # 8. 폴링 루프 시작 (별도 스레드)
    logger.info("Starting polling loop...")
    
    polling_thread = threading.Thread(
        target=polling_loop,
        args=(
            api,
            store,
            config,
            state,
            failure_recorder,
            shutdown,
            health_server,
            celery_app,
            logger,
        ),
        name="PollingLoop",
    )
    polling_thread.start()
    
    logger.info("DART Ingestion Service started. Press Ctrl+C to stop.")
    
    # 메인 스레드는 폴링 스레드 종료 대기
    try:
        polling_thread.join()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    
    # 9. 정리
    logger.info("Shutting down...")
    
    if health_server:
        health_server.stop()
    
    # 최종 통계
    stats = state.get_stats()
    logger.info(f"Final stats: {stats}")
    
    logger.info("DART Ingestion Service stopped.")
    return 0


if __name__ == "__main__":
    exit(main())
