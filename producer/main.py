import os
import time
import logging
import signal
import threading
from datetime import datetime
from typing import List, Dict, Any

from dotenv import load_dotenv
from celery import Celery

from services.dart_api_client import DartApiClient
from services.storage_client import MinioClient
from services.content_normalizer import normalize_payload
from models.disclosure import Disclosure

# .env 파일에서 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s', encoding='utf-8')
LOG = logging.getLogger(__name__)

# Celery Producer 설정: 수집된 공시 정보를 처리할 워커에게 작업 전달
# 브로커 URL은 환경 변수에서 가져오며, 기본값은 로컬 RabbitMQ로 지정
celery_app = Celery('producer', broker=os.getenv("CELERY_BROKER_URL", "amqp://admin:password@localhost:5672/"))
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=False,
)

FAILED_ATTEMPTS: Dict[str, int] = {}                    # 특정 공시 처리 실패 횟수를 기록하는 딕셔너리
PERM_FAILED: set[str] = set()                           # 최대 실패 횟수를 초과하여 영구적으로 실패 처리된 공시 집합
MAX_FAIL = int(os.getenv("MAX_FAIL", "3"))              # 최대 재시도 횟수(3)

manual_refresh_event = threading.Event()                # 스레드 간 통신을 위한 이벤트 객체(수동 갱신 및 종료에 사용)

# 시스템 신호 처리
class SignalHandler:
    '''
    운영체제의 종료 시그널 (CTRL + C 등)을 감지하여 프로그램을 안전하게 종료시키는 클래스
    '''
    def __init__(self):
        self.shutdown_requested = False                         # 종료 요청 플래그
        signal.signal(signal.SIGINT, self._request_shutdown)    # SIGINT: CTRL + C
        signal.signal(signal.SIGTERM, self._request_shutdown)   # SIGTERM: 프로세스 종료

    def _request_shutdown(self, signum, frame):
        LOG.info("Shutdown signal received. Stopping...")
        self.shutdown_requested = True
        manual_refresh_event.set()

# 파일 처리 로직
def process_document(api: DartApiClient, store: MinioClient, d: Disclosure) -> bool:
    '''
    하나의 공시 정보를 받아 문서를 다운로드, 정규화, 저장하는 전체 과정을 처리
    MinIO를 통해 중복 확인
    
    Args:
        api (DartApiClient): DART API 통신 클라이언트
        store (MinioClient): MinIO 스토리지 클라이언트
        d (Disclosure): 처리할 공시 정보 객체
    '''
    if d.rcept_no in PERM_FAILED:                       # 영구 실패 목록에 있는 공시는 스킵
        return False

    try:
        # 1. 문서 다운로드
        raw_content = api.fetch_document_content(d.rcept_no)
        if not raw_content:
            raise ValueError("Downloaded content is empty.")

        # 2. content_normalizer.py를 통한 처리
        content_type, data, filename = normalize_payload(d.rcept_no, raw_content)
        
        # 3. 최종 객체 이름 결정 및 중복 확인
        object_name = f"{d.rcept_dt}/{d.corp_code}/{filename}"
        if store.object_exists(object_name):
            LOG.info(f"Skip exists: {object_name}")
            return True

        # 4. Minio에 업로드
        if store.upload_document(object_name, data, content_type):
            celery_app.send_task('tasks.summarize_report', args=[object_name])      # 업로드 성공 시, Celery를 통해 후처리 작업을 워커에게 전달
            FAILED_ATTEMPTS.pop(d.rcept_no, None)                                   # 실패 기록 삭제
            return True
        else:
            raise IOError("Minio upload failed")

    except Exception as e:
        LOG.error(f"Processing failed for {d.rcept_no}: {e}", exc_info=False)
        FAILED_ATTEMPTS[d.rcept_no] = FAILED_ATTEMPTS.get(d.rcept_no, 0) + 1
        if FAILED_ATTEMPTS[d.rcept_no] >= MAX_FAIL:                                 # 실패 횟수가 최대치를 넘으면 영구 실패 목록에 추가하여 시도 중지
            PERM_FAILED.add(d.rcept_no)
            LOG.error(f"Permanently failed to process {d.rcept_no} after {MAX_FAIL} attempts.")
        return False

# 폴링 로직
def polling_loop(api: DartApiClient, store: MinioClient, handler: SignalHandler, manual_refresh_event: threading.Event, interval_sec: int):
    '''
    지정된 간격으로 최신 공시 수집
    '''
    LOG.info("Stateless poller started.")
    
    while not handler.shutdown_requested:                                           # 종료 요청 전까지 무한 반복
        yyyymmdd = datetime.now().strftime("%Y%m%d")                                
        LOG.info(f"Polling disclosures for {yyyymmdd}...")

        try:
            # 1. 현 시점 날짜를 기준으로 최근 공시 목록 호출 (최대 20건)
            data = api.fetch_disclosures(date=yyyymmdd, page_no=1, page_count=20)  
            items = (data or {}).get("list", []) if data else []

            if items:
                # 2. API 응답(JSON)을 Disclosure 데이터 객체 리스트로 변환
                disclosures = [Disclosure.from_dict(row) for row in items if isinstance(row, dict)]
                LOG.info(f"Fetched {len(disclosures)} disclosures from API.")
                
                # 3. 가져온 모든 공시에 대해 순차적으로 처리 시도
                for d in disclosures:
                    if handler.shutdown_requested:
                        break
                    
                    # 4. 처리할 공시 출력
                    LOG.info(f"> Processing disclosure: [{d.corp_name}] {d.report_nm}")
                    process_document(api, store, d)
            else:
                LOG.info("No disclosures found in API response for today.")

        except Exception as e:
            # 5. 예외 발생 시 로그 출력 후, 다음 과정 준비
            LOG.error(f"Polling cycle error: {e}", exc_info=False)

        # 6. 다음 폴링 주기까지 대기
        # interval_sec 만큼 대기 OR 수동 갱신/종료 이벤트 발생 시, 스레드 즉시 실행
        manual_refresh_event.wait(timeout=interval_sec)
        if manual_refresh_event.is_set():
            LOG.info("Manual refresh triggered.")
            manual_refresh_event.clear()

# 사용자 입력 처리
def handle_user_input(handler: SignalHandler, manual_refresh_event: threading.Event):
    ''' 
    백그라운드에서 사용자 입력을 받아 수동갱신 및 종료를 처리하는 스레드
    '''
    while not handler.shutdown_requested:
        try:
            command = input("Enter 're' to refresh, 'q' to quit: ").strip().lower()
            if command == 'q':                              # q: 종료 이벤트
                handler.shutdown_requested = True
                manual_refresh_event.set()                  # 메인 루프를 통한 즉시 종료
                break
            elif command == 're':                           # re: 수동 갱신 이벤트
                manual_refresh_event.set()                  # 메인 루프를 통한 즉시 갱신
        except (EOFError, KeyboardInterrupt):
            handler.shutdown_requested = True
            manual_refresh_event.set()
            break

# 메인 실행
if __name__ == "__main__":
    '''
    스크립트 실행 시, 호출되는 부분
    전체 애플리케이션의 초기화 및 실행을 담당
    '''
    try:
        # 1. 환경 변수를 사용하여 각 서비스 클라이언트 초기화
        api = DartApiClient(api_key=os.environ["DART_API_KEY"])
        store = MinioClient(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            bucket_name=os.environ.get("MINIO_BUCKET", "dart-disclosures"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
        
        # 2. 시그널 핸들러 초기화
        handler = SignalHandler()
        
        # 3. 별도의 스레드에서 사용자 입력 처리 시작
        threading.Thread(target=handle_user_input, args=(handler, manual_refresh_event), daemon=True, name="UserInput").start()
        
        # 4. 폴링 간격 호출
        interval = int(os.getenv("POLL_INTERVAL", "300"))
        
        # 5. 별도의 스레드에서 메인 폴링 루프 시작
        polling_thread = threading.Thread(
            target=polling_loop, 
            args=(api, store, handler, manual_refresh_event, interval), 
            daemon=True, 
            name="Polling"
        )
        polling_thread.start()
        
        LOG.info("DART ingestion service started.")
        polling_thread.join()
        LOG.info("Shutting down...")

    except KeyError as e:
        LOG.error(f"Configuration error: Environment variable not set: {e}")
    except Exception as e:
        LOG.error(f"An unexpected error occurred during startup: {e}", exc_info=True)