import uuid
import time
from celery import Celery
import logging

# --Start: Polling Logic--



import os
import io
import re                   # XML 인코딩 선언문 수정                  
import zipfile
import signal
import threading
import chardet              # 문자 인코딩 감지
from datetime import datetime, UTC
from typing import Set

from dotenv import load_dotenv

from producer.services.dart_api_client import DartApiClient
from producer.services.storage_client import MinioClient
from producer.models.disclousre import Disclosure

load_dotenv()

class Config:
    ''' 환경 변수 기반 설정 값 보관/검증 클래스 '''
    
    DART_API_KEY = os.getenv("DART_API_KEY")
    POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS", 300))
    
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

    @classmethod
    def validate(cls):
        if not all([cls.DART_API_KEY, cls.MINIO_ACCESS_KEY, cls.MINIO_SECRET_KEY, cls.MINIO_BUCKET_NAME]):
            raise ValueError("Required environment variables are not set.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)

# 그레이스풀 셧다운 핸들러
class SignalHandler:
    shutdown_requested = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

    def request_shutdown(self, *args):
        if not self.shutdown_requested:
            logging.info("Shutdown requested. Finishing current cycle...")
            self.shutdown_requested = True

def handle_user_input(client: DartApiClient, handler: SignalHandler, refresh_event: threading.Event):
    ''' 사용자 입력을 받아 기업 상세 정보 조회 또는 수동 갱신 신호 발생 '''
    
    while not handler.shutdown_requested:
        try:
            print("\nEnter the corporation code (Enter: Renewal, Exit: Ctrl+C): ", end='', flush=True)
            user_input = input().strip()
            
            if not user_input:
                continue
            
            if user_input == 'Renewal':
                logging.info("Manual refresh triggered by user.")
                refresh_event.set()
                continue

            profile = client.fetch_company_profile(user_input)
            
            if profile:
                print("\n--- 기업 상세 정보 ---")
                for key, value in profile.items():
                    if key not in ['status', 'message']:
                        print(f"{key}: {value}")
                print("---------------------")
            else:
                print(f" > [{user_input}] is not existence...")

        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            logging.error(f"An error occurred in user input thread: {e}")

def polling_loop(dart_client: DartApiClient, storage_client: MinioClient, handler: SignalHandler, refresh_event: threading.Event):
    ''' 주기적으로 DART API를 폴링하여 신규 공시 수집 및 저장 '''
    
    processed_rcept_nos: Set[str] = set()

    while not handler.shutdown_requested:
        try:
            # 날짜 설정 
            date_to_fetch = datetime.now(UTC).strftime('%Y%m%d')
            
            logging.info(f"Fetching disclosures for {date_to_fetch}...")
            disclosures = dart_client.fetch_disclosures(date_to_fetch)

            if disclosures:
                new_disclosures_found = False
                for disclosure in reversed(disclosures):
                    if disclosure.rcept_no not in processed_rcept_nos:
                        new_disclosures_found = True
                        logging.info(f"NEW: [{disclosure.corp_code}] [{disclosure.corp_name}] {disclosure.report_nm}")
                        
                        # 파일 처리 로직 
                        content_bytes = dart_client.fetch_document_content(disclosure.rcept_no)
                        if not content_bytes:
                            logging.warning(f" > Failed to fetch document for {disclosure.rcept_no}.")
                            processed_rcept_nos.add(disclosure.rcept_no)
                            continue

                        logging.info(f" > Fetched document for {disclosure.rcept_no}, size: {len(content_bytes)} bytes.")
                        xml_content_bytes = None

                        # 1. 파일이 ZIP 형식인지 명시적으로 확인
                        if zipfile.is_zipfile(io.BytesIO(content_bytes)):
                            logging.info(f" > Document for {disclosure.rcept_no} is a zip file. Extracting XML...")
                            try:
                                with zipfile.ZipFile(io.BytesIO(content_bytes)) as z:
                                    # 압축 파일 내의 .xml 파일 찾기
                                    xml_filename = next((name for name in z.namelist() if name.lower().endswith('.xml')), None)
                                    if xml_filename:
                                        xml_content_bytes = z.read(xml_filename)
                                        logging.info(f" > Successfully extracted {xml_filename} from zip.")
                                    else:
                                        logging.warning(f" > No XML file found in zip for {disclosure.rcept_no}.")
                            except zipfile.BadZipFile:
                                logging.error(f" > Failed to process zip file for {disclosure.rcept_no}. It may be corrupted.")
                        else:
                            # 2. ZIP 파일이 아니면, 내용 전체를 XML/HTML로 간주
                            logging.info(f" > Document for {disclosure.rcept_no} is not a zip file. Processing as plain text.")
                            xml_content_bytes = content_bytes
                        
                        if xml_content_bytes:
                            # 3. chardet으로 인코딩 감지
                            detected = chardet.detect(xml_content_bytes)
                            encoding = detected['encoding'] if detected['confidence'] > 0.7 else 'utf-8'
                            logging.info(f" > Detected encoding: {encoding} with confidence {detected['confidence']:.2f}")

                            try:
                                # 4. 감지된 인코딩으로 디코딩
                                xml_str = xml_content_bytes.decode(encoding, errors='ignore')
                                
                                # 5. XML 선언의 인코딩을 UTF-8로 수정 (꼬리표와 내용 일치)
                                xml_str = re.sub(r'(<\?xml[^>]*encoding=")[^"]*(")', r'\1UTF-8\2', xml_str, count=1, flags=re.IGNORECASE)
                                
                                # 6. 최종적으로 UTF-8 바이트로 변환하여 저장 준비
                                final_content_bytes = xml_str.encode('utf-8')
                                
                                object_name = f"{disclosure.rcept_dt}/{disclosure.rcept_no}.xml"
                                storage_client.upload_document(object_name, final_content_bytes)
                            except (UnicodeDecodeError, TypeError) as e:
                                logging.error(f" > Failed to decode/encode content for {disclosure.rcept_no} with detected encoding {encoding}: {e}")
                        else:
                            logging.warning(f" > Could not extract final XML content for {disclosure.rcept_no}.")
                    
                        processed_rcept_nos.add(disclosure.rcept_no)
                
                if not new_disclosures_found:
                    logging.info("No new disclosures found in this cycle.")
            
            logging.info("Waiting for next cycle or manual refresh trigger...")
            refresh_event.wait(timeout=Config.POLLING_INTERVAL_SECONDS)
            if refresh_event.is_set():
                logging.info("Manual refresh signal received, starting new cycle immediately.")
                refresh_event.clear()

        except Exception as e:
            logging.error(f"Unexpected error in polling loop: {e}", exc_info=True)
            time.sleep(60)

def run_polling_service():
    ''' 폴링 서비스와 사용자 입력을 별도 스레드로 실행하는 메인 함수 '''
    
    try:
        Config.validate()
    except ValueError as e:
        logging.error(e)
        return

    dart_client = DartApiClient(api_key=Config.DART_API_KEY)
    
    try:
        storage_client = MinioClient(
            endpoint=Config.MINIO_ENDPOINT,
            access_key=Config.MINIO_ACCESS_KEY,
            secret_key=Config.MINIO_SECRET_KEY,
            bucket_name=Config.MINIO_BUCKET_NAME
        )
    except Exception as e:
        logging.error(f"Could not connect to Minio. Aborting. Error: {e}")
        return
        
    handler = SignalHandler()
    manual_refresh_event = threading.Event()

    user_thread = threading.Thread(
        target=handle_user_input, 
        args=(dart_client, handler, manual_refresh_event), 
        daemon=True,
        name="UserInput"
    )
    user_thread.start()
    
    polling_thread = threading.Thread(
        target=polling_loop, 
        args=(dart_client, storage_client, handler, manual_refresh_event), 
        daemon=True,
        name="Polling"
    )
    polling_thread.start()
    
    logging.info("Starting DART ingestion service")
    logging.info(f"Polling interval {Config.POLLING_INTERVAL_SECONDS} seconds")
    
    # 메인 스레드는 시그널 핸들러가 종료를 요청할 때까지 대기
    while not handler.shutdown_requested:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            handler.request_shutdown()
            break
            
    manual_refresh_event.set()           # 스레드가 대기 상태에 빠져있을 경우 깨우기
    polling_thread.join(timeout=5)
    logging.info("DART Ingestion Service has been shut down.")

if __name__ == "__main__":
    run_polling_service()



# --End: Polling Logic--

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery configuration for producer
app = Celery(
    'producer',
    broker='amqp://admin:password@rabbitmq:5672/',
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

def mock_report_producer():
    """Start the UUID generator that sends tasks every 1-2 seconds"""
    logger.info("Starting UUID generator...")
    
    while True:
        try:
            # Generate UUID
            task_uuid = str(uuid.uuid4())
            logger.info(f"Generated UUID: {task_uuid}")
            
            # Send task to worker queue using the correct task name
            result = app.send_task('tasks.summarize_report', args=[task_uuid])
            logger.info(f"Task sent with ID: {result.id}")
            
            # Wait 1-2 seconds (random between 1000-2000ms)
            import random
            wait_time = random.uniform(1.0, 2.0)
            time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error in UUID generator: {e}")
            time.sleep(1)

if __name__ == "__main__":
    mock_report_producer()
