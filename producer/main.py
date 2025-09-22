import os
import io
import re                                   # XML 인코딩 선언문 수정
import zipfile
import time
import logging
import signal
import threading
import chardet                              # 문자 인코딩 감지 라이브러리
import xml.etree.ElementTree as ET          # XML 유효성 검사
from datetime import datetime, UTC
from typing import Set

from dotenv import load_dotenv
from celery import Celery

from services.dart_api_client import DartApiClient
from services.storage_client import MinioClient
from models.disclosure import Disclosure

load_dotenv()

# Celery Producer App 설정 - RabbitMQ 접속 정보는 환경에 맞게 수정 필요
celery_app = Celery('producer', broker='amqp://admin:password@localhost:5672/')
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Seoul',
    enable_utc=False,
)

class Config:
    ''' 환경 변수 기반 설정 값 보관/검증 클래스 '''
    
    DART_API_KEY = os.getenv("DART_API_KEY")
    POLLING_INTERVAL_SECONDS = int(os.getenv("POLLING_INTERVAL_SECONDS", 300))
    
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "dart-disclosures")
    
    @staticmethod
    def validate():
        if not Config.DART_API_KEY:
            raise ValueError("DART_API_KEY must be set in environment variables.")
        if not Config.MINIO_ACCESS_KEY or not Config.MINIO_SECRET_KEY:
            raise ValueError("MINIO access credentials must be set.")


class SignalHandler:
    ''' SIGINT, SIGTERM 시그널 처리 및 종료 플래그 관리 클래스 '''
    
    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)

    def _request_shutdown(self, signum, frame):
        logging.info("Shutdown signal received. Finishing current tasks...")
        self.shutdown_requested = True


def polling_loop(
    dart_client: DartApiClient, 
    storage_client: MinioClient, 
    handler: SignalHandler,
    manual_refresh_event: threading.Event
):
    ''' 주기적으로 DART API를 폴링하여 신규 공시를 처리하는 메인 루프 '''
    
    processed_rcept_nos: Set[str] = set()
    
    while not handler.shutdown_requested:
        try:
            today_str = datetime.now().strftime('%Y%m%d')
            logging.info(f"[{threading.current_thread().name}] Fetching disclosures for {today_str}...")
            
            # 일일 최대 공시 건수를 고려하여 limit 증가 (기존 10 -> 500)
            disclosures = dart_client.fetch_disclosures(date=today_str, limit=500)
            
            if disclosures is None:
                logging.warning("Failed to fetch disclosures. Will retry.")
            
            elif not disclosures:
                logging.info("API returned no disclosures for today.")

            else:
                new_disclosures_found = False
                for disclosure in disclosures:
                    if handler.shutdown_requested: break
                    if disclosure.rcept_no in processed_rcept_nos:
                        continue
                    
                    new_disclosures_found = True
                    logging.info(f" > New disclosure found: [{disclosure.corp_name}] {disclosure.report_nm}")
                    
                    content_bytes = dart_client.fetch_document_content(disclosure.rcept_no)
                    if not content_bytes or len(content_bytes) < 100:
                        logging.warning(f" > Content for {disclosure.rcept_no} is empty or too small. Skipping.")
                        continue
                    
                    document_content_bytes = None
                    document_filename = None

                    # 1. 압축 파일(ZIP)인 경우, 우선순위에 따라 내부 파일 탐색
                    if zipfile.is_zipfile(io.BytesIO(content_bytes)):
                        with zipfile.ZipFile(io.BytesIO(content_bytes)) as z:
                            priority_extensions = ['.xml', '.html', '.pdf', '.docx']
                            file_list = z.namelist()
                            
                            for ext in priority_extensions:
                                found_file = next((name for name in file_list if name.lower().endswith(ext)), None)
                                if found_file:
                                    document_filename = os.path.basename(found_file)
                                    document_content_bytes = z.read(found_file)
                                    break
                            
                            if not document_content_bytes:
                                logging.warning(f" > No priority document file found in zip for {disclosure.rcept_no}. Skipping.")
                                continue
                    else:
                        # 2. 압축 파일이 아닌 경우, 파일 시그니처(Magic Number)를 기반으로 확장자 추측
                        document_content_bytes = content_bytes
                        
                        # PDF 파일 시그니처 (%PDF) 확인
                        if document_content_bytes.startswith(b'%PDF'):
                            document_filename = f"{disclosure.rcept_no}.pdf"
                        else:
                            # 기존의 텍스트 기반 추측 로직 수행
                            content_str_start = document_content_bytes[:100].decode('latin-1').lower()
                            if '<html>' in content_str_start:
                                document_filename = f"{disclosure.rcept_no}.html"
                            elif '<?xml' in content_str_start:
                                document_filename = f"{disclosure.rcept_no}.xml"
                            else:
                                # 어느 쪽도 아니면 일반 바이너리 파일로 처리
                                document_filename = f"{disclosure.rcept_no}.bin"

                    # 3. 최종 저장 경로를 결정하고, Minio에 이미 존재하는지 확인하여 중복 처리 방지
                    if document_content_bytes and document_filename:
                        _, extension = os.path.splitext(document_filename)
                        object_name = f"{disclosure.rcept_dt}/{disclosure.rcept_no}{extension}"

                        try:
                            if storage_client.object_exists(object_name):
                                logging.info(f" > Document {object_name} already exists. Skipping.")
                                processed_rcept_nos.add(disclosure.rcept_no)
                                continue
                        except Exception as e:
                            logging.error(f" > Failed to check existence of {object_name}. Skipping. Error: {e}")
                            continue

                        # 4. 텍스트 기반 파일(.xml, .html 등)에 대해서만 인코딩 정규화 수행
                        final_content_bytes = document_content_bytes
                        if extension.lower() in ['.xml', '.html', '.htm', '.txt']:
                            try:
                                detected = chardet.detect(document_content_bytes)
                                encoding = detected['encoding'] if detected['confidence'] > 0.7 else 'utf-8'
                                decoded_content = document_content_bytes.decode(encoding, errors='ignore')

                                if extension.lower() == '.xml':
                                    decoded_content = re.sub(r'<\?xml[^>]+encoding="[^"]+"', '<?xml version="1.0" encoding="UTF-8"', decoded_content, count=1)
                                
                                final_content_bytes = decoded_content.encode('utf-8')
                            except Exception as e:
                                logging.warning(f" > Could not process encoding for {object_name}. Uploading as is. Error: {e}")
                                final_content_bytes = document_content_bytes

                        # 5. Minio에 업로드하고 성공 시 Celery 작업 발행
                        if storage_client.upload_document(object_name, final_content_bytes):
                            logging.info(f" > Upload success. Sending task for: {object_name}")
                            celery_app.send_task('tasks.summarize_report', args=[object_name])
                        
                        processed_rcept_nos.add(disclosure.rcept_no)

                if not new_disclosures_found:
                    logging.info("Found disclosures, but all have been processed previously in this session.")

        except Exception as e:
            logging.error(f"An unexpected error occurred in polling loop: {e}")

        # 다음 폴링까지 대기 또는 수동 갱신 신호 대기
        manual_refresh_event.wait(timeout=Config.POLLING_INTERVAL_SECONDS)
        if manual_refresh_event.is_set():
            logging.info("Manual refresh triggered.")
            manual_refresh_event.clear()


def handle_user_input(dart_client: DartApiClient, handler: SignalHandler, manual_refresh_event: threading.Event):
    ''' 사용자 입력 처리 스레드 함수 (수동 갱신, 종료) '''
    
    while not handler.shutdown_requested:
        try:
            command = input("Enter 're' to refresh, 'q' to quit: ").strip().lower()
            if command == 'q':
                handler.request_shutdown()
                manual_refresh_event.set() # 대기 중인 폴링 스레드 깨우기
                break
            elif command == 're':
                manual_refresh_event.set() # 폴링 루프 즉시 실행 신호
        except (EOFError, KeyboardInterrupt):
            handler.request_shutdown()
            manual_refresh_event.set()
            break


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s')
    
    try:
        Config.validate()
    except ValueError as e:
        logging.error(e)
        # return 구문은 스크립트 실행을 중단시키므로 제거하고 exit() 사용을 고려할 수 있습니다.
        # 이 컨텍스트에서는 그냥 두어도 무방합니다.
        exit(1)

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
        exit(1)
        
    handler = SignalHandler()
    manual_refresh_event = threading.Event()

    user_thread = threading.Thread(target=handle_user_input, args=(dart_client, handler, manual_refresh_event), daemon=True, name="UserInput")
    user_thread.start()
    
    polling_thread = threading.Thread(target=polling_loop, args=(dart_client, storage_client, handler, manual_refresh_event), daemon=True, name="Polling")
    polling_thread.start()
    
    logging.info("Starting DART ingestion service and producer.")
    logging.info(f"Polling interval: {Config.POLLING_INTERVAL_SECONDS} seconds")
    
    # 메인 스레드는 종료 요청이 있을 때까지 대기합니다.
    while not handler.shutdown_requested:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            # 이중 안전장치
            handler.request_shutdown()
            manual_refresh_event.set()
            break
    
    logging.info("Shutting down DART ingestion service.")

