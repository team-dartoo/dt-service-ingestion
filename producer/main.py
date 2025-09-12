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
    MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

    @classmethod
    def validate(cls):
        if not all([cls.DART_API_KEY, cls.MINIO_ACCESS_KEY, cls.MINIO_SECRET_KEY, cls.MINIO_BUCKET_NAME]):
            raise ValueError("Required environment variables are not set.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="\t%(asctime)s \n\t %(levelname)s | %(threadName)s \n\t\t %(message)s"
)

# Graceful Shutdown Handler
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
    ''' 사용자 입력을 받아 수동 갱신 신호 발생 '''
    
    while not handler.shutdown_requested:
        try:
            # 수동 갱신
            print("\nEnter \'re\' if you want a manual refresh (Exit: Ctrl+C):", end='', flush=True)
            user_input = input().strip()
            
            if not user_input: 
                continue
            
            if user_input == 're':
                logging.info("Manual refresh triggered by user.")
                refresh_event.set()
                continue
            else:
                print(f"  > Unknown command: '{user_input}'. Please enter 'refresh' or Ctrl+C.")

        except (EOFError, KeyboardInterrupt):
            break
        except Exception as e:
            logging.error(f"An error occurred in user input thread: {e}")

def polling_loop(dart_client: DartApiClient, storage_client: MinioClient, handler: SignalHandler, refresh_event: threading.Event):
    ''' 주기적으로 DART API를 폴링하여 신규 공시 수집, 저장 후 Celery 작업 발행 '''
    
    processed_rcept_nos: Set[str] = set()

    while not handler.shutdown_requested:
        try:
            # 날짜 설정
            date_to_fetch = datetime.now(UTC).strftime('%Y%m%d')
            
            logging.info(f"Fetching latest 10 disclosures for {date_to_fetch}...")
            disclosures = dart_client.fetch_disclosures(date_to_fetch, limit=10)

            if not disclosures:
                logging.info("No disclosures found in this cycle.")
            else:
                for disclosure in reversed(disclosures):
                    if disclosure.rcept_no in processed_rcept_nos:
                        continue

                    logging.info(f"NEW: [{disclosure.corp_code}] [{disclosure.corp_name}] {disclosure.report_nm}")
                    
                    # 파일 처리 로직: Zip 압축 해제
                    content_bytes = dart_client.fetch_document_content(disclosure.rcept_no)
                    if not content_bytes:
                        logging.warning(f" > Failed to fetch document for {disclosure.rcept_no}.")
                        processed_rcept_nos.add(disclosure.rcept_no)
                        continue

                    xml_content_bytes = None
                    try:
                        # 1. ZIP 파일 확인
                        if zipfile.is_zipfile(io.BytesIO(content_bytes)):
                            with zipfile.ZipFile(io.BytesIO(content_bytes)) as z:
                                # 압축 파일 내 .xml 파일 찾기
                                xml_filename = next((name for name in z.namelist() if name.lower().endswith('.xml')), None)
                                if xml_filename:
                                    xml_content_bytes = z.read(xml_filename)
                                else:
                                    logging.warning(f" > No XML file found in zip for {disclosure.rcept_no}.")
                        else:
                            # 2. ZIP 파일 아닌 경우, 내용 전체를 XML/HTML로 판단
                            xml_content_bytes = content_bytes
                    except zipfile.BadZipFile:
                        logging.error(f" > Corrupted zip file for {disclosure.rcept_no}.")
                        processed_rcept_nos.add(disclosure.rcept_no)
                        continue
                    
                    if xml_content_bytes:
                        try:
                            # 3. chardet으로 인코딩 감지
                            detected = chardet.detect(xml_content_bytes)
                            encoding = detected['encoding'] if detected['confidence'] > 0.7 else 'utf-8'
                            
                            # 4. 감지된 인코딩으로 디코딩 진행
                            xml_str = xml_content_bytes.decode(encoding, errors='ignore')
                            xml_str = re.sub(r'(<\?xml[^>]*encoding=")[^"]*(")', r'\1UTF-8\2', xml_str, count=1, flags=re.IGNORECASE)
                            final_content_bytes = xml_str.encode('utf-8')
                            
                            # 기본적인 데이터 유효성 확인 (파일 크기, 내용 존재)
                            if len(final_content_bytes) > 100:  # 최소 100바이트 이상
                                logging.info(f" > Document processed successfully for {disclosure.rcept_no} ({len(final_content_bytes)} bytes)")
                            else:
                                logging.warning(f" > Document too small for {disclosure.rcept_no} ({len(final_content_bytes)} bytes)")

                            object_name = f"{disclosure.rcept_dt}/{disclosure.rcept_no}.xml"
                            
                            # Minio에 업로드하고 성공 시 Celery 작업 발행
                            if storage_client.upload_document(object_name, final_content_bytes):
                                logging.info(f" > Upload success. Sending task to worker for: {object_name}")
                                celery_app.send_task('tasks.summarize_report', args=[object_name])
                            else:
                                logging.error(f" > Failed to upload {object_name}. Task not sent.")

                        except (UnicodeDecodeError, TypeError) as e:
                            logging.error(f" > Failed to decode/encode content for {disclosure.rcept_no}: {e}")
                    
                    processed_rcept_nos.add(disclosure.rcept_no)
            
            logging.info("Waiting for next cycle...")
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

    user_thread = threading.Thread(target=handle_user_input, args=(dart_client, handler, manual_refresh_event), daemon=True, name="UserInput")
    user_thread.start()
    
    polling_thread = threading.Thread(target=polling_loop, args=(dart_client, storage_client, handler, manual_refresh_event), daemon=True, name="Polling")
    polling_thread.start()
    
    logging.info("Starting DART ingestion service and producer.")
    logging.info(f"Polling interval: {Config.POLLING_INTERVAL_SECONDS} seconds")
    
    # 종료 요청까지 메인 스레드는 대기
    while not handler.shutdown_requested:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            handler.request_shutdown()
            break
    
    # 대기 상태인 경우 깨우기
    manual_refresh_event.set()
    polling_thread.join(timeout=5)
    logging.info("DART Ingestion Service has been shut down.")

if __name__ == "__main__":
    run_polling_service()