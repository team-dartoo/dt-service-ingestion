import io
import logging
import mimetypes                                                                # 파일 확장자를 기반으로 MIME 타입을 추측하기 위한 모듈
from minio import Minio                                                         # MinIO 서버와 통신하기 위한 메인 라이브러리
from minio.error import S3Error                                                 # MinIO 관련 예외처리를 위한 클래스

# -------------------- MinIO 객체 스토리지 서버와의 연결 및 파일 관리를 담당하는 클래스 --------------------
class MinIOClient:
    
    # ---------- MinIO 클라이언트를 초기화하고 서버에 연결 ----------
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
        try:
            self.client = Minio(                                                # Minio 클라이언트 객체 생성
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            self.bucket_name = bucket_name
            self._ensure_bucket_exists()                                        # 생성자에서 버킷 존재 여부를 확인하고, 없으면 생성
        except Exception as e:                                                  # 초기화 과정에서 발생하는 모든 예외 처리
            logging.error(f"Failed to initialize Minio client: {e}")
            raise

    # ---------- 지정된 버킷이 존재하는지 확인하고, 없으면 새로 생성하는 내부 메서드 ----------
    def _ensure_bucket_exists(self):
        try:
            found = self.client.bucket_exists(self.bucket_name)                 # 버킷 존재 여부 확인
            if not found:
                self.client.make_bucket(self.bucket_name)                       # 버킷이 없으면 생성
                logging.info(f"Bucket '{self.bucket_name}' created.")
        except S3Error as e:                                                    # 버킷 확인/생성 중 발생할 수 있는 S3 관련 오류 처리
            logging.error(f"Error checking or creating bucket '{self.bucket_name}': {e}")
            raise
        
    # ---------- 특정 객체(파일)가 버킷에 이미 존재하는지 확인 ----------
    def object_exists(self, object_name: str) -> bool:
        try:
            if '*' in object_name:                                            # 와일드카드(*)가 포함된 객체 이름 처리
                prefix = object_name.split('*', 1)[0]                         # 와일드카드 앞부분을 접두사로 사용
                objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
                for _ in objects:
                    return True
                return False
            
            self.client.stat_object(self.bucket_name, object_name)              # 객체의 메타데이터 요청
            return True                                                         # 성공 시 객체가 존재함을 의미
        
        except S3Error as e:
            if e.code == 'NoSuchKey':                                           # 'NoSuchKey' 에러는 객체가 존재하지 않음을 의미
                return False
            logging.error(f"Error checking existence of {object_name}: {e}")    # 그 외의 에러는 로그로 기록
            return False

    # ---------- 주어진 바이트 데이터를 MinIO 버킷에 객체로 업로드 ----------
    def upload_document(self, object_name: str, content_bytes: bytes, content_type: str | None) -> bool:
        try:
            if content_type is None:                                            # Content-Type(MIME 타입)이 명시되지 않은 경우
                content_type, _ = mimetypes.guess_type(object_name)             # 파일 확장자를 기반으로 자동 추정
                if content_type is None:
                    content_type = 'application/octet-stream'                   # 타입을 알 수 없으면 일반 바이너리로 설정

            content_stream = io.BytesIO(content_bytes)                          # 바이트 데이터를 메모리 내 스트림으로 변환
            
            self.client.put_object(                                             # put_object API를 사용하여 실제 파일 업로드 실행
                self.bucket_name,
                object_name,
                content_stream,
                len(content_bytes),
                content_type=content_type
            )
            return True                                                         # 업로드 성공
        except S3Error as e:                                                    # 파일 업로드 중 발생할 수 있는 모든 S3 관련 오류 처리
            logging.error(f"Failed to upload {object_name} to Minio: {e}")
            return False
