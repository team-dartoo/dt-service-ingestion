import io
import logging
import mimetypes                    # 파일 확장자를 기반으로 MIME 타입을 추측하기 위한 모듈
from minio import Minio             # MinIO 서버와 통신하기 위한 메인 라이브러리
from minio.error import S3Error     # MinIO 관련 예외처리를 위한 클래스

class MinioClient:
    ''' 
    MinIO 서버 연결 및 파일 업로드/관리를 위한 클래스
    '''
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
        '''
        MinIO 클라이언트를 초기화하고 서버에 연결
        생성자에서 버킷 존재 여부 확인하고, 사용 가능한 상태로 저장
        
        Args:
            endpoint (str): MinIO 서버 주소 (예: "127.0.0.1:9000")
            access_key (str): 접속 ID
            secret_key (str): 접속 비밀번호
            bucket_name (str): 사용할 버킷 이름
            secure (bool): HTTPS 사용 여부
        '''
        try:
            self.client = Minio(                                                # Minio 클라이언트 객체 생성
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            self.bucket_name = bucket_name          
            self._ensure_bucket_exists()                                        # 클라이언트 생성하고 버킷 존재 확인, 없으면 생성
        except Exception as e:                                                  # 초기화 과정에서 발생하는 모든 예외처리
            logging.error(f"Failed to initialize Minio client: {e}")
            raise

    def _ensure_bucket_exists(self):
        '''
        지정된 버킷이 MinIO 서버에 존재하는지 확인하고, 없으면 새로 생성
            - 해당 메서드는 private으로 선언되어 클래스 내부에서만 사용
        '''
        try:
            found = self.client.bucket_exists(self.bucket_name)
            if not found:
                self.client.make_bucket(self.bucket_name)
                logging.info(f"Bucket '{self.bucket_name}' created.")
            else:
                logging.info(f"Bucket '{self.bucket_name}' already exists.")
        except S3Error as e:                                                    # 버킷 생성/확인 중 발생할 수 있는 S3 관련 오류 처리
            logging.error(f"Error checking or creating bucket: {e}")
            raise

    def object_exists(self, object_name: str) -> bool:
        '''
        Minio 버킷에 객체가 존재하는지 확인
        'main.py'의 상태 비저장(Stateless) 폴링 로직에서 데이터 중복 처리 방지 역할
        
        Args:
            object_name (str): 확인할 객체의 전체 경로 및 이름

        Returns:
            bool: 객체가 존재하면 True, 존재하지 않으면 False
        '''
        try:
            self.client.stat_object(self.bucket_name, object_name)              # stat_object: 객체의 메타 데이터 요청
            return True                                                         # 객체 존재하면 성공, 없으면 S3Error 예외 발생
        except S3Error as e:
            if e.code in ('NoSuchKey', 'NoSuchObject'):                         # 'NoSuchKey' 또는 'NoSuchObject'는 "객체가 존재하지 않는다"는 의미의 정상적인 오류 코드
                return False
            logging.error(f"Error checking existence for {object_name}: {e}")   # 그 외의 오류(예: 네트워크 문제, 권한 문제)는 로깅하고 예외 발생
            raise

    def upload_document(self, object_name: str, content_bytes: bytes, content_type: str | None = None) -> bool:
        ''' 
        정규화된 공시 원문(바이너리 데이터)를 MinIO 버킷에 업로드
        
        Args:
            object_name (str): 저장할 객체의 전체 경로 및 이름
            content_bytes (bytes): 저장할 파일의 실제 데이터
            content_type (str | None): 파일의 MIME 타입. 제공되지 않으면 파일명으로 추정

        Returns:
            bool: 업로드 성공 시 True, 실패 시 False
        '''
        try:
            if content_type is None:                                            # Content-Type이 명시되지 않은 경우
                content_type, _ = mimetypes.guess_type(object_name)             # 'mimetypes' 모듈을 사용하여 파일 확장자를 기반으로 자동으로 추정
                if content_type is None:                                        # 타입을 알 수 없는 경우, 일반적인 바이너리 스트림으로 기본값 설정
                    content_type = 'application/octet-stream'

            content_stream = io.BytesIO(content_bytes)                          # 바이트 데이터를 메모리 상의 스트림으로 변환하여 'put_object'에 전달
            
            self.client.put_object(
                self.bucket_name,               # 대상 버킷
                object_name,                    # 지정될 이름
                content_stream,                 # 데이터 스트림
                len(content_bytes),             # 데이터 길이
                content_type=content_type       # 파일 종류(MIME type)
            )
            logging.info(f"Successfully uploaded {object_name} to bucket {self.bucket_name} with type {content_type}.")
            return True
        except S3Error as e:                                                    # 파일 업로드 중 발생할 수 있는 모든 S3관련 오류 처리
            logging.error(f"Failed to upload {object_name} to Minio: {e}")
            return False