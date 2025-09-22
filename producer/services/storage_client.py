import io
import logging
import mimetypes
from minio import Minio
from minio.error import S3Error

class MinioClient:
    ''' Minio 서버 연결 및 파일 업로드 관리 클래스 '''
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
        try:
            self.client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            self.bucket_name = bucket_name
            self._ensure_bucket_exists()
        except Exception as e:
            logging.error(f"Failed to initialize Minio client: {e}")
            raise

    def _ensure_bucket_exists(self):
        ''' 버킷 존재 여부 확인 및 필요 시 생성 '''
        
        try:
            found = self.client.bucket_exists(self.bucket_name)
            if not found:
                self.client.make_bucket(self.bucket_name)
                logging.info(f"Bucket '{self.bucket_name}' created.")
            else:
                logging.info(f"Bucket '{self.bucket_name}' already exists.")
        except S3Error as e:
            logging.error(f"Error checking or creating bucket: {e}")
            raise
            
    def object_exists(self, object_name: str) -> bool:
        """
        Minio 버킷에 객체가 존재하는지 확인합니다.
        
        Args:
            object_name: 확인할 객체의 이름
        Returns:
            객체가 존재하면 True, 그렇지 않으면 False
        """
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            logging.error(f"Error checking existence for {object_name}: {e}")
            raise


    def upload_document(self, object_name: str, content_bytes: bytes) -> bool:
        ''' 공시 원문(바이너리)을 Minio 버킷에 업로드 (Content-Type 자동 감지) '''
        
        try:
            content_stream = io.BytesIO(content_bytes)
            
            # 파일 확장자를 기반으로 Content-Type(MIME 타입)을 추측합니다.
            content_type, _ = mimetypes.guess_type(object_name)
            if content_type is None:
                # 추측할 수 없는 경우, 일반적인 바이너리 스트림으로 처리합니다.
                content_type = 'application/octet-stream'
            
            self.client.put_object(
                self.bucket_name,
                object_name,
                content_stream,
                len(content_bytes),
                content_type=content_type
            )
            logging.info(f"Successfully uploaded {object_name} to bucket {self.bucket_name}.")
            return True
        except S3Error as e:
            logging.error(f"Failed to upload {object_name}: {e}")
            return False
