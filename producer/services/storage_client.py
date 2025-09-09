import io
import logging
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

    def upload_document(self, object_name: str, content_bytes: bytes) -> bool:
        ''' 공시 원문(바이너리)을 Minio 버킷에 업로드 '''
        
        try:
            # 바이너리 데이터는 인코딩 과정 제거
            content_stream = io.BytesIO(content_bytes)
            
            self.client.put_object(
                self.bucket_name,
                object_name,
                content_stream,
                len(content_bytes),
                content_type='application/xml'
            )
            logging.info(f"Successfully uploaded {object_name} to bucket {self.bucket_name}.")
            return True
        except S3Error as e:
            logging.error(f"Failed to upload {object_name}: {e}")
            return False