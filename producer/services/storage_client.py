import io
import logging
import mimetypes
from minio import Minio
from minio.error import S3Error

class MinioClient:
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
        try:
            found = self.client.bucket_exists(self.bucket_name)
            if not found:
                self.client.make_bucket(self.bucket_name)
                logging.info(f"Bucket '{self.bucket_name}' created.")
            else:
                logging.info(f"Bucket '{self.bucket_name}' already exists.")
        except S3Error as e:
            logging.error(f"Error checking or creating bucket '{self.bucket_name}': {e}")
            raise

    def object_exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(self.bucket_name, object_name)
            return True
        except S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            logging.error(f"Error checking existence of {object_name}: {e}")
            return False

    def upload_document(self, object_name: str, content_bytes: bytes, content_type: str | None) -> bool:
        try:
            if content_type is None:
                content_type, _ = mimetypes.guess_type(object_name)
                if content_type is None:
                    content_type = 'application/octet-stream'

            content_stream = io.BytesIO(content_bytes)
            
            self.client.put_object(
                self.bucket_name,
                object_name,
                content_stream,
                len(content_bytes),
                content_type=content_type
            )
            return True
        except S3Error as e:
            logging.error(f"Failed to upload {object_name} to Minio: {e}")
            return False

