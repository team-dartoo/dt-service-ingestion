"""
Pytest fixtures for Ingestion Service tests.

이 파일은 테스트에서 사용되는 공통 fixture들을 정의합니다.
"""

import pytest
import os
import sys
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime

# Producer 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))

# 테스트 전에 환경 변수 설정
os.environ["MOCK_MODE"] = "true"
os.environ["DART_API_KEY"] = "test-api-key-40-characters-long-1234567"
os.environ["MINIO_ENDPOINT"] = "localhost:9000"
os.environ["MINIO_ACCESS_KEY"] = "minioadmin"
os.environ["MINIO_SECRET_KEY"] = "minioadmin123"
os.environ["MINIO_BUCKET"] = "test-bucket"
os.environ["CELERY_BROKER_URL"] = "amqp://admin:admin123@localhost:5672/"
os.environ["WORKER_API_KEY"] = "test-worker-api-key"
os.environ["DISCLOSURE_SERVICE_URL"] = "http://localhost:8000"


# ============================================================
# Sample Data Fixtures
# ============================================================

@pytest.fixture
def sample_disclosure_message() -> dict:
    """Producer에서 Consumer로 전송되는 메시지 형식"""
    return {
        "corp_code": "00126380",
        "corp_name": "삼성전자",
        "stock_code": "005930",
        "corp_cls": "Y",
        "report_nm": "사업보고서",
        "rcept_no": "20241229000001",
        "flr_nm": "삼성전자",
        "rcept_dt": "20241229",
        "rm": "",
        "object_key": "2024/12/29/20241229000001.html",
        "content_type": "text/html; charset=UTF-8",
        "file_size": 12345,
        "polling_date": "20241229"
    }


@pytest.fixture
def sample_dart_api_response() -> list:
    """DART API 응답 형식"""
    return [
        {
            "corp_code": "00126380",
            "corp_name": "삼성전자",
            "stock_code": "005930",
            "corp_cls": "Y",
            "report_nm": "사업보고서",
            "rcept_no": "20241229000001",
            "flr_nm": "삼성전자",
            "rcept_dt": "20241229",
            "rm": "",
        },
        {
            "corp_code": "00164742",
            "corp_name": "SK하이닉스",
            "stock_code": "000660",
            "corp_cls": "Y",
            "report_nm": "분기보고서",
            "rcept_no": "20241229000002",
            "flr_nm": "SK하이닉스",
            "rcept_dt": "20241229",
            "rm": "유",
        }
    ]


@pytest.fixture
def sample_html_document() -> bytes:
    """샘플 HTML 문서"""
    return b"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>Test Document</title>
</head>
<body>
    <h1>Test Disclosure</h1>
    <p>This is a test document.</p>
</body>
</html>"""


# ============================================================
# Mock Fixtures
# ============================================================

@pytest.fixture
def mock_minio_client():
    """MinIO 클라이언트 Mock"""
    with patch("services.storage_client.Minio") as mock:
        client = MagicMock()
        client.bucket_exists.return_value = True
        client.put_object.return_value = MagicMock()
        client.get_object.return_value = MagicMock(
            read=lambda: b"test content",
            close=lambda: None
        )
        mock.return_value = client
        yield client


@pytest.fixture
def mock_celery_task():
    """Celery Task Mock"""
    with patch("celery.Celery") as mock:
        app = MagicMock()
        app.send_task.return_value = MagicMock(id="test-task-id")
        mock.return_value = app
        yield app


@pytest.fixture
def mock_httpx_client():
    """HTTP 클라이언트 Mock"""
    with patch("httpx.Client") as mock:
        client = MagicMock()
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"status": "success"}
        client.put.return_value = response
        client.get.return_value = response
        mock.return_value.__enter__ = MagicMock(return_value=client)
        mock.return_value.__exit__ = MagicMock(return_value=False)
        yield client


# ============================================================
# Config Fixtures
# ============================================================

@pytest.fixture
def mock_config():
    """테스트용 설정 객체"""
    from dataclasses import dataclass
    
    @dataclass
    class MockDartConfig:
        api_key: str = "test-api-key-40-characters-long-1234567"
        base_url: str = "https://opendart.fss.or.kr/api"
        timeout: int = 30
        max_retries: int = 3
        mock_mode: bool = True
    
    @dataclass
    class MockMinioConfig:
        endpoint: str = "localhost:9000"
        access_key: str = "minioadmin"
        secret_key: str = "minioadmin123"
        bucket_name: str = "test-bucket"
        secure: bool = False
    
    @dataclass
    class MockCeleryConfig:
        broker_url: str = "amqp://admin:admin123@localhost:5672/"
    
    @dataclass
    class MockDisclosureConfig:
        base_url: str = "http://localhost:8000"
        api_key: str = "test-worker-api-key"
        timeout: int = 30
        max_retries: int = 3
    
    @dataclass
    class MockPollingConfig:
        interval_seconds: int = 60
        target_date: str = None
        max_fail: int = 3
        failed_log_dir: str = None
    
    @dataclass
    class MockHealthConfig:
        enabled: bool = True
        port: int = 8001
        host: str = "0.0.0.0"
    
    @dataclass
    class MockAppConfig:
        dart: MockDartConfig = None
        minio: MockMinioConfig = None
        celery: MockCeleryConfig = None
        disclosure: MockDisclosureConfig = None
        polling: MockPollingConfig = None
        health: MockHealthConfig = None
        log_level: str = "INFO"
        
        def __post_init__(self):
            self.dart = MockDartConfig()
            self.minio = MockMinioConfig()
            self.celery = MockCeleryConfig()
            self.disclosure = MockDisclosureConfig()
            self.polling = MockPollingConfig()
            self.health = MockHealthConfig()
    
    return MockAppConfig()
