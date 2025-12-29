"""
Configuration Tests

Producer 설정 로드 및 검증 테스트
"""

import pytest
import os
import sys

# Producer 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))


class TestDartApiConfig:
    """DART API 설정 테스트"""
    
    def test_mock_mode_skips_api_key_validation(self):
        """Mock 모드에서 API Key 검증이 스킵되는지 확인"""
        from config import DartApiConfig
        
        config = DartApiConfig(
            api_key="",  # 빈 API Key
            mock_mode=True
        )
        
        errors = config.validate()
        assert errors == []  # 에러 없음
    
    def test_real_mode_requires_api_key(self):
        """실제 모드에서 API Key가 필수인지 확인"""
        from config import DartApiConfig
        
        config = DartApiConfig(
            api_key="",
            mock_mode=False
        )
        
        errors = config.validate()
        assert len(errors) > 0
        assert any("DART_API_KEY" in e for e in errors)
    
    def test_api_key_length_validation(self):
        """API Key 길이 검증 (40자)"""
        from config import DartApiConfig
        
        # 40자 미만
        config = DartApiConfig(
            api_key="short-key",
            mock_mode=False
        )
        
        errors = config.validate()
        assert any("40 characters" in e for e in errors)
    
    def test_valid_api_key(self):
        """유효한 API Key"""
        from config import DartApiConfig
        
        config = DartApiConfig(
            api_key="a" * 40,  # 정확히 40자
            mock_mode=False
        )
        
        errors = config.validate()
        assert errors == []


class TestMinioConfig:
    """MinIO 설정 테스트"""
    
    def test_minio_config_defaults(self):
        """MinIO 설정 기본값"""
        from config import MinioConfig
        
        config = MinioConfig(
            endpoint="localhost:9000",
            access_key="admin",
            secret_key="admin123"
        )
        
        assert config.bucket_name == "dart-disclosures"
        assert config.secure is False
    
    def test_minio_config_validation(self):
        """MinIO 설정 검증"""
        from config import MinioConfig
        
        config = MinioConfig(
            endpoint="localhost:9000",
            access_key="admin",
            secret_key="admin123",
            bucket_name="test-bucket"
        )
        
        errors = config.validate()
        assert errors == []


class TestDisclosureServiceConfig:
    """Disclosure Service 연동 설정 테스트"""
    
    def test_disclosure_config_defaults(self):
        """Disclosure Service 설정 기본값"""
        from config import DisclosureServiceConfig
        
        config = DisclosureServiceConfig(
            base_url="http://localhost:8000",
            api_key="test-key"
        )
        
        assert config.timeout == 30
        assert config.max_retries == 3
    
    def test_disclosure_config_validation(self):
        """Disclosure Service 설정 검증"""
        from config import DisclosureServiceConfig
        
        # API Key 없음
        config = DisclosureServiceConfig(
            base_url="http://localhost:8000",
            api_key=""
        )
        
        errors = config.validate()
        assert any("api_key" in e.lower() for e in errors)


class TestPollingConfig:
    """폴링 설정 테스트"""
    
    def test_polling_config_defaults(self):
        """폴링 설정 기본값"""
        from config import PollingConfig
        
        config = PollingConfig()
        
        assert config.interval_seconds == 300
        assert config.max_fail == 3
    
    def test_polling_interval_validation(self):
        """폴링 간격 검증"""
        from config import PollingConfig
        
        config = PollingConfig(interval_seconds=0)
        
        errors = config.validate()
        assert any("interval" in e.lower() for e in errors)


class TestConfigLoading:
    """설정 로드 테스트"""
    
    def test_load_config_with_mock_mode(self, monkeypatch):
        """Mock 모드로 설정 로드"""
        # 환경 변수 설정
        monkeypatch.setenv("MOCK_MODE", "true")
        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "admin")
        monkeypatch.setenv("MINIO_SECRET_KEY", "admin123")
        monkeypatch.setenv("CELERY_BROKER_URL", "amqp://localhost")
        
        from config import load_config
        
        config = load_config()
        
        assert config.dart.mock_mode is True
    
    def test_env_bool_parsing(self):
        """환경 변수 불리언 파싱"""
        from config import _get_env_bool
        
        # True 값들
        assert _get_env_bool("TEST", True) is True
        
        os.environ["TEST_BOOL"] = "true"
        assert _get_env_bool("TEST_BOOL", False) is True
        
        os.environ["TEST_BOOL"] = "1"
        assert _get_env_bool("TEST_BOOL", False) is True
        
        os.environ["TEST_BOOL"] = "yes"
        assert _get_env_bool("TEST_BOOL", False) is True
        
        # False 값들
        os.environ["TEST_BOOL"] = "false"
        assert _get_env_bool("TEST_BOOL", True) is False
        
        os.environ["TEST_BOOL"] = "0"
        assert _get_env_bool("TEST_BOOL", True) is False
        
        # 정리
        del os.environ["TEST_BOOL"]
