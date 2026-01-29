"""
Environment Configuration Validator

시작 시 필수 환경 변수를 검증하고 설정을 관리하는 모듈.
pydantic-settings를 사용하여 타입 검증과 기본값 관리를 수행한다.

[OOP 원칙 적용]
- SRP: 환경 변수 검증만 담당
- OCP: 새 설정 추가 시 확장 용이
- DIP: 설정 클래스를 통한 추상화
"""

import os
import sys
import logging
from typing import Optional, List
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """환경 변수 검증 실패 예외"""
    
    def __init__(self, missing: List[str], invalid: List[str]):
        self.missing = missing
        self.invalid = invalid
        
        messages = []
        if missing:
            messages.append(f"Missing required variables: {', '.join(missing)}")
        if invalid:
            messages.append(f"Invalid variable values: {', '.join(invalid)}")
        
        super().__init__(" | ".join(messages))


@dataclass
class DartApiConfig:
    """DART API 설정"""
    api_key: str
    base_url: str = "https://opendart.fss.or.kr/api"
    timeout: int = 30
    max_retries: int = 5
    mock_mode: bool = False  # Mock 모드 활성화
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        # Mock 모드일 때는 API Key 검증 스킵
        if self.mock_mode:
            return errors
        
        if not self.api_key:
            errors.append("DART_API_KEY is empty")
        elif len(self.api_key) != 40:
            errors.append(f"DART_API_KEY must be 40 characters (got {len(self.api_key)})")
        
        if self.timeout < 1:
            errors.append(f"DART_TIMEOUT must be positive (got {self.timeout})")
        
        return errors


@dataclass
class MinioConfig:
    """MinIO 스토리지 설정"""
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str = "dart-disclosures"
    secure: bool = False
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if not self.endpoint:
            errors.append("MINIO_ENDPOINT is empty")
        if not self.access_key:
            errors.append("MINIO_ACCESS_KEY is empty")
        if not self.secret_key:
            errors.append("MINIO_SECRET_KEY is empty")
        
        return errors


@dataclass
class CeleryConfig:
    """Celery/RabbitMQ 설정"""
    broker_url: str
    task_name: str = "tasks.process_disclosure"
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if not self.broker_url:
            errors.append("CELERY_BROKER_URL is empty")
        elif not self.broker_url.startswith(("amqp://", "redis://")):
            errors.append(f"CELERY_BROKER_URL must start with amqp:// or redis://")
        
        return errors


@dataclass
class DisclosureServiceConfig:
    """Disclosure Service 연동 설정"""
    base_url: str
    api_key: str
    timeout: int = 30
    max_retries: int = 3
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if not self.base_url:
            errors.append("DISCLOSURE_SERVICE_URL is empty")
        elif not self.base_url.startswith(("http://", "https://")):
            errors.append("DISCLOSURE_SERVICE_URL must start with http:// or https://")
        
        if not self.api_key:
            errors.append("WORKER_API_KEY is empty")
        elif len(self.api_key) < 16:
            errors.append(f"WORKER_API_KEY should be at least 16 characters for security")
        
        return errors

DEFAULT_FAILED_LOG_DIR = "/app/failed_logs"

@dataclass 
class PollingConfig:
    """폴링 설정"""
    interval_seconds: int = 300
    target_date: Optional[str] = None
    max_fail: int = 3
    failed_log_dir: Optional[str] = None
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if self.interval_seconds < 10:
            errors.append(f"POLL_INTERVAL must be at least 10 seconds (got {self.interval_seconds})")
        
        if self.target_date:
            if len(self.target_date) != 8 or not self.target_date.isdigit():
                errors.append(f"TARGET_DATE must be YYYYMMDD format (got {self.target_date})")
        
        if self.max_fail < 1:
            errors.append(f"MAX_FAIL must be positive (got {self.max_fail})")
        
        return errors


@dataclass
class HealthCheckConfig:
    """헬스체크 설정"""
    enabled: bool = True
    port: int = 8001
    host: str = "0.0.0.0"
    
    def validate(self) -> List[str]:
        """설정 유효성 검증"""
        errors = []
        
        if self.port < 1 or self.port > 65535:
            errors.append(f"HEALTH_PORT must be 1-65535 (got {self.port})")
        
        return errors


@dataclass
class AppConfig:
    """
    전체 애플리케이션 설정.
    
    모든 하위 설정을 통합하고 일괄 검증을 수행한다.
    """
    dart: DartApiConfig
    minio: MinioConfig
    celery: CeleryConfig
    disclosure: DisclosureServiceConfig
    polling: PollingConfig
    health: HealthCheckConfig
    log_level: str = "INFO"
    
    def validate_all(self) -> None:
        """
        모든 설정 검증.
        
        Raises:
            ConfigValidationError: 검증 실패 시
        """
        all_errors = []
        
        all_errors.extend(self.dart.validate())
        all_errors.extend(self.minio.validate())
        all_errors.extend(self.celery.validate())
        all_errors.extend(self.disclosure.validate())
        all_errors.extend(self.polling.validate())
        all_errors.extend(self.health.validate())
        
        if all_errors:
            raise ConfigValidationError(missing=[], invalid=all_errors)
    
    def to_dict(self) -> dict:
        """설정을 딕셔너리로 변환 (민감 정보 마스킹)"""
        return {
            "dart": {
                "api_key": self.dart.api_key[:8] + "..." if self.dart.api_key else None,
                "timeout": self.dart.timeout,
            },
            "minio": {
                "endpoint": self.minio.endpoint,
                "bucket_name": self.minio.bucket_name,
                "secure": self.minio.secure,
            },
            "celery": {
                "broker_url": self._mask_url(self.celery.broker_url),
            },
            "disclosure": {
                "base_url": self.disclosure.base_url,
                "timeout": self.disclosure.timeout,
            },
            "polling": {
                "interval_seconds": self.polling.interval_seconds,
                "target_date": self.polling.target_date,
                "max_fail": self.polling.max_fail,
                "failed_log_dir": self.polling.failed_log_dir,
            },
            "health": {
                "enabled": self.health.enabled,
                "port": self.health.port,
            },
            "log_level": self.log_level,
        }
    
    @staticmethod
    def _mask_url(url: str) -> str:
        """URL에서 비밀번호 마스킹"""
        if not url:
            return ""
        # amqp://user:password@host:port/ → amqp://user:***@host:port/
        if "@" in url and "://" in url:
            prefix, suffix = url.split("://", 1)
            if "@" in suffix:
                creds, host = suffix.rsplit("@", 1)
                if ":" in creds:
                    user, _ = creds.split(":", 1)
                    return f"{prefix}://{user}:***@{host}"
        return url


def _get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    """환경 변수 조회 (빈 문자열은 None 처리)"""
    value = os.getenv(key, default)
    if value is not None:
        value = value.strip()
        if value == "":
            return default
    return value


def _get_env_int(key: str, default: int) -> int:
    """정수형 환경 변수 조회"""
    value = _get_env(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid integer for {key}={value}, using default={default}")
        return default


def _get_env_bool(key: str, default: bool) -> bool:
    """불린형 환경 변수 조회"""
    value = _get_env(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def load_config() -> AppConfig:
    """
    환경 변수에서 설정 로드.
    
    Returns:
        AppConfig: 로드된 설정 객체
        
    Raises:
        ConfigValidationError: 필수 환경 변수 누락 시
    """
    # Mock 모드 확인
    mock_mode = _get_env_bool("MOCK_MODE", False)
    
    # 필수 환경 변수 확인 (Mock 모드일 때는 DART_API_KEY 제외)
    if mock_mode:
        required_vars = [
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY", 
            "MINIO_SECRET_KEY",
            "CELERY_BROKER_URL",
        ]
        logger.info("🧪 MOCK_MODE enabled - DART API Key not required")
    else:
        required_vars = [
            "DART_API_KEY",
            "MINIO_ENDPOINT",
            "MINIO_ACCESS_KEY", 
            "MINIO_SECRET_KEY",
            "CELERY_BROKER_URL",
        ]
    
    missing = [var for var in required_vars if not _get_env(var)]
    if missing:
        raise ConfigValidationError(missing=missing, invalid=[])
    
    failed_log_dir = _get_env("FAILED_LOG_DIR")
    if not failed_log_dir:
        failed_log_dir = DEFAULT_FAILED_LOG_DIR
        logger.info(f"FAILED_LOG_DIR not set, using default: {failed_log_dir}")
    
    # 설정 객체 생성
    config = AppConfig(
        dart=DartApiConfig(
            api_key=_get_env("DART_API_KEY", "mock-api-key-for-testing-only"),
            timeout=_get_env_int("DART_TIMEOUT", 30),
            max_retries=_get_env_int("DART_MAX_RETRIES", 5),
            mock_mode=mock_mode,
        ),
        minio=MinioConfig(
            endpoint=_get_env("MINIO_ENDPOINT", ""),
            access_key=_get_env("MINIO_ACCESS_KEY", ""),
            secret_key=_get_env("MINIO_SECRET_KEY", ""),
            bucket_name=_get_env("MINIO_BUCKET", "dart-disclosures"),
            secure=_get_env_bool("MINIO_SECURE", False),
        ),
        celery=CeleryConfig(
            broker_url=_get_env("CELERY_BROKER_URL", ""),
        ),
        disclosure=DisclosureServiceConfig(
            base_url=_get_env("DISCLOSURE_SERVICE_URL", "http://disclosure-service:8000"),
            api_key=_get_env("WORKER_API_KEY", ""),
            timeout=_get_env_int("REQUEST_TIMEOUT", 30),
            max_retries=_get_env_int("MAX_RETRIES", 3),
        ),
        polling=PollingConfig(
            interval_seconds=_get_env_int("POLL_INTERVAL", 300),
            target_date=_get_env("TARGET_DATE"),
            max_fail=_get_env_int("MAX_FAIL", 3),
            failed_log_dir=failed_log_dir,
        ),
        health=HealthCheckConfig(
            enabled=_get_env_bool("HEALTH_ENABLED", True),
            port=_get_env_int("HEALTH_PORT", 8001),
            host=_get_env("HEALTH_HOST", "0.0.0.0"),
        ),
        log_level=_get_env("LOG_LEVEL", "INFO"),
    )
    
    return config


def validate_and_load() -> AppConfig:
    """
    설정 로드 및 검증 수행.
    
    실패 시 에러 메시지 출력 후 프로세스 종료.
    
    Returns:
        AppConfig: 검증된 설정 객체
    """
    try:
        config = load_config()
        config.validate_all()
        
        logger.info("Configuration loaded successfully")
        logger.debug(f"Config: {config.to_dict()}")
        
        return config
        
    except ConfigValidationError as e:
        logger.critical(f"Configuration validation failed: {e}")
        print(f"\n{'='*60}", file=sys.stderr)
        print("❌ CONFIGURATION ERROR", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)
        
        if e.missing:
            print("\nMissing required environment variables:", file=sys.stderr)
            for var in e.missing:
                print(f"  • {var}", file=sys.stderr)
        
        if e.invalid:
            print("\nInvalid configuration values:", file=sys.stderr)
            for error in e.invalid:
                print(f"  • {error}", file=sys.stderr)
        
        print(f"\n{'='*60}", file=sys.stderr)
        print("Please check your .env file or environment variables.", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)
        
        sys.exit(1)


# 모듈 레벨 싱글톤 (지연 로딩)
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """
    설정 싱글톤 반환.
    
    처음 호출 시 설정을 로드하고 검증한다.
    """
    global _config
    if _config is None:
        _config = validate_and_load()
    return _config
