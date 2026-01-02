"""
Health Check HTTP Server

Producer 프로세스의 헬스 상태를 HTTP 엔드포인트로 제공하는 모듈.
별도 스레드에서 실행되어 메인 폴링 루프와 독립적으로 동작한다.

[엔드포인트]
- GET /health      : 기본 상태 확인
- GET /health/live : Liveness probe (프로세스 생존)
- GET /health/ready: Readiness probe (의존성 연결 상태)
- GET /metrics     : 간단한 메트릭 정보

[OOP 원칙 적용]
- SRP: 헬스체크 HTTP 서버만 담당
- OCP: 새 헬스체크 항목 추가 용이
- DIP: HealthChecker 인터페이스를 통한 추상화
"""

import json
import threading
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, field
from datetime import datetime
from abc import ABC, abstractmethod
from enum import Enum

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """헬스 상태 열거형"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class CheckResult:
    """개별 헬스체크 결과"""
    name: str
    status: HealthStatus
    message: str = ""
    latency_ms: float = 0.0
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status.value,
            "message": self.message,
            "latency_ms": round(self.latency_ms, 2),
            "details": self.details,
        }


class HealthChecker(ABC):
    """
    헬스체크 인터페이스 (추상 클래스).
    
    새로운 헬스체크 항목 추가 시 이 클래스를 상속받아 구현한다.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """체크 항목 이름"""
        pass
    
    @abstractmethod
    def check(self) -> CheckResult:
        """헬스체크 수행"""
        pass


class RabbitMQHealthChecker(HealthChecker):
    """RabbitMQ 연결 상태 체크"""
    
    def __init__(self, broker_url: str):
        self._broker_url = broker_url
        self._connected = False
    
    @property
    def name(self) -> str:
        return "rabbitmq"
    
    def set_connected(self, connected: bool):
        """연결 상태 업데이트 (외부에서 호출)"""
        self._connected = connected
    
    def check(self) -> CheckResult:
        import time
        start = time.time()
        
        if self._connected:
            return CheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message="Connected to RabbitMQ",
                latency_ms=(time.time() - start) * 1000,
            )
        else:
            return CheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message="Not connected to RabbitMQ",
                latency_ms=(time.time() - start) * 1000,
            )


class MinIOHealthChecker(HealthChecker):
    """MinIO 연결 상태 체크"""
    
    def __init__(self, client=None):
        self._client = client
    
    def set_client(self, client):
        """클라이언트 설정 (지연 초기화)"""
        self._client = client
    
    @property
    def name(self) -> str:
        return "minio"
    
    def check(self) -> CheckResult:
        import time
        start = time.time()
        
        if self._client is None:
            return CheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message="MinIO client not initialized",
                latency_ms=(time.time() - start) * 1000,
            )
        
        try:
            # 버킷 목록 조회로 연결 확인
            buckets = list(self._client.client.list_buckets())
            return CheckResult(
                name=self.name,
                status=HealthStatus.HEALTHY,
                message=f"Connected, {len(buckets)} bucket(s) accessible",
                latency_ms=(time.time() - start) * 1000,
                details={"bucket_count": len(buckets)},
            )
        except Exception as e:
            return CheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Connection failed: {str(e)[:100]}",
                latency_ms=(time.time() - start) * 1000,
            )


class DartApiHealthChecker(HealthChecker):
    """DART API 상태 체크"""
    
    def __init__(self, client=None):
        self._client = client
        self._last_success: Optional[datetime] = None
        self._consecutive_failures = 0
    
    def set_client(self, client):
        """클라이언트 설정"""
        self._client = client
    
    def record_success(self):
        """성공 기록"""
        self._last_success = datetime.now()
        self._consecutive_failures = 0
    
    def record_failure(self):
        """실패 기록"""
        self._consecutive_failures += 1
    
    @property
    def name(self) -> str:
        return "dart_api"
    
    def check(self) -> CheckResult:
        import time
        start = time.time()
        
        # 최근 성공 여부로 판단 (실제 API 호출은 하지 않음)
        if self._last_success is None:
            status = HealthStatus.DEGRADED
            message = "No successful API call yet"
        elif self._consecutive_failures > 5:
            status = HealthStatus.UNHEALTHY
            message = f"Too many consecutive failures: {self._consecutive_failures}"
        elif self._consecutive_failures > 0:
            status = HealthStatus.DEGRADED
            message = f"Recent failures: {self._consecutive_failures}"
        else:
            status = HealthStatus.HEALTHY
            message = f"Last success: {self._last_success.isoformat()}"
        
        return CheckResult(
            name=self.name,
            status=status,
            message=message,
            latency_ms=(time.time() - start) * 1000,
            details={
                "last_success": self._last_success.isoformat() if self._last_success else None,
                "consecutive_failures": self._consecutive_failures,
            },
        )


class PollingHealthChecker(HealthChecker):
    """폴링 프로세스 상태 체크"""
    
    def __init__(self):
        self._is_running = False
        self._last_poll: Optional[datetime] = None
        self._processed_count = 0
        self._error_count = 0
    
    @property
    def name(self) -> str:
        return "polling"
    
    def set_running(self, running: bool):
        self._is_running = running
    
    def record_poll(self):
        self._last_poll = datetime.now()
    
    def record_processed(self, count: int = 1):
        self._processed_count += count
    
    def record_error(self, count: int = 1):
        self._error_count += count
    
    def check(self) -> CheckResult:
        import time
        start = time.time()
        
        if not self._is_running:
            return CheckResult(
                name=self.name,
                status=HealthStatus.UNHEALTHY,
                message="Polling loop is not running",
                latency_ms=(time.time() - start) * 1000,
            )
        
        return CheckResult(
            name=self.name,
            status=HealthStatus.HEALTHY,
            message="Polling loop is active",
            latency_ms=(time.time() - start) * 1000,
            details={
                "last_poll": self._last_poll.isoformat() if self._last_poll else None,
                "processed_count": self._processed_count,
                "error_count": self._error_count,
            },
        )


@dataclass
class HealthAggregator:
    """
    여러 헬스체커를 통합하여 전체 상태를 판단.
    
    [집계 규칙]
    - 모든 체커가 HEALTHY → HEALTHY
    - 하나라도 UNHEALTHY → UNHEALTHY
    - 그 외 → DEGRADED
    """
    
    checkers: List[HealthChecker] = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.now)
    
    def add_checker(self, checker: HealthChecker):
        """헬스체커 추가"""
        self.checkers.append(checker)
    
    def check_all(self) -> Dict[str, Any]:
        """모든 체커 실행 및 결과 집계"""
        results = [checker.check() for checker in self.checkers]
        
        # 전체 상태 결정
        has_unhealthy = any(r.status == HealthStatus.UNHEALTHY for r in results)
        has_degraded = any(r.status == HealthStatus.DEGRADED for r in results)
        
        if has_unhealthy:
            overall_status = HealthStatus.UNHEALTHY
        elif has_degraded:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        return {
            "status": overall_status.value,
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "checks": {r.name: r.to_dict() for r in results},
        }
    
    def is_ready(self) -> bool:
        """Readiness 체크 (모든 필수 서비스 연결됨)"""
        for checker in self.checkers:
            result = checker.check()
            if result.status == HealthStatus.UNHEALTHY:
                return False
        return True
    
    def is_alive(self) -> bool:
        """Liveness 체크 (프로세스 생존)"""
        # 폴링 체커가 있으면 확인
        for checker in self.checkers:
            if isinstance(checker, PollingHealthChecker):
                return checker._is_running
        return True


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP 요청 핸들러"""
    
    # 클래스 변수로 aggregator 공유
    aggregator: Optional[HealthAggregator] = None
    
    def log_message(self, format, *args):
        """HTTP 로그를 Python 로거로 리다이렉트"""
        logger.debug(f"Health check request: {format % args}")
    
    def _send_json(self, status_code: int, data: Dict[str, Any]):
        """JSON 응답 전송"""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))
    
    def do_GET(self):
        """GET 요청 처리"""
        if self.aggregator is None:
            self._send_json(503, {"error": "Health aggregator not initialized"})
            return
        
        if self.path == "/health" or self.path == "/":
            # 전체 헬스 상태
            result = self.aggregator.check_all()
            status_code = 200 if result["status"] != "unhealthy" else 503
            self._send_json(status_code, result)
        
        elif self.path == "/health/live":
            # Liveness probe
            is_alive = self.aggregator.is_alive()
            self._send_json(
                200 if is_alive else 503,
                {
                    "status": "alive" if is_alive else "dead",
                    "timestamp": datetime.now().isoformat(),
                }
            )
        
        elif self.path == "/health/ready":
            # Readiness probe
            is_ready = self.aggregator.is_ready()
            self._send_json(
                200 if is_ready else 503,
                {
                    "status": "ready" if is_ready else "not_ready",
                    "timestamp": datetime.now().isoformat(),
                }
            )
        
        elif self.path == "/metrics":
            # 간단한 메트릭
            result = self.aggregator.check_all()
            metrics = {
                "service": "ingestion-producer",
                "uptime_seconds": result["uptime_seconds"],
                "status": result["status"],
                "checks": {
                    name: check["status"]
                    for name, check in result["checks"].items()
                },
            }
            
            # 폴링 메트릭 추가
            for checker in self.aggregator.checkers:
                if isinstance(checker, PollingHealthChecker):
                    metrics["polling"] = {
                        "processed_count": checker._processed_count,
                        "error_count": checker._error_count,
                    }
            
            self._send_json(200, metrics)
        
        else:
            self._send_json(404, {"error": f"Not found: {self.path}"})


class HealthCheckServer:
    """
    헬스체크 HTTP 서버.
    
    별도 스레드에서 실행되어 메인 프로세스와 독립적으로 동작한다.
    """
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8001):
        self.host = host
        self.port = port
        self.aggregator = HealthAggregator()
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        
        # 헬스체커들
        self.rabbitmq_checker = RabbitMQHealthChecker("")
        self.minio_checker = MinIOHealthChecker()
        self.dart_checker = DartApiHealthChecker()
        self.polling_checker = PollingHealthChecker()
        
        # 체커 등록
        self.aggregator.add_checker(self.rabbitmq_checker)
        self.aggregator.add_checker(self.minio_checker)
        self.aggregator.add_checker(self.dart_checker)
        self.aggregator.add_checker(self.polling_checker)
    
    def start(self):
        """서버 시작 (백그라운드 스레드)"""
        if self._running:
            logger.warning("Health check server is already running")
            return
        
        # 핸들러에 aggregator 설정
        HealthCheckHandler.aggregator = self.aggregator
        
        try:
            self._server = HTTPServer((self.host, self.port), HealthCheckHandler)
            self._running = True
            
            self._thread = threading.Thread(
                target=self._serve_forever,
                name="HealthCheckServer",
                daemon=True,
            )
            self._thread.start()
            
            logger.info(f"Health check server started on http://{self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start health check server: {e}")
            raise
    
    def _serve_forever(self):
        """서버 루프"""
        while self._running and self._server:
            try:
                self._server.handle_request()
            except Exception as e:
                if self._running:
                    logger.error(f"Health check server error: {e}")
    
    def stop(self):
        """서버 중지"""
        self._running = False
        
        if self._server:
            self._server.shutdown()
            self._server = None
        
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        
        logger.info("Health check server stopped")
    
    # 상태 업데이트 메서드들
    def set_rabbitmq_connected(self, connected: bool):
        self.rabbitmq_checker.set_connected(connected)
    
    def set_minio_client(self, client):
        self.minio_checker.set_client(client)
    
    def set_dart_client(self, client):
        self.dart_checker.set_client(client)
    
    def set_polling_running(self, running: bool):
        self.polling_checker.set_running(running)
    
    def record_poll(self):
        self.polling_checker.record_poll()
    
    def record_processed(self, count: int = 1):
        self.polling_checker.record_processed(count)
    
    def record_error(self, count: int = 1):
        self.polling_checker.record_error(count)
    
    def record_dart_success(self):
        self.dart_checker.record_success()
    
    def record_dart_failure(self):
        self.dart_checker.record_failure()


# 모듈 레벨 싱글톤
_health_server: Optional[HealthCheckServer] = None


def get_health_server() -> HealthCheckServer:
    """헬스체크 서버 싱글톤 반환"""
    global _health_server
    if _health_server is None:
        _health_server = HealthCheckServer()
    return _health_server


def start_health_server(host: str = "0.0.0.0", port: int = 8001) -> HealthCheckServer:
    """헬스체크 서버 시작"""
    global _health_server
    _health_server = HealthCheckServer(host=host, port=port)
    _health_server.start()
    return _health_server
