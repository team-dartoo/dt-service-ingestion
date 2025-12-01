# Consumer Worker Configuration
import os
import logging
from celery import Celery

# -------------------- 로깅 설정 --------------------
log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
numeric_level = getattr(logging, log_level_name, logging.INFO)

logging.basicConfig(
    level=numeric_level,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------- Celery 브로커 설정 --------------------
default_broker = "amqp://admin:password@rabbitmq:5672/"
broker_url = os.getenv("CELERY_BROKER_URL", default_broker)

logger.info(f"Starting Celery worker with broker: {broker_url}")

app = Celery(
    "consumer",
    broker=broker_url,
    include=["tasks"],  # tasks.py 에 정의된 태스크들을 로드
)

# -------------------- Celery 공통 설정 --------------------
# - JSON 직렬화만 허용
# - 타임존은 Asia/Seoul 기준 사용
# - enable_utc=False 로 설정해 로컬 타임존 기준으로 동작
# - worker_prefetch_multiplier=1 로 한 번에 하나의 태스크만 선점
# - task_acks_late=True 로 작업 완료 후에 ack 전송
app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Seoul",
    enable_utc=False,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)