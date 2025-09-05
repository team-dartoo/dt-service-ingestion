# Consumer Worker Configuration
from celery import Celery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery configuration for worker
app = Celery(
    'consumer',
    broker='amqp://admin:password@rabbitmq:5672/',
    include=['tasks']  # Import tasks from tasks.py
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    worker_prefetch_multiplier=1,  # Process one task at a time
    task_acks_late=True,           # Acknowledge after completion
)
