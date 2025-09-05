import uuid
import time
from celery import Celery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Celery configuration for producer
app = Celery(
    'producer',
    broker='amqp://admin:password@rabbitmq:5672/',
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

def mock_report_producer():
    """Start the UUID generator that sends tasks every 1-2 seconds"""
    logger.info("Starting UUID generator...")
    
    while True:
        try:
            # Generate UUID
            task_uuid = str(uuid.uuid4())
            logger.info(f"Generated UUID: {task_uuid}")
            
            # Send task to worker queue using the correct task name
            result = app.send_task('tasks.summarize_report', args=[task_uuid])
            logger.info(f"Task sent with ID: {result.id}")
            
            # Wait 1-2 seconds (random between 1000-2000ms)
            import random
            wait_time = random.uniform(1.0, 2.0)
            time.sleep(wait_time)
            
        except Exception as e:
            logger.error(f"Error in UUID generator: {e}")
            time.sleep(1)

if __name__ == "__main__":
    mock_report_producer()
