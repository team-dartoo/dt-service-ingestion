from worker import app
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='tasks.summarize_report')
def summarize_report(uuid_string):
    """Process the received UUID and return success message"""
    try:
        logger.info(f"🔄 Consumer received UUID: {uuid_string}")
        
        # Simulate some processing time
            # 추후 요약 로직이 포함되는 부분입니다. (load report from minIO & summarize)
        time.sleep(0.5)
        
        success_message = f"✅ Successfully processed UUID: {uuid_string}"
        logger.info(success_message)
        
        return {
            'status': 'success',
            'uuid': uuid_string,
            'message': success_message,
            'processed_at': time.time()
        }
        
    except Exception as e:
        error_message = f"❌ Error processing UUID {uuid_string}: {e}"
        logger.error(error_message)
        return {
            'status': 'error',
            'uuid': uuid_string,
            'message': error_message,
            'processed_at': time.time()
        }
    
