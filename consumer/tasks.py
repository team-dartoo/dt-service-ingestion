from worker import app
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


@app.task(name="tasks.summarize_report")
def summarize_report(
    corp_code: str,
    corp_name: str,
    report_nm: str,
    rcept_no: str,
    object_key: str,
    rcept_dt: str,
    polling_date: str,
):
    """
    Ingestion 서비스에서 전달한 공시 메타데이터를 기반으로 후속 작업을 수행하는 Celery Task.

    현재 단계에서는 실제 요약/분석 대신,
    - 메타데이터를 로그로 출력하고
    - 후속 Disclosure Service 개발을 위한 형태를 고정하는 데 목적이 있다.
    """
    start_ts = time.time()
    try:
        logger.info(
            "🔄 Worker received disclosure task | "
            "rcept_no=%s | corp=%s(%s) | report_nm=%s | object_key=%s | rcept_dt=%s | polled=%s",
            rcept_no,
            corp_name,
            corp_code,
            report_nm,
            object_key,
            rcept_dt,
            polling_date,
        )

        # TODO:
        #   - MinIO에서 object_key에 해당하는 파일을 다운로드
        #   - 공시 원문 파싱 / 요약 / 가공
        #   - 추후 Disclosure Service(PostgreSQL)로 저장/전달
        #
        # 현재는 파이프라인 구조만 잡기 위해 간단한 sleep으로 처리 시간을 흉내낸다.
        time.sleep(0.5)

        elapsed = time.time() - start_ts
        success_message = (
            f"✅ Successfully processed disclosure rcept_no={rcept_no} "
            f"for corp={corp_name}({corp_code}) "
            f"| object_key={object_key} | elapsed={elapsed:.3f}s"
        )
        logger.info(success_message)

        return {
            "status": "success",
            "corp_code": corp_code,
            "corp_name": corp_name,
            "report_nm": report_nm,
            "rcept_no": rcept_no,
            "object_key": object_key,
            "rcept_dt": rcept_dt,
            "polling_date": polling_date,
            "processed_at": time.time(),
            "message": success_message,
        }

    except Exception as e:
        elapsed = time.time() - start_ts
        error_message = (
            f"❌ Error processing disclosure rcept_no={rcept_no} "
            f"for corp={corp_name}({corp_code}) "
            f"| object_key={object_key} | elapsed={elapsed:.3f}s | error={e}"
        )
        logger.error(error_message, exc_info=True)

        return {
            "status": "error",
            "corp_code": corp_code,
            "corp_name": corp_name,
            "report_nm": report_nm,
            "rcept_no": rcept_no,
            "object_key": object_key,
            "rcept_dt": rcept_dt,
            "polling_date": polling_date,
            "processed_at": time.time(),
            "message": error_message,
        }