import os
import json
import logging
from datetime import datetime
from dataclasses import asdict
from models.disclosure import Disclosure 

LOG = logging.getLogger(__name__)

# -------------------- 데이터 처리 실패 시, 상세 내용을 별도의 JSON 파일로 영구 저장하는 클래스 --------------------
class FailureRecorder:
    
    # ----------생성자: 실패 로그를 저장할 디렉토리를 설정하고, 없으면 생성----------
    def __init__(self, log_dir: str | None):
        if log_dir:                                                                         # 로그 디렉토리 경로가 제공되었는지 확인
            try:
                os.makedirs(log_dir, exist_ok=True)                                         # 디렉토리가 없으면 생성 (이미 있어도 에러 없음)
                self.log_dir = log_dir
                LOG.info(f"Failure recorder is enabled. Logs will be saved to: {log_dir}")
            except OSError as e:                                                            # 디렉토리 생성 중 권한 등의 문제 발생 시
                LOG.error(f"Failed to create failure log directory {log_dir}: {e}")
                self.log_dir = None                                                         # 기능을 비활성화
        else:
            self.log_dir = None                                                             # 경로가 없으면 기능을 비활성화
            LOG.warning("FAILED_LOG_DIR is not set. Failure recording is disabled.")

    # ---------- 실제 실패 내용을 파일로 기록하는 메서드 ----------
    def record(self, doc: Disclosure, reason: str):
        
        if not self.log_dir:                                                                # 기능이 비활성화 상태이면 아무것도 하지 않고 즉시 종료
            return

        try:                                                                                # 저장할 데이터 구조화: 기록 시간, 실패 원인, 원본 공시 정보
            failure_data = {
                "recorded_at": datetime.now().isoformat(), 
                "failure_reason": reason,
                "disclosure_details": asdict(doc)
            }
            file_path = os.path.join(self.log_dir, f"{doc.rcept_no}.json")
            
            with open(file_path, 'w', encoding='utf-8') as f:                               # JSON 파일로 저장 (UTF-8 인코딩, 가독성을 위한 들여쓰기 적용)
                json.dump(failure_data, f, ensure_ascii=False, indent=4)
                
        except Exception as e:                                                              # 파일 쓰기 등 과정에서 예외 발생 시 에러 로그 기록
            LOG.error(f"Could not record failure for rcept_no {doc.rcept_no}: {e}")
