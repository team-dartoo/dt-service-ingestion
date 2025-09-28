import os
import json
import logging
from datetime import datetime
from dataclasses import asdict
from models.disclosure import Disclosure

LOG = logging.getLogger(__name__)

class FailureRecorder:

    def __init__(self, log_dir: str | None):

        if log_dir:
            try:
                os.makedirs(log_dir, exist_ok=True)
                self.log_dir = log_dir
                LOG.info(f"Failure recorder is enabled. Logs will be saved to: {log_dir}")
            except OSError as e:
                LOG.error(f"Failed to create failure log directory {log_dir}: {e}")
                self.log_dir = None
        else:
            self.log_dir = None
            LOG.warning("FAILED_LOG_DIR is not set. Failure recording is disabled.")

    def record(self, doc: Disclosure, reason: str):

        if not self.log_dir:
            return

        try:
            failure_data = {
                "recorded_at": datetime.now().isoformat(),
                "failure_reason": reason,
                "disclosure_details": asdict(doc)
            }
            file_path = os.path.join(self.log_dir, f"{doc.rcept_no}.json")
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(failure_data, f, ensure_ascii=False, indent=4)
                
        except Exception as e:
            LOG.error(f"Could not record failure for rcept_no {doc.rcept_no}: {e}")
