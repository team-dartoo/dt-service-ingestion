"""
Mock DART API Client

DART API Key 없이 서비스 테스트를 위한 Mock 클라이언트.
실제 API 호출 대신 가짜 공시 데이터를 생성합니다.

사용법:
    MOCK_MODE=true 환경변수 설정 시 자동 활성화
"""

import logging
import random
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


# 샘플 회사 데이터
SAMPLE_COMPANIES = [
    {"corp_code": "00126380", "corp_name": "삼성전자", "stock_code": "005930", "corp_cls": "Y"},
    {"corp_code": "00164742", "corp_name": "SK하이닉스", "stock_code": "000660", "corp_cls": "Y"},
    {"corp_code": "00356370", "corp_name": "네이버", "stock_code": "035420", "corp_cls": "Y"},
    {"corp_code": "00401731", "corp_name": "카카오", "stock_code": "035720", "corp_cls": "Y"},
    {"corp_code": "00155246", "corp_name": "현대자동차", "stock_code": "005380", "corp_cls": "Y"},
    {"corp_code": "00104752", "corp_name": "LG에너지솔루션", "stock_code": "373220", "corp_cls": "Y"},
    {"corp_code": "00164779", "corp_name": "셀트리온", "stock_code": "068270", "corp_cls": "K"},
    {"corp_code": "00155319", "corp_name": "기아", "stock_code": "000270", "corp_cls": "Y"},
    {"corp_code": "00126186", "corp_name": "포스코홀딩스", "stock_code": "005490", "corp_cls": "Y"},
    {"corp_code": "00547583", "corp_name": "삼성바이오로직스", "stock_code": "207940", "corp_cls": "Y"},
]

# 샘플 보고서 유형
SAMPLE_REPORT_TYPES = [
    "사업보고서",
    "반기보고서", 
    "분기보고서",
    "주요사항보고서",
    "임원ㆍ주요주주특정증권등소유상황보고서",
    "최대주주등소유주식변동신고서",
    "기업설명회(IR)개최",
    "공정공시",
    "자기주식취득결정",
    "유상증자결정",
    "무상증자결정",
    "전환사채권발행결정",
]

# 샘플 비고
SAMPLE_REMARKS = ["", "유", "코", "채", "넥", "공", "연", "정", "철"]


class MockDartApiClient:
    """
    Mock DART API Client.
    
    실제 API 호출 없이 테스트용 가짜 데이터를 생성합니다.
    DartApiClient와 동일한 인터페이스를 제공합니다.
    """
    
    def __init__(self, api_key: str = "", timeout: int = 30):
        """
        Mock 클라이언트 초기화.
        
        Args:
            api_key: 무시됨 (Mock 모드)
            timeout: 무시됨 (Mock 모드)
        """
        self.api_key = api_key
        self.timeout = timeout
        self._generated_rcept_nos = set()
        logger.info("🧪 MockDartApiClient initialized - using fake data")
    
    def _generate_rcept_no(self, date_str: str) -> str:
        """고유한 접수번호 생성"""
        while True:
            # 형식: YYYYMMDD + 6자리 순번
            seq = random.randint(100000, 999999)
            rcept_no = f"{date_str}{seq}"
            if rcept_no not in self._generated_rcept_nos:
                self._generated_rcept_nos.add(rcept_no)
                return rcept_no

    def fetch_disclosures(
        self,
        date: str,
        page_no: int = 1,
        page_count: int = 100,
        corp_code: Optional[str] = None,
        corp_cls: Optional[str] = None,
        pblntf_ty: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        실제 DartApiClient와 동일한 인터페이스.

        Note:
            page_no, page_count, corp_code, corp_cls, pblntf_ty는 Mock에서 미사용.
        """
        disclosures = self.fetch_disclosure_list(date)
        return {
            "status": "000",
            "total_count": len(disclosures),
            "total_page": 1,
            "list": disclosures,
        }

    def fetch_document_content(self, rcept_no: str) -> Optional[bytes]:
        """
        실제 DartApiClient와 동일한 인터페이스.
        """
        return self.download_document(rcept_no)
    
    def fetch_disclosure_list(self, target_date: str) -> List[Dict[str, Any]]:
        """
        가짜 공시 목록 생성.
        
        Args:
            target_date: 대상 날짜 (YYYYMMDD)
            
        Returns:
            가짜 공시 목록
        """
        logger.info(f"🧪 [MOCK] Generating fake disclosures for date: {target_date}")
        
        # 시뮬레이션 딜레이
        time.sleep(0.5)
        
        # 3~8개의 가짜 공시 생성
        num_disclosures = random.randint(3, 8)
        disclosures = []
        
        for _ in range(num_disclosures):
            company = random.choice(SAMPLE_COMPANIES)
            report_type = random.choice(SAMPLE_REPORT_TYPES)
            remark = random.choice(SAMPLE_REMARKS)
            
            disclosure = {
                "corp_code": company["corp_code"],
                "corp_name": company["corp_name"],
                "stock_code": company["stock_code"],
                "corp_cls": company["corp_cls"],
                "report_nm": report_type,
                "rcept_no": self._generate_rcept_no(target_date),
                "flr_nm": company["corp_name"],
                "rcept_dt": target_date,
                "rm": remark,
            }
            disclosures.append(disclosure)
        
        logger.info(f"🧪 [MOCK] Generated {len(disclosures)} fake disclosures")
        return disclosures
    
    def download_document(self, rcept_no: str) -> bytes:
        """
        가짜 문서 내용 생성.
        
        Args:
            rcept_no: 접수번호
            
        Returns:
            가짜 HTML 문서 바이트
        """
        logger.info(f"🧪 [MOCK] Generating fake document for rcept_no: {rcept_no}")
        
        # 시뮬레이션 딜레이
        time.sleep(0.3)
        
        # 가짜 HTML 문서 생성
        html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>공시 문서 - {rcept_no}</title>
</head>
<body>
    <h1>공시 문서</h1>
    <p>접수번호: {rcept_no}</p>
    <p>생성일시: {datetime.now().isoformat()}</p>
    <hr>
    <h2>본문</h2>
    <p>이 문서는 테스트를 위해 자동 생성된 Mock 데이터입니다.</p>
    <p>실제 DART API 연동 시에는 실제 공시 문서가 표시됩니다.</p>
    <hr>
    <h2>재무정보 (샘플)</h2>
    <table border="1">
        <tr><th>항목</th><th>금액</th></tr>
        <tr><td>자산총계</td><td>{random.randint(1000, 9999):,}억원</td></tr>
        <tr><td>매출액</td><td>{random.randint(100, 999):,}억원</td></tr>
        <tr><td>영업이익</td><td>{random.randint(10, 99):,}억원</td></tr>
    </table>
    <hr>
    <footer>
        <p>🧪 이 문서는 MOCK_MODE에서 생성되었습니다.</p>
    </footer>
</body>
</html>"""
        
        return html_content.encode('utf-8')


def get_dart_client(config):
    """
    설정에 따라 적절한 DART 클라이언트 반환.
    
    Args:
        config: AppConfig 또는 DartApiConfig
        
    Returns:
        DartApiClient 또는 MockDartApiClient
    """
    # config가 AppConfig인 경우
    if hasattr(config, 'dart'):
        dart_config = config.dart
    else:
        dart_config = config
    
    if dart_config.mock_mode:
        logger.info("🧪 Using MockDartApiClient (MOCK_MODE=true)")
        return MockDartApiClient(
            api_key=dart_config.api_key,
            timeout=dart_config.timeout
        )
    else:
        # 실제 클라이언트 import 및 반환
        from services.dart_api_client import DartApiClient
        logger.info("🔗 Using real DartApiClient")
        return DartApiClient(
            api_key=dart_config.api_key,
            timeout=dart_config.timeout
        )
