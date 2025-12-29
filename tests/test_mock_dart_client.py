"""
Mock DART API Client Tests

MockDartApiClient 기능 테스트
"""

import pytest
import os
import sys

# Producer 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'producer'))


class TestMockDartApiClient:
    """MockDartApiClient 테스트"""
    
    def test_mock_client_initialization(self):
        """Mock 클라이언트 초기화"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient(api_key="test", timeout=30)
        
        assert client.api_key == "test"
        assert client.timeout == 30
    
    def test_fetch_disclosure_list_returns_list(self):
        """공시 목록 조회가 리스트를 반환하는지 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        result = client.fetch_disclosure_list("20241229")
        
        assert isinstance(result, list)
        assert len(result) >= 3  # 최소 3개
        assert len(result) <= 8  # 최대 8개
    
    def test_fetch_disclosure_list_structure(self):
        """공시 목록의 구조 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        result = client.fetch_disclosure_list("20241229")
        
        # 필수 필드 확인
        required_fields = [
            "corp_code", "corp_name", "stock_code", "corp_cls",
            "report_nm", "rcept_no", "flr_nm", "rcept_dt"
        ]
        
        for disclosure in result:
            for field in required_fields:
                assert field in disclosure, f"Missing field: {field}"
    
    def test_fetch_disclosure_list_date_in_rcept_no(self):
        """rcept_no에 대상 날짜가 포함되는지 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        target_date = "20241229"
        result = client.fetch_disclosure_list(target_date)
        
        for disclosure in result:
            assert disclosure["rcept_no"].startswith(target_date)
    
    def test_download_document_returns_bytes(self):
        """문서 다운로드가 bytes를 반환하는지 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        result = client.download_document("20241229000001")
        
        assert isinstance(result, bytes)
    
    def test_download_document_is_html(self):
        """다운로드된 문서가 HTML 형식인지 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        result = client.download_document("20241229000001")
        content = result.decode('utf-8')
        
        assert "<!DOCTYPE html>" in content or "<html" in content
    
    def test_download_document_contains_rcept_no(self):
        """다운로드된 문서에 rcept_no가 포함되는지 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        rcept_no = "20241229000001"
        result = client.download_document(rcept_no)
        content = result.decode('utf-8')
        
        assert rcept_no in content
    
    def test_unique_rcept_no_generation(self):
        """고유한 rcept_no 생성 확인"""
        from services.mock_dart_client import MockDartApiClient
        
        client = MockDartApiClient()
        
        # 여러 번 호출
        result1 = client.fetch_disclosure_list("20241229")
        result2 = client.fetch_disclosure_list("20241229")
        
        rcept_nos_1 = {d["rcept_no"] for d in result1}
        rcept_nos_2 = {d["rcept_no"] for d in result2}
        
        # 두 결과의 rcept_no가 겹치지 않아야 함
        assert rcept_nos_1.isdisjoint(rcept_nos_2)


class TestGetDartClient:
    """get_dart_client 함수 테스트"""
    
    def test_returns_mock_client_when_mock_mode(self, mock_config):
        """Mock 모드일 때 MockDartApiClient 반환"""
        from services.mock_dart_client import get_dart_client, MockDartApiClient
        
        mock_config.dart.mock_mode = True
        client = get_dart_client(mock_config)
        
        assert isinstance(client, MockDartApiClient)
    
    def test_returns_real_client_when_not_mock_mode(self, mock_config):
        """Mock 모드가 아닐 때 DartApiClient 반환"""
        from services.mock_dart_client import get_dart_client
        from services.dart_api_client import DartApiClient
        
        mock_config.dart.mock_mode = False
        mock_config.dart.api_key = "a" * 40
        client = get_dart_client(mock_config)
        
        assert isinstance(client, DartApiClient)


class TestSampleData:
    """샘플 데이터 테스트"""
    
    def test_sample_companies_exist(self):
        """샘플 회사 데이터 존재"""
        from services.mock_dart_client import SAMPLE_COMPANIES
        
        assert len(SAMPLE_COMPANIES) >= 10
        
        # 주요 회사 확인
        corp_names = [c["corp_name"] for c in SAMPLE_COMPANIES]
        assert "삼성전자" in corp_names
        assert "SK하이닉스" in corp_names
    
    def test_sample_report_types_exist(self):
        """샘플 보고서 유형 존재"""
        from services.mock_dart_client import SAMPLE_REPORT_TYPES
        
        assert len(SAMPLE_REPORT_TYPES) >= 10
        
        # 주요 보고서 유형 확인
        assert "사업보고서" in SAMPLE_REPORT_TYPES
        assert "분기보고서" in SAMPLE_REPORT_TYPES
