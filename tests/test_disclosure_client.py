"""
Disclosure Service Client Tests

Consumer의 DisclosureServiceClient 기능 테스트
"""

import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Consumer 모듈 경로 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'consumer'))


class TestDisclosureServiceClient:
    """DisclosureServiceClient 테스트"""
    
    def test_client_initialization(self):
        """클라이언트 초기화"""
        from tasks import DisclosureServiceClient
        
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key",
            timeout=30,
            max_retries=3
        )
        
        assert client.base_url == "http://localhost:8000"
        assert client.api_key == "test-api-key"
        assert client.timeout == 30
        assert client.max_retries == 3
    
    def test_base_url_trailing_slash_removal(self):
        """base_url 끝의 슬래시 제거"""
        from tasks import DisclosureServiceClient
        
        client = DisclosureServiceClient(
            base_url="http://localhost:8000/",
            api_key="test-api-key"
        )
        
        assert client.base_url == "http://localhost:8000"
    
    def test_get_headers(self):
        """요청 헤더 생성"""
        from tasks import DisclosureServiceClient
        
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key"
        )
        
        headers = client._get_headers()
        
        assert headers["Content-Type"] == "application/json"
        assert headers["X-Worker-API-Key"] == "test-api-key"
    
    def test_missing_api_key_warning(self, caplog):
        """API Key 누락 시 경고"""
        from tasks import DisclosureServiceClient
        import logging
        
        with caplog.at_level(logging.WARNING):
            client = DisclosureServiceClient(
                base_url="http://localhost:8000",
                api_key=""
            )
        
        assert "WORKER_API_KEY is not set" in caplog.text
    
    @patch("httpx.Client")
    def test_upsert_disclosure_success(self, mock_client_class, sample_disclosure_message):
        """공시 저장 성공"""
        from tasks import DisclosureServiceClient
        
        # Mock 설정
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"rcept_no": sample_disclosure_message["rcept_no"]}
        mock_response.raise_for_status = MagicMock()
        mock_client.put.return_value = mock_response
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        
        # 테스트
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key"
        )
        
        result = client.upsert_disclosure(
            sample_disclosure_message["rcept_no"],
            sample_disclosure_message
        )
        
        assert result["rcept_no"] == sample_disclosure_message["rcept_no"]
        mock_client.put.assert_called_once()
    
    @patch("httpx.Client")
    def test_upsert_disclosure_failure(self, mock_client_class, sample_disclosure_message):
        """공시 저장 실패 (5xx 에러)"""
        from tasks import DisclosureServiceClient
        import httpx
        
        # Mock 설정 - 5xx 에러
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        
        error = httpx.HTTPStatusError(
            "Server Error",
            request=MagicMock(),
            response=mock_response
        )
        mock_response.raise_for_status.side_effect = error
        mock_client.put.return_value = mock_response
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        
        # 테스트
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key",
            max_retries=1  # 빠른 테스트를 위해 1회만
        )
        
        with pytest.raises(httpx.HTTPStatusError):
            client.upsert_disclosure(
                sample_disclosure_message["rcept_no"],
                sample_disclosure_message
            )
    
    @patch("httpx.Client")
    def test_upsert_disclosure_auth_failure(self, mock_client_class, sample_disclosure_message):
        """인증 실패 (4xx 에러는 재시도 안함)"""
        from tasks import DisclosureServiceClient
        import httpx
        
        # Mock 설정 - 401 응답
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        
        error = httpx.HTTPStatusError(
            "Unauthorized",
            request=MagicMock(),
            response=mock_response
        )
        mock_response.raise_for_status.side_effect = error
        mock_client.put.return_value = mock_response
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        
        # 테스트
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="wrong-key",
            max_retries=3
        )
        
        with pytest.raises(httpx.HTTPStatusError):
            client.upsert_disclosure(
                sample_disclosure_message["rcept_no"],
                sample_disclosure_message
            )
        
        # 4xx 에러는 재시도하지 않으므로 1번만 호출
        assert mock_client.put.call_count == 1


class TestPayloadBuilding:
    """Disclosure Service용 페이로드 생성 테스트"""
    
    @patch("httpx.Client")
    def test_payload_structure(self, mock_client_class, sample_disclosure_message):
        """페이로드 구조 확인"""
        from tasks import DisclosureServiceClient
        
        # Mock 설정
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()
        mock_client.put.return_value = mock_response
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key"
        )
        
        client.upsert_disclosure(
            sample_disclosure_message["rcept_no"],
            sample_disclosure_message
        )
        
        # PUT 호출 시 전달된 payload 확인
        call_args = mock_client.put.call_args
        payload = call_args.kwargs.get("json") or call_args[1].get("json")
        
        # 필수 필드 확인
        assert payload["reportName"] == sample_disclosure_message["report_nm"]
        assert payload["corpCode"] == sample_disclosure_message["corp_code"]
        assert payload["corpName"] == sample_disclosure_message["corp_name"]
        assert payload["minioObjectName"] == sample_disclosure_message["object_key"]
        assert payload["receptionDate"] == "2024-12-29T00:00:00Z"


class TestRetryLogic:
    """재시도 로직 테스트"""
    
    @patch("httpx.Client")
    @patch("time.sleep")  # 테스트 속도를 위해 sleep mock
    def test_retry_on_network_error(self, mock_sleep, mock_client_class, sample_disclosure_message):
        """네트워크 에러 시 재시도"""
        from tasks import DisclosureServiceClient
        import httpx
        
        # 첫 번째 호출은 실패, 두 번째는 성공
        mock_client = MagicMock()
        mock_success_response = MagicMock()
        mock_success_response.status_code = 201
        mock_success_response.json.return_value = {"rcept_no": sample_disclosure_message["rcept_no"]}
        mock_success_response.raise_for_status = MagicMock()
        
        call_count = [0]
        
        def side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise httpx.ConnectError("Connection refused")
            return mock_success_response
        
        mock_client.put.side_effect = side_effect
        mock_client_class.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_class.return_value.__exit__ = MagicMock(return_value=False)
        
        # 테스트
        client = DisclosureServiceClient(
            base_url="http://localhost:8000",
            api_key="test-api-key",
            max_retries=3
        )
        
        result = client.upsert_disclosure(
            sample_disclosure_message["rcept_no"],
            sample_disclosure_message
        )
        
        # 재시도 후 성공
        assert call_count[0] == 2
        assert result["rcept_no"] == sample_disclosure_message["rcept_no"]
