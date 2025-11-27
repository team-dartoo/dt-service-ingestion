from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass 
# -------------------- DART API의 단일 공시 정보를 구조화하고 유효성을 보장하는 데이터 클래스 --------------------
class Disclosure:
    """
    DART Open API list.json 응답의 개별 공시 항목을 표현하는 데이터 클래스.
    
    모든 필드는 DART API 명세에 따라 STRING 타입으로 반환되므로,
    타입 변환 없이 문자열 그대로 저장한다.
    
    Attributes:
        corp_code: 기업 고유 코드 (8자리, 0패딩)
        corp_name: 기업명 (상장사는 종목명, 비상장사는 법인명)
        stock_code: 주식 티커 코드 (6자리, 비상장사는 빈 문자열)
        corp_cls: 법인 구분 (Y:유가증권, K:코스닥, N:코넥스, E:기타)
        report_nm: 보고서명 (공시분류 + 보고서명 + 부가정보)
        rcept_no: 접수 번호 (14자리, 핵심 식별자)
        flr_nm: 공시 제출인 명칭
        rcept_dt: 접수 일자 (YYYYMMDD)
        rm: 비고 (유/코/채/넥/공/연/정/철 등 복합 코드 문자열)
        url: 공시 원문 뷰어 URL (자동 생성)
    """
    corp_code: str              # 기업 고유 코드 (8자리)
    corp_name: str              # 기업명
    stock_code: Optional[str]   # 종목 코드 (6자리, 비상장사는 빈값)
    corp_cls: str               # 법인 구분 (Y/K/N/E)
    report_nm: str              # 보고서명
    rcept_no: str               # 접수 번호 (14자리, 고유 식별자)
    flr_nm: Optional[str]       # 공시 제출인명
    rcept_dt: str               # 접수 일자 (YYYYMMDD)
    rm: Optional[str] = None    # 비고 (유/코/채/넥/공/연/정/철 등)
    url: Optional[str] = None   # 공시 원문 URL (자동 생성)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Disclosure':
        """
        딕셔너리(dict) 형태의 DART API 응답 데이터를 받아
        유효성 검사 후 Disclosure 객체로 변환한다.
        
        DART API 명세에 따라 모든 필드는 STRING 타입으로 반환되며,
        stock_code는 비상장사의 경우 빈 문자열("")로 반환된다.
        
        Args:
            data: DART API list.json의 개별 공시 항목 딕셔너리
            
        Returns:
            Disclosure: 변환된 Disclosure 객체
            
        Raises:
            TypeError: 필수 키 누락 또는 타입 불일치 시
        """
        # 필수 키 목록 (DART API 명세 기준)
        required_keys = ['corp_code', 'corp_name', 'corp_cls', 'report_nm', 'rcept_no', 'rcept_dt']

        try:
            # 필수 키 존재 및 타입 검사
            for key in required_keys:
                if key not in data:
                    raise KeyError(f"Required key '{key}' is missing.")
                if not isinstance(data.get(key), str):
                    raise TypeError(f"Field '{key}' must be a string.")

            # stock_code: 비상장사는 빈 문자열로 반환됨 → None으로 정규화
            stock_code_raw = data.get('stock_code', '')
            stock_code = stock_code_raw if stock_code_raw else None

            # flr_nm: 선택 필드
            flr_nm = data.get('flr_nm') or None

            # rm: 비고 필드 (선택)
            rm = data.get('rm') or None

            return cls(
                corp_code=data['corp_code'],
                corp_name=data['corp_name'],
                stock_code=stock_code,
                corp_cls=data['corp_cls'],
                report_nm=data['report_nm'],
                rcept_no=data['rcept_no'],
                flr_nm=flr_nm,
                rcept_dt=data['rcept_dt'],
                rm=rm,
                # 접수 번호로 DART 공시 뷰어 URL 생성
                url=f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={data['rcept_no']}"
            )
        except (KeyError, TypeError) as e:
            raise TypeError(f"Failed to create Disclosure object from data: {data}. Reason: {e}")