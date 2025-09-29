from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass 
# -------------------- DART API의 단일 공시 정보를 구조화하고 유효성을 보장하는 데이터 클래스 --------------------
class Disclosure:
    corp_code: str              # 기업 고유 코드
    corp_name: str              # 기업명
    report_nm: str              # 보고서명
    rcept_no: str               # 접수 번호 (고유 식별자)
    rcept_dt: str               # 접수 일자 (YYYYMMDD)
    url: Optional[str] = None   # 공시 원문 URL (없을 수도 있음)

    @classmethod                # 인스턴스가 아닌 클래스 자체에 바인딩되는 메서드
    
    # ---------- 딕셔너리(dict) 형태의 원본 데이터를 받아 유효성 검사 후 Disclosure 객체로 변환 ----------
    def from_dict(cls, data: Dict[str, Any]) -> 'Disclosure':
        
        required_keys = ['corp_code', 'corp_name', 'report_nm', 'rcept_no', 'rcept_dt'] # 필수 키 목록

        try:
            for key in required_keys:                                                   # 필수 키가 모두 존재하는지, 타입은 올바른지 검사
                if key not in data:
                    raise KeyError(f"Required key '{key}' is missing.")
                if not isinstance(data.get(key), str):
                    raise TypeError(f"Field '{key}' must be a string.")

            return cls(                                                                 # 모든 검증 통과 시, 클래스 생성자를 호출하여 객체 생성 및 반환
                corp_code=data['corp_code'],
                corp_name=data['corp_name'],
                report_nm=data['report_nm'],
                rcept_no=data['rcept_no'],
                rcept_dt=data['rcept_dt'],
                url=f"http://dart.fss.or.kr/dsaf001/main.do?rcpNo={data['rcept_no']}"   # 접수 번호로 공시 뷰어 URL 생성
            )
        except (KeyError, TypeError) as e:                                              # 데이터 검증 실패 시, 원본 데이터를 포함한 상세 예외 발생
            raise TypeError(f"Failed to create Disclosure object from data: {data}. Reason: {e}")