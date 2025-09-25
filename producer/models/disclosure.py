from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class Disclosure:
    ''' 
    DART API로부터 받은 단일 공시 정보를 구조화하여 저장하는 데이터 클래스
    데이터 유효성 보장 역할
    '''    
    corp_code: str              # 기업 고유 코드
    corp_name: str              # 기업명
    report_nm: str              # 보고서명
    rcept_no: str               # 접수 번호
    rcept_dt: str               # 접수 일자
    url: Optional[str] = None   # 공시 원문 URL 정보

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Disclosure':
        ''' 딕셔너리 데이터로부터 Disclosure 객체 변환 함수 '''

        # 필수 필드 정의
        required_keys = ['corp_code', 'corp_name', 'report_nm', 'rcept_no', 'rcept_dt']
        
        try:
            # 필수 필드 누락 검사
            for key in required_keys:
                if key not in data:                         # 필수 키 존재 여부 확인
                    raise KeyError(f"Required key '{key}' is missing from disclosure data.")
                if not isinstance(data.get(key), str):      # 데이터 타입 확인(문자열이 아닌 경우 방지)
                    raise TypeError(f"Field '{key}' must be a string, but got {type(data.get(key))}")

            return cls(                                     # 모든 검증 통과 후, 클래스의 생성자를 호출하여 객체 생성
                corp_code=data['corp_code'],
                corp_name=data['corp_name'],
                report_nm=data['report_nm'],
                rcept_no=data['rcept_no'],
                rcept_dt=data['rcept_dt'],
                # 접수 번호를 사용하여 공시 원문 URL 동적으로 생성
                url=f"http://dart.fss.or.kr/dsaf001/main.do?rcpNo={data['rcept_no']}"
            )
        except (KeyError, TypeError) as e:
            # 오류 발생 시 원본 데이터를 포함하여 예외 다시 발생
            raise TypeError(f"Failed to create Disclosure object from data: {data}. Reason: {e}")