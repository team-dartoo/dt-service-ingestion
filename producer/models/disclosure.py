from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class Disclosure:
    ''' DART 공시 정보 저장 데이터 클래스 '''
    
    corp_code: str              # 기업 고유 코드
    corp_name: str              # 기업명
    report_nm: str              # 보고서명
    rcept_no: str               # 접수 번호
    rcept_dt: str               # 접수 일자
    url: Optional[str] = None   # 공시 원문 URL 정보

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Disclosure':
        ''' 딕셔너리 데이터로부터 Disclosure 객체 변환 함수 '''

        # 접수번호 기반 동적 생성 URL
        try:
            # 타입 검증 로직
            for key in ['corp_code', 'corp_name', 'report_nm', 'rcept_no', 'rcept_dt']:
                if not isinstance(data.get(key), str):
                    raise TypeError(f"Field '{key}' must be a string, but got {type(data.get(key))}")
            return cls(
                corp_code=data['corp_code'],
                corp_name=data['corp_name'],
                report_nm=data['report_nm'],
                rcept_no=data['rcept_no'],
                rcept_dt=data['rcept_dt'],
                url=f"http://dart.fss.or.kr/dsaf001/main.do?rcpNo={data['rcept_no']}"
            )
        except KeyError as e:
            # 필수 필드 누락 오류 상황
            raise TypeError(f"Missing required key in disclosure data: {e}")
        except TypeError as e:
            # 타입 에러 동일 발생 상황
            raise e
        except Exception as e:
            # 기타 예기치 못한 오류 상황
            raise TypeError(f"An unexpected error occurred during Disclosure creation: {e}")